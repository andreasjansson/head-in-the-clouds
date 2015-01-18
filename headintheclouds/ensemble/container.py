import re
import sys
import fabric.api as fab

from headintheclouds import docker
from headintheclouds.ensemble import remote
from headintheclouds.ensemble.exceptions import ConfigException
from headintheclouds.ensemble.thing import Thing

def parse_string(value):
    if not isinstance(value, basestring):
        raise ConfigException('Value is not a string: "%s"' % value)
    return value

def parse_provider(value):
    known_providers = ['ec2', 'digitalocean']
    if value not in known_providers:
        raise ConfigException('Invalid provider: "%s", valid providers are %s' %
                              (value, known_providers))
    return value

def parse_float(value):
    try:
        return float(value)
    except ValueError:
        raise ConfigException('Value is not a float: "%s"' % value)

def parse_bool(value):
    str_val = str(value).lower()
    if str_val in ('t', 'true', '1'):
        return True
    elif str_val in ('f', 'false', '0'):
        return False
    raise ConfigException('Value is not a boolean: "%s"' % value)

def parse_dict(value):
    if not isinstance(value, dict):
        raise ConfigException('Value is not a dictionary: "%s"' % value)
    return value.copy()

def parse_ports(value):
    error = ConfigException(
        '"ports" should be a list in the format "FROM[:TO][/udp]": %s' % value)
    if not isinstance(value, list):
        raise error
    ports = []
    for x in value:
        x = str(x)
        try:
            fr, to, protocol = docker.parse_port_spec(x)
        except ValueError:
            raise error

        ports.append([fr, to, protocol])
    return ports[:]

def parse_size(value):
    value = value.lower()
    if not re.match(r'^[0-9]+[bkmg]?$', value):
        raise ConfigException('Invalid size: %s' % value)
    return value

class Container(Thing):

    field_parsers = {
        'image': parse_string,
        'command': parse_string,
        'environment': parse_dict,
        'ports': parse_ports,
        'volumes': parse_dict,
        'ip': parse_string,
        'max_memory': parse_size,
        'hostname': parse_string,
        'privileged': parse_bool,
    }
    
    def __init__(self, name, host, **kwargs):
        super(Container, self).__init__()
        kwargs.setdefault('ports', [])
        kwargs.setdefault('environment', {})
        kwargs.setdefault('volumes', [])
        self.name = name
        self.host = host
        self.fields.update(kwargs)
        self.fields['name'] = name
        self._pulled_image_id = None

    def is_active(self):
        return self.fields['running']

    def pre_create(self):
        with remote.host_settings(self.host):
            with fab.hide('output'):
                self._pulled_image_id = docker.pull_image(self.fields['image'])

    def create(self):
        with remote.host_settings(self.host):
            if not self._pulled_image_id:
                with fab.hide('output'):
                    self._pulled_image_id = docker.pull_image(self.fields['image'])
            if not self._pulled_image_id:
                raise ConfigException('Image not found: "%s"' % self.fields['image'])
            container = docker.run_container(
                image=self.fields['image'],
                name=self.fields['name'],
                command=self.fields['command'],
                environment=self.fields['environment'],
                ports=self.fields['ports'],
                volumes=self.fields['volumes'],
                max_memory=self.fields['max_memory'],
                hostname=self.fields['hostname'],
                privileged=self.fields['privileged'],
            )
            self.update(container)
        return [self]

    def delete(self):
        with remote.host_settings(self.host):
            try:
                docker.kill(self.name)
            except Exception as e:
                print 'Failed to kill container: %s (host %s)' % (e, self.host)

    def is_equivalent(self, other):
        checks = {
            'host': self.is_equivalent_host,
            'name': self.is_equivalent_name,
            'command': self.is_equivalent_command,
            'environment': self.is_equivalent_environment,
            'ports': self.are_equivalent_ports,
            'volumes': self.are_equivalent_volumes,
            'image': self.is_equivalent_image,
            'hostname': self.is_equivalent_hostname,
            'privilege': self.has_equivalent_privilege,
        }

        return self.check_equivalent(checks, other)

    def is_equivalent_host(self, other):
        is_equivalent = self.host.is_equivalent(other.host)
        log_string = '' if is_equivalent else '%s != %s' % (self.host, other.host)
        return is_equivalent, log_string

    def is_equivalent_name(self, other):
        log_string = '' if self.name == other.name else '%s != %s' % (self.name, other.name)
        return self.name == other.name, log_string

    def is_equivalent_image(self, other):
        existing = self.fields['image']
        new = other.fields['image']
        
        sys.stdout.write('.')
        sys.stdout.flush()

        if existing != new:
            return False, '%s != %s' % (existing, new)

        if existing == new:

            if new is None and existing is None:
                return True, ''

            with remote.host_settings(self.host):
                with fab.settings(fab.hide('everything')):
                    # check if image is updated
                    registry_image_id = docker.get_registry_image_id(new)
                    running_image_id = docker.get_image_id(other.name)
 
            is_equivalent = running_image_id.startswith(registry_image_id)
            log_string = '' if is_equivalent else '%s != %s' % (running_image_id, registry_image_id)
            return is_equivalent, log_string

    def is_equivalent_command(self, other):
        # can't know for sure, so playing safe
        # self will be the remote machine!
        is_equivalent = (other.fields['command'] is None
                         or self.fields['command'] == other.fields['command'])
        log_string = '' if is_equivalent else '%s != %s' % (self.fields['command'], other.fields['command'])
        return is_equivalent, log_string

    def are_equivalent_ports(self, other):
        # same here, can't know for sure,
        # self will be the remote machine!
        public_ports = []
        for fr, to, protocol in self.fields['ports']:
            if to is not None:
                public_ports.append([fr, to, protocol])
        diff_string = set_difference_string(other.fields['ports'], public_ports)
        is_equivalent = not diff_string
        log_string = '' if is_equivalent else diff_string
        return is_equivalent, log_string

    def are_equivalent_volumes(self, other):
        if other.fields['volumes']:
            new_volumes = {k.rstrip('/'): v.rstrip('/')
                           for k, v in other.fields['volumes'].items()}
        else:
            new_volumes = {}
        if self.fields['volumes']:
            old_volumes = {k.rstrip('/'): v.rstrip('/')
                           for k, v in self.fields['volumes'].items()
                           if not k.startswith('/var/lib/docker/vfs/dir/')} # hack
        else:
            old_volumes = {}
        diff_string = set_difference_string(new_volumes, old_volumes)
        is_equivalent = not diff_string
        log_string = '' if is_equivalent else diff_string
        return is_equivalent, log_string

    def is_equivalent_environment(self, other):
        # TODO: this is really janky...
        # the problem is that you can't tell the difference between an environment
        # variable defined in the Dockerfile and one set by -e
        ignored_keys = {'HOME', 'PATH', 'DEBIAN_FRONTEND', 'JAVA_HOME'}

        this_dict = {k: v for k, v in self.fields['environment'].items()}
        other_dict = {k: v for k, v in other.fields['environment'].items()}

        env_lists = ([], [])

        for k in set(this_dict) | set(other_dict):
            if k in ignored_keys:
                continue

            env_lists[0].append((k, str(other_dict.get(k, None))))
            env_lists[1].append((k, str(this_dict.get(k, None))))

        diff_string = set_difference_string(*env_lists)
        is_equivalent = not diff_string
        log_string = '' if is_equivalent else diff_string
        return is_equivalent, log_string

    def is_equivalent_hostname(self, other):
        # TODO: check this
        return True, ''

    def has_equivalent_privilege(self, other):
        # TODO: check this
        return True, ''

    def thing_name(self):
        return ('CONTAINER', self.host.name, self.name)

    def __repr__(self):
        return '<Container: %s (%s)>' % (self.name, self.host.name if self.host else None)


def set_difference_string(new, old):
    if isinstance(new, dict):
        new = set('%s: %s' % (str(k), str(v)) for k, v in new.items())
    if isinstance(old, dict):
        old = set('%s: %s' % (str(k), str(v)) for k, v in old.items())

    new_strings = set(str(x) for x in new)
    old_strings = set(str(x) for x in old)
    in_new = new_strings - old_strings
    in_old = old_strings - new_strings
    s = []
    if in_new:
        s.append('in new: %s' % in_new)
    if in_old:
        s.append('in old: %s' % in_old)
    return '%s' % '; '.join(s)
