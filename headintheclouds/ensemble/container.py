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
            )
            self.update(container)
        return [self]

    def delete(self):
        with remote.host_settings(self.host):
            docker.kill(self.name)

    def is_equivalent(self, other):
        return (self.host.is_equivalent(other.host)
                and self.name == other.name
                and self.is_equivalent_command(other)
                and self.is_equivalent_environment(other)
                and self.are_equivalent_ports(other)
                and set(self.fields['volumes']) == set(other.fields['volumes'])
                and self.is_equivalent_image(other))

    def is_equivalent_image(self, other):
        if self.fields['image'] != other.fields['image']:
            return False

        if self.fields['image'] == other.fields['image']:
            with remote.host_settings(self.host):
                with fab.settings(fab.hide('everything')):
                    registry_image_id = docker.get_registry_image_id(other.fields['image'])
                    running_image_id = docker.get_image_id(other.name)
 
            sys.stdout.write('.')
            sys.stdout.flush()
 
            return registry_image_id == running_image_id

    def is_equivalent_command(self, other):
        # can't know for sure, so playing safe
        # self will be the remote machine!
        return (other.fields['command'] is None
                or self.fields['command'] == other.fields['command'])

    def are_equivalent_ports(self, other):
        # same here, can't know for sure,
        # self will be the remote machine!
        public_ports = []
        for fr, to, protocol in self.fields['ports']:
            if to is not None:
                public_ports.append([fr, to, protocol])
        return sorted(public_ports) == sorted(other.fields['ports'])

    def is_equivalent_environment(self, other):
        ignored_keys = {'HOME', 'PATH', 'DEBIAN_FRONTEND'} # TODO: for now (or forever maybe?)
        this_dict = {k: v for k, v in self.fields['environment'].items()}
        other_dict = {k: v for k, v in other.fields['environment'].items()}
        for k in set(this_dict) | set(other_dict):
            if k in ignored_keys:
                continue

            # compare apples with 'apples'
            if str(this_dict.get(k, None)) != str(other_dict.get(k, None)):
                return False
        return True

    def thing_name(self):
        return ('CONTAINER', self.host.name, self.name)

    def __repr__(self):
        return '<Container: %s (%s)>' % (self.name, self.host.name if self.host else None)

