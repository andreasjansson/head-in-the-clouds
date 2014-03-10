import re
import os
import simplejson as json
import subprocess
import dateutil.parser
import fabric.contrib.files
import fabric.api as fab
import fabric.context_managers
from fabric.api import * # pylint: disable=W0614,W0401
from headintheclouds.tasks import cloudtask
from headintheclouds.util import print_table
import collections
from StringIO import StringIO

@cloudtask
def ssh(process, cmd=''):
    ip = get_ip(process)
    ssh_cmd = 'sshpass -p root ssh -A -t -o StrictHostKeyChecking=no root@%s' % ip
    local('ssh -A -t -o StrictHostKeyChecking=no -i "%s" %s@%s %s %s' % (
        env.key_filename, env.user, env.host, ssh_cmd, cmd))

@cloudtask
def sshfs(process, remote_dir, local_dir):
    ip = get_ip(process)
    os.path.makedirs(local_dir)
    local('sshfs -o ssh_command="ssh -i %(key_filename)s %(user)s@%(host)s sshpass -p root ssh" root@%(docker_ip)s:"%(remote_dir)s" "%(local_dir)s"' % {
        'key_filename': env.key_filename, 'user': env.user, 'host': env.host,
        'docker_ip': ip, 'remote_dir': remote_dir, 'local_dir': local_dir})

@cloudtask
def ps():
    containers = get_containers()
    containers = [pretty_container(c) for c in containers]
    print_table(containers, ['name', 'ip', 'ports', 'created', 'image'], sort='name')

@cloudtask
#@parallel
def bind(process, *port_specs):
    '''
    Bind one or more ports to the container.

    Usage:
        fab docker.bind:process,port_spec1,...

      where
        process is the name of the container process
        port_spec1,... is a list in the format
            "CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]"
    '''

    ip = get_ip(process)
    for port_spec in port_specs:
        port, public_port, protocol = parse_port_spec(port_spec)
        bind_process(ip, port, public_port, protocol)

@cloudtask
@parallel
def unbind(process, *port_specs):
    '''
    Unbind one or more ports from the container.

    Usage:
        fab docker.unbind:process,port_spec1,...

      where
        process is the name of the container process
        port_spec1,... is a list in the format
            "CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]"
    '''

    ip = get_ip(process)
    for port_spec in port_specs:
        port, public_port, protocol = parse_port_spec(port_spec)
        unbind_process(ip, port, public_port, protocol)

@cloudtask
@parallel
def setup(directory=None, version=None):
    if not version:
        version = getattr(env, 'docker_version', '0.7.6')

    # a bit hacky
    if os.path.exists('dot_dockercfg') and not fabric.contrib.files.exists('~/.dockercfg'):
        put('dot_dockercfg', '~/.dockercfg')

    if not fabric.contrib.files.exists('~/.ssh/id_rsa'):
        fab.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')

    # check if it's already there
    with settings(hide('everything'), warn_only=True):
        if not fab.run('which docker').failed:
            return

    sudo('sh -c "wget -qO- https://get.docker.io/gpg | apt-key add -"')
    sudo('sh -c "echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"')
    sudo('apt-get update')
    sudo('apt-get -y install linux-image-extra-virtual')

    with settings(warn_only=True):
        # this seems to fail occasionally, but seems to work
        # second time. computer:(
        for retry in range(3):
            ret = sudo('apt-get -y install lxc-docker-%s' % version)
            if ret.succeeded:
                break
            sudo('apt-get update')

    sudo('apt-get -y install sshpass')

    if directory is not None:
        sudo('stop docker')
        parent_dir = '/'.join(directory.split('/')[:-1])
        sudo('mkdir -p "%s"' % parent_dir)
        sudo('mv /var/lib/docker "%s"' % directory)
        sudo('ln -s "%s" /var/lib/docker' % directory)
        sudo('start docker')
    
#    if reboot:
#        sudo('reboot')

@cloudtask
@parallel
def run(image, name=None, *port_specs, **kwargs):
    '''
    Run a docker container

    Usage:
        fab docker.run:image,name=None,cmd=None,*port_specs,**env_vars

      where
        image is the name of the image, can be either a hash or a tag,
            e.g. ec85d8f5ea3d or quay.io/myusername/myimage
        name is the name of the created container
        cmd is the command to run
        *port_specs is a list of items in the format "CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]"
        **env_vars is a list of NAME=VALUE pairs that become part of the environment
    '''

    if port_specs and not name:
        abort('The ports flag currently only works if you specify a process name')

    if 'cmd' in kwargs:
        cmd = kwargs['cmd']
        del kwargs['cmd']
    else:
        cmd = None
    env_vars = kwargs

    run_container(image=image,
                  name=name,
                  command=cmd,
                  ports=[parse_port_spec(p) for p in port_specs],
                  environment=env_vars)

@cloudtask
#@parallel
def kill(process, rm=True):
    container = get_container(process)
    if not container:
        abort('No such container: %s' % process)
    unbind_all(container['ip'])

    sudo('docker kill %s' % process)
    if rm:
        sudo('docker rm %s' % process)

@cloudtask
@parallel
def upstart(image, name=None, cmd='', respawn=True, n_instances=1, start=True, **kwargs):
    # TODO: deprecate this

    n_instances = int(n_instances)
    assert n_instances > 0
    respawn = str(respawn).lower() == 'true'

    upstart_template = '''
%(instances_stanza)s

script
    docker run %(env_vars)s -rm -name %(name)s %(image)s %(cmd)s
end script

%(respawn_stanza)s
'''

    if not name:
        name = image.split('/')[-1].split('.')[0]

    args = collections.defaultdict(str)
    args['image'] = image
    args['name'] = name
    if n_instances > 1:
        args['instances_stanza'] = 'instance $N'
        args['name'] += '-$N'
    if respawn:
        args['respawn_stanza'] = 'respawn'
    if cmd:
        args['cmd'] = cmd
    if kwargs:
        for key, value in kwargs.items():
            args['env_vars'] += ('-e %s=%s' % (key, value))

    upstart_script = upstart_template % args
    put(StringIO(upstart_script), '/etc/init/%s.conf' % name, use_sudo=True)

    if start:
        if n_instances > 1:
            for i in range(n_instances):
                sudo('start %s N=%d' % (name, i))
        else:
            sudo('start %s' % name)

@cloudtask
def pull(image):
    sudo('docker pull %s' % image)

@cloudtask
def inspect(process):
    sudo('docker inspect %s' % process)

@cloudtask
def tunnel(container, local_port, remote_port=None, gateway_port=None):
    if remote_port is None:
        remote_port = local_port
    if gateway_port is None:
        gateway_port = remote_port

    remote_host = get_ip(container)

    command = '''
        ssh -v
            -o StrictHostKeyChecking=no
            -i "%(key_filename)s"
            -L %(local_port)s:localhost:%(gateway_port)s
            %(gateway_user)s@%(gateway_host)s
                sshpass -p root
                    ssh -o StrictHostKeyChecking=no
                        -L %(gateway_port)s:localhost:%(remote_port)s
                            root@%(remote_host)s
    ''' % {
        'key_filename': env.key_filename,
        'local_port': local_port,
        'gateway_port': gateway_port,
        'gateway_user': env.user,
        'gateway_host': env.host,
        'remote_port': remote_port,
        'remote_host': remote_host,
    }

    command = command.replace('\n', '')

    local(command)

def run_container(image, name=None, command=None, environment=None,
                  ports=None, volumes=None, max_memory=None):

    setup()

    container = get_container(name)
    if container and container['state'] == 'stopped':
        remove_container(name)

    if isinstance(environment, (list, tuple)):
        environment = {k: v for k, v in environment}

    parts = ['docker', 'run', '-d']
    if name:
        parts += ['-name', name]
    if volumes:
        for host_dir, container_dir in volumes.items():
            sudo('mkdir -p "%s"' % host_dir)
            parts += ['-v', '"%s":"%s"' % (host_dir, container_dir)]
    if environment:
        for key, value in environment.items():
            parts += ['-e', "%s='%s'" % (key, value)]
    if ports:
        for local_port, public_port, protocol in ports:
            parts += ['-expose']
            if protocol == 'udp':
                # import ipdb; ipdb.set_trace() TODO: debug why on earth udp would be first
                parts += ['%s/udp' % local_port]
            else:
                parts += ['%s' % local_port]
    if max_memory:
        parts += ['-m', max_memory]
    parts += [image]
    if command:
        parts += ['%s' % command]
    command_line = ' '.join(parts)
    sudo(command_line)

    container = get_container(name)
    if ports:
        ip = container['ip']
        for port, public_port, protocol in ports:
            bind_process(ip, port, public_port, protocol)

    return container

def remove_container(id):
    sudo('docker rm %s' % id)

def get_metadata(process):
    with settings(hide('everything'), warn_only=True):
        result = sudo('docker inspect %s' % process)
    if result.failed:
        return None
    return json.loads(result)

def get_ip(process):
    container = get_container(process)
    if container:
        return container['ip']
    return None

def inside(process):
    ip = get_ip(process)
    if not ip:
        abort('No such container: %s' % process)

    # paramiko caches connections by ip. different containers often have
    # the same ip.
    fabric.network.disconnect_all() 

    return fabric.context_managers.settings(gateway='%s@%s:%s' % (env.user, env.host, env.port),
                                            host=ip, host_string='root@%s' % ip, user='root',
                                            password='root', no_keys=True, allow_agent=False)

def get_containers():
    containers = []
    container_ids = get_container_ids()
    for id in container_ids:
        containers.append(get_container(id))
    return containers

def get_container(id):
    metadata = get_metadata(id)
    if not metadata:
        return None
    metadata = metadata[0]

    created = dateutil.parser.parse(metadata['Created'])
    name = metadata['Name'][1:]
    ip = metadata['NetworkSettings']['IPAddress']
    local_ports = metadata['NetworkSettings']['Ports']

    if local_ports:
        local_ports = set([tuple(k.split('/')) for k in metadata['NetworkSettings']['Ports']])
    else:
        local_ports = {}
    ports = get_public_ports(ip)
    for local_port, public_port, protocol in ports:
        if (local_port, protocol) in local_ports:
            local_ports.remove((local_port, protocol))
    for port, protocol in local_ports:
        ports.append((port, None, protocol))

    int_or_none = lambda x: None if x is None else int(x)
    # make it a list cause ensemble wants it
    ports = [[int_or_none(fr), int_or_none(to), protocol] for
             fr, to, protocol in ports
             if fr != 'udp'] # for some reason the from port can end up being udp. no idea why. TODO: figure out why
    
    environment = metadata['Config']['Env'] or []
    environment = dict([e.split('=', 1) for e in environment])
    state = 'running' if metadata['State']['Running'] else 'stopped'
    command = subprocess.list2cmdline(metadata['Config']['Cmd'])
    running = state == 'running'

    # for some reason docker run's syntax is inconsistent with its internal representation
    volumes = {v: k for k, v in metadata['Volumes'].items()}

    image = metadata['Config']['Image']
    return {
        'created': created.strftime('%Y-%m-%d %H:%M:%S'),
        'name': name,
        'command': command,
        'ip': ip,
        'ports': ports,
        'image': image,
        'environment': environment,
        'state': state,
        'volumes': volumes,
        'running': running,
    }

#hack
def pull_image(name):
    sudo('docker pull %s' % name)
    with settings(hide('everything'), warn_only=True):
        result = sudo('docker inspect %s' % name)
    if result.failed:
        return None
    result = json.loads(result)
    return result[0]['id']

#hack
def get_image_id(container_name):
    metadata = get_metadata(container_name)
    return metadata[0]['Image']

def get_container_ids():
    setup() # defensive

    container_ids = []
    with hide('everything'):
        output = sudo('docker ps')
    for line in output.split('\r\n')[1:]:
        id = line.split(' ', 1)[0]
        container_ids.append(id)
    return container_ids

def get_public_ports(ip):
    with hide('everything'):
        rules = sudo('iptables -t nat -S')
    public_ports = []
    for protocol in ('tcp', 'udp'):
        for rule in rules.split('\r\n'):
            match = re.search('^-A DOCKER -p %s -m %s --dport ([0-9]+) -j DNAT --to-destination %s:([0-9]+)' % (protocol, protocol, ip), rule)
            if match:
                public_ports.append((match.group(2), match.group(1), protocol))
    return public_ports

def bind_process(ip, port, public_port, protocol='tcp'):
    unbind_process(ip, port, public_port)
    sudo('iptables -t nat -A DOCKER -p %s --dport %s -j DNAT --to-destination %s:%s' % (protocol, public_port, ip, port))

def unbind_process(ip, port, public_port, protocol='tcp'):
    with hide('everything'):
        rules = sudo('iptables -t nat -S')
    for rule in rules.split('\r\n'):
        if re.search('^-A DOCKER -p %s -m %s --dport %s -j DNAT --to-destination %s:%s' % (protocol, protocol, public_port, ip, port), rule):
            undo_rule = re.sub('-A DOCKER', '-D DOCKER', rule)
            sudo('iptables -t nat %s' % undo_rule)

def parse_port_spec(port_spec):
    regex = r'^(?P<from>[^/:]+)(:(?P<to>[^/]+))?(/(?P<protocol>.+))?$'
    matches = re.match(regex, port_spec)
    if not matches:
        raise ValueError('Invalid port spec: %s' % port_spec)

    fr = matches.group('from')
    to = matches.group('to') or fr
    protocol = matches.group('protocol') or 'tcp'

    # try to make them ints, if possible
    try:
        fr = int(fr)
    except ValueError:
        pass
    try:
        to = int(to)
    except ValueError:
        pass

    return fr, to, protocol

def unbind_all(ip):
    ports = get_public_ports(ip)
    for local_port, public_port, protocol in ports:
        unbind_process(ip, local_port, public_port, protocol)

def pretty_container(container):
    container = container.copy()

    ports = container['ports']
    pretty_ports = []
    for fr, to, protocol in ports:
        string = '%s:%s' % (fr, to)
        if protocol != 'tcp':
            string += '/%s' % protocol
        pretty_ports.append(string)

    container['ports'] = ', '.join(pretty_ports)

    return container
