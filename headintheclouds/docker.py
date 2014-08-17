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
def ssh(container, cmd='', user='root', password='root'):
    '''
    SSH into a running container, using the host as a jump host. This requires
    the container to have a running sshd process.

    Args:
        * container: Container name or ID
        * cmd='': Command to run in the container
        * user='root': SSH username
        * password='root': SSH password
    '''
    ip = get_ip(container)
    ssh_cmd = 'sshpass -p \'%s\' ssh -A -t -o StrictHostKeyChecking=no \'%s\'@%s' % (password, user, ip)
    local('ssh -A -t -o StrictHostKeyChecking=no -i "%s" %s@%s %s %s' % (
        env.key_filename, env.user, env.host, ssh_cmd, cmd))

@cloudtask
def sshfs(container, remote_dir, local_dir):
    ip = get_ip(container)
    os.path.makedirs(local_dir)
    local('sshfs -o ssh_command="ssh -i %(key_filename)s %(user)s@%(host)s sshpass -p root ssh" root@%(docker_ip)s:"%(remote_dir)s" "%(local_dir)s"' % {
        'key_filename': env.key_filename, 'user': env.user, 'host': env.host,
        'docker_ip': ip, 'remote_dir': remote_dir, 'local_dir': local_dir})

@cloudtask
def ps():
    '''
    Print a table of all running containers on a host
    '''
    containers = get_containers()
    containers = [pretty_container(c) for c in containers]
    print_table(containers, ['name', 'ip', 'ports', 'created', 'image'], sort='name')

@cloudtask
@parallel
def bind(container, *ports):
    '''
    Bind one or more ports to the container.

    Args:
        * container: Container name or ID
        * \*ports: List of items in the format CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]

    Example:
        fab docker.bind:mycontainer,80,"3306:3307","12345/udp"
    '''

    ip = get_ip(container)
    for port_spec in ports:
        port, public_port, protocol = parse_port_spec(port_spec)
        bind_container(ip, port, public_port, protocol)

@cloudtask
@parallel
def unbind(container, *ports):
    '''
    Unbind one or more ports from the container.

    Args:
        * container: Container name or ID
        * \*port: List of items in the format CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]

    Example:
        fab docker.unbind:mycontainer,80,"3306:3307","12345/udp"
    '''

    ip = get_ip(container)
    for port_spec in ports:
        port, public_port, protocol = parse_port_spec(port_spec)
        unbind_container(ip, port, public_port, protocol)

@cloudtask
@parallel
def setup(version=None):
    '''
    Prepare a vanilla server by installing docker, curl, and sshpass. If a file called ``dot_dockercfg``
    exists in the current working directory, it is uploaded as ``~/.dockercfg``.

    Args:
        * version=None: Docker version. If undefined, will install 0.7.6. You can also specify this in env.docker_version
    '''
    if version is None:
        version = getattr(env, 'docker_version', '0.7.6')

    # a bit hacky
    if os.path.exists('dot_dockercfg') and not fabric.contrib.files.exists('~/.dockercfg'):
        put('dot_dockercfg', '~/.dockercfg')

    if not fabric.contrib.files.exists('~/.ssh/id_rsa'):
        fab.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')

    if docker_is_installed():
        return

    if is_ubuntu():
        for attempt in range(3):
            sudo('apt-get update')
            with settings(warn_only=True):
                failed = sudo('apt-get -y install sshpass curl docker.io').failed
                if not failed:
                    break
        sudo('ln -s /usr/bin/docker.io /usr/bin/docker')
    else:
        sudo('yum update -y')
        sudo('yum install -y curl docker')
        install_sshpass_from_source()
        sudo('/etc/init.d/docker start')

@cloudtask
@parallel
def run(image, name=None, command=None, environment=None, ports=None, volumes=None):
    '''
    Run a docker container.

    Args:
        * image: Docker image to run, e.g. orchardup/redis, quay.io/hello/world
        * name=None: Container name
        * command=None: Command to execute
        * environment: Comma separated environment variables in the format NAME=VALUE
        * ports=None: Comma separated port specs in the format CONTAINER_PORT[:EXPOSED_PORT][/PROTOCOL]
        * volumes=None: Comma separated volumes in the format HOST_DIR:CONTAINER_DIR

    Examples:
        * fab docker.run:orchardup/redis,name=redis,ports=6379
        * fab docker.run:quay.io/hello/world,name=hello,ports="80:8080,1000/udp",volumes="/docker/hello/log:/var/log"
        * fab docker.run:andreasjansson/redis,environment="MAX_MEMORY=4G,FOO=bar",ports=6379
    '''

    if ports and not name:
        abort('The ports flag currently only works if you specify a container name')

    if ports:
        ports = [parse_port_spec(p) for p in ports.split(',')]
    else:
        ports = None
    if environment:
        environment = dict([x.split('=') for x in environment.split(',')])
    else:
        environment = None
    if volumes:
        volumes = dict([x.split(':') for x in volumes.split(',')])
    else:
        volumes = None

    run_container(
        image=image,
        name=name,
        command=command,
        ports=ports,
        environment=environment,
        volumes=volumes,
    )

@cloudtask
@parallel
def kill(container, rm=True):
    '''
    Kill a container

    Args:
        * container: Container name or ID
        * rm=True: Remove the container or not
    '''
    container = get_container(container)
    if not container:
        abort('No such container: %s' % container)
    unbind_all(container['ip'])

    sudo('docker kill %s' % container['name'])
    if rm:
        sudo('docker rm %s' % container['name'])

@cloudtask
def pull(image):
    '''
    Pull down an image from a repository (without running it)

    Args:
        image: Docker image
    '''
    sudo('docker pull %s' % image)

@cloudtask
def inspect(container):
    '''
    Inspect a container. Same as running ``docker inspect CONTAINER``
    on the host.

    Args:
        container: Container name or ID
    '''
    sudo('docker inspect %s' % container)

@cloudtask
def logs(container):
    '''
    Get logs from the container. Same as running ``docker logs CONTAINER``
    on the host.    

    Args:
        container: Container name or ID
    '''
    sudo('docker logs %s' % container)

@cloudtask
def tunnel(container, local_port, remote_port=None, gateway_port=None):
    '''
    Set up an SSH tunnel into the container, using the host as a gateway host.

    Args:
        * container: Container name or ID
        * local_port: Local port
        * remote_port=None: Port on the Docker container (defaults to local_port)
        * gateway_port=None: Port on the gateway host (defaults to remote_port)
    '''
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
        parts += ['--name', name]
    if volumes:
        for host_dir, container_dir in volumes.items():
            sudo('mkdir -p "%s"' % host_dir)
            parts += ['-v', '"%s":"%s"' % (host_dir, container_dir)]
    if environment:
        for key, value in environment.items():
            parts += ['-e', "%s='%s'" % (key, value)]
    if ports:
        for local_port, public_port, protocol in ports:
            parts += ['--expose']
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
        if not ip:
            raise Exception('Failed to get container IP')
        for port, public_port, protocol in ports:
            bind_container(ip, port, public_port, protocol)

    return container

def remove_container(id):
    sudo('docker rm %s' % id)

def get_metadata(container):
    with settings(hide('everything'), warn_only=True):
        result = sudo('docker inspect %s' % container)
    if result.failed:
        return None
    return json.loads(result)

def get_ip(container):
    container = get_container(container)
    if container:
        return container['ip']
    return None

def inside(container):
    ip = get_ip(container)
    if not ip:
        abort('No such container: %s' % container)

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
    if metadata['Volumes']:
        volumes = {v: k for k, v in metadata['Volumes'].items()}
    else:
        volumes = {}

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

    if not docker_is_installed():
        return []

    container_ids = []
    with hide('everything'):
        output = sudo('docker ps')
    for line in output.splitlines()[1:]:
        id = line.split(' ', 1)[0]
        container_ids.append(id)
    return container_ids

def docker_is_installed():
    with settings(hide('everything'), warn_only=True):
        return not fab.run('which docker').failed

def get_public_ports(ip):
    with hide('everything'):
        rules = sudo('iptables -t nat -S')
    public_ports = []
    for protocol in ('tcp', 'udp'):
        for rule in rules.splitlines():
            match = re.search('^-A DOCKER -p %s -m %s --dport ([0-9]+) -j DNAT --to-destination %s:([0-9]+)' % (protocol, protocol, ip), rule)
            if match:
                public_ports.append((match.group(2), match.group(1), protocol))
    return public_ports

def bind_container(ip, port, public_port, protocol='tcp'):
    unbind_port(public_port, protocol)
    sudo('iptables -t nat -A DOCKER -p %s --dport %s -j DNAT --to-destination %s:%s' % (protocol, public_port, ip, port))

def unbind_container(ip, port, public_port, protocol='tcp'):
    with hide('everything'):
        rules = sudo('iptables -t nat -S')
    for rule in rules.splitlines():
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
        unbind_container(ip, local_port, public_port, protocol)

def unbind_port(public_port, protocol='tcp'):
    with hide('everything'):
        rules = sudo('iptables -t nat -S')
    for rule in rules.splitlines():
        if re.search('^-A DOCKER -p %s -m %s --dport %s -j DNAT --to-destination (?P<ip>.+):(?P<local_port>[0-9]+)$' % (protocol, protocol, public_port), rule):
            undo_rule = re.sub('-A DOCKER', '-D DOCKER', rule)
            sudo('iptables -t nat %s' % undo_rule)

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

def get_registry_image_id(name):
    registry, namespace, repository, tag = parse_image_name(name)
    response = registry_api(registry, 'repositories/%s/%s/tags' % (namespace, repository))
    if type(response) == list:
        for r in response:
            if r['name'] == tag:
                return r['layer']
    else:
        return response[tag]

    raise ValueError('Unknown tag: %s' % tag)

def parse_image_name(name):
    regex = r'''
        ^(?:(?P<registry>[^/]+)/)?
         (?P<namespace>[^/]+)/
         (?P<repository>[^:]+)
         (?::(?P<tag>.+))?$
    '''
    match = re.match(regex, name, re.VERBOSE)
    if not match:
        raise ValueError('Invalid image name: "%s"' % name)
    groups = match.groupdict()
    return (groups['registry'] or 'index.docker.io',
            groups['namespace'],
            groups['repository'],
            groups['tag'] or 'latest')

def registry_api(registry, endpoint):
    cfg = get_docker_cfg()
    for host, details in cfg.items():
        host_registry = re.sub(r'^(?:https://)?([^/]+)(?:/v1/)?', r'\1', host)
        if host_registry == registry:
            ret = fab.run('curl --header "Authorization: Basic %s" "https://%s/v1/%s"' %
                      (details['auth'], registry, endpoint))
            return json.loads(ret)
    if registry == 'index.docker.io':
        ret = fab.run('curl "https://%s/v1/%s"' % (registry, endpoint))
        return json.loads(ret)
        
    raise ValueError('Registry not in .dockercfg: %s' % registry)

def get_docker_cfg():
    ret = fab.run('cat ~/.dockercfg 2>/dev/null || echo "{}"')
    return json.loads(ret)

def is_ubuntu():
    with settings(hide('everything'), warn_only=True):
        return not fab.run('which apt-get').failed

def install_sshpass_from_source():
    run('mkdir tmpbuild')
    with cd('tmpbuild'):
        run('wget -O sshpass.tar.gz "http://downloads.sourceforge.net/project/sshpass/sshpass/1.05/sshpass-1.05.tar.gz?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fsshpass%2F&ts=1407636443&use_mirror=hivelocity"')
        run('tar xzvf sshpass.tar.gz')
        with('cd sshpass-1.05'):
            run('./configure')
            run('make')
            sudo('make install')
