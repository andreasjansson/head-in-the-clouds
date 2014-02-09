import re
import os
import simplejson as json
import subprocess
import dateutil.parser
import fabric.contrib.files
import fabric.api as fab
import fabric.context_managers
from fabric.api import *
from headintheclouds import cloudtask
from headintheclouds.util import autodoc, print_table
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
@autodoc
def ps():
    containers = get_containers()
    print_table(containers, ['name', 'ip', 'ports', 'created', 'image'], sort='name')

@cloudtask
@parallel
@autodoc
def bind(process, port_spec1, *other_port_specs):
    '''
    Bind one or more ports to the container.

    Usage:
        fab docker.bind:process,port_spec1,...

      where
        process is the name of the container process
        port_spec1,... is either a list of either single port or
            CONTAINER_PORT-EXPOSED_PORT (hyphen-delimited) strings
    '''

    ip = get_ip(process)
    for port, public_port in parse_port_specs([port_spec1] + list(other_port_specs)):
        bind_process(ip, port, public_port)

@cloudtask
@parallel
@autodoc
def unbind(process, port_spec1, *other_port_specs):
    '''
    Unbind one or more ports from the container.

    Usage:
        fab docker.unbind:process,port_spec1,...

      where
        process is the name of the container process
        port_spec1,... is either a list of either single port or
            CONTAINER_PORT-EXPOSED_PORT (hyphen-delimited) strings
    '''

    ip = get_ip(process)
    for port, public_port in parse_port_specs([port_spec1] + list(other_port_specs)):
        unbind_process(ip, port, public_port)

@cloudtask
@parallel
def setup(directory=None, reboot=True):
    # TODO: make this not require a reboot

    # a bit hacky
    if os.path.exists('dot_dockercfg') and not fabric.contrib.files.exists('~/.dockercfg'):
        put('dot_dockercfg', '~/.dockercfg')

    if not fabric.contrib.files.exists('~/.ssh/id_rsa'):
        fab.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')

    with settings(hide('everything'), warn_only=True):
        if not fab.run('which docker').failed:
            return

    sudo('sh -c "wget -qO- https://get.docker.io/gpg | apt-key add -"')
    sudo('sh -c "echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"')
    sudo('apt-get update')
    sudo('apt-get -y install linux-image-extra-virtual')

    with settings(warn_only=True):
        for retry in range(3):
            ret = sudo('apt-get -y install lxc-docker-0.7.6')
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
@autodoc
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
        *port_specs is a list of port numbers, or PORT-EXPOSED PORT strings
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
                  ports=parse_port_specs(port_specs),
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
@autodoc
def inspect(process):
    sudo('docker inspect %s' % process)



def run_container(image, name=None, command=None, environment=None,
                  ports=None, volumes=None):

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
        for volume in volumes:
            parts += ['-volume', volume]
    if environment:
        for key, value in environment.items():
            parts += ['-e', '%s=%s' % (key, value)]
    parts += [image]
    if command:
        parts += ['%s' % command]
    command_line = ' '.join(parts)
    sudo(command_line)

    if ports:
        ip = get_ip(name)
        for port, public_port in parse_port_specs(ports):
            bind_process(ip, port, public_port)

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
    return container['ip']

def inside(process):
    ip = get_ip(process)
    return fabric.context_managers.settings(gateway='%s@%s:%s' % (env.user, env.host, env.port),
                                            host=ip, host_string='root@%s' % ip, user='root',
                                            key_filename=None, password='root', no_keys=True, allow_agent=False)

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
        local_ports = set([k.split('/')[0] for k in metadata['NetworkSettings']['Ports']])
    else:
        local_ports = []
    ports = get_public_ports(ip)
    for local_port, public_port in ports:
        if local_port in local_ports:
            local_ports.remove(local_port)
    for port in local_ports:
        ports.append((port, None))
    ports = [[fr, to] for fr, to in ports] # make it a list cause ensemble wants it
    environment = metadata['Config']['Env']
    environment = [e.split('=', 1) for e in environment]
    state = 'running' if metadata['State']['Running'] else 'stopped'
    command = subprocess.list2cmdline(metadata['Config']['Cmd'])

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
    }

def get_container_ids():
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
    for rule in rules.split('\r\n'):
        match = re.search('^-A DOCKER -p tcp -m tcp --dport ([0-9]+) -j DNAT --to-destination %s:([0-9]+)' % ip, rule)
        if match:
            public_ports.append((match.group(2), match.group(1)))
    return public_ports

def bind_process(ip, port, public_port):
    unbind_process(ip, port, public_port)
    sudo('iptables -t nat -A DOCKER -p tcp --dport %s -j DNAT --to-destination %s:%s' % (public_port, ip, port))

def unbind_process(ip, port, public_port):
    with hide('everything'):
        rules = sudo('iptables -t nat -S')
    for rule in rules.split('\r\n'):
        if re.search('^-A DOCKER -p tcp -m tcp --dport %s -j DNAT --to-destination %s:%s' % (public_port, ip, port), rule):
            undo_rule = re.sub('-A DOCKER', '-D DOCKER', rule)
            sudo('iptables -t nat %s' % undo_rule)

def parse_port_specs(port_specs):
    parsed = []
    for port_spec in port_specs:
        split = port_spec.split('-')
        if len(split) > 1:
            port, public_port = split
        else:
            port = public_port = split[0]
        port = int(port.strip())
        public_port = int(public_port.strip())
        parsed.append((port, public_port))
    return parsed

def unbind_all(ip):
    ports = get_public_ports(ip)
    for local_port, public_port in ports:
        unbind_process(ip, local_port, public_port)

