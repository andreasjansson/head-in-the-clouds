import re
import os
import contextlib
import simplejson as json
import dateutil.parser
import fabric.contrib.files
import fabric.api as fab
import fabric.context_managers
from fabric.api import sudo, env, abort # cause i'm lazy
from headintheclouds.tasks import task
from headintheclouds.util import autodoc, print_table
import collections
from StringIO import StringIO

def get_metadata(process):
    with fab.hide('everything'):
        result = sudo('docker inspect %s' % process)
    if result.failed:
        return None
    return json.loads(result)

def get_ip(process):
    info = get_metadata(process)
    ip = info[0]['NetworkSettings']['IPAddress']
    return ip

def inside(process):
    ip = get_ip(process)
    return fabric.context_managers.settings(gateway='%s@%s:%s' % (env.user, env.host, env.port),
                                            host=ip, host_string='root@%s' % ip, user='root',
                                            key_filename=None, password='root', no_keys=True, allow_agent=False)

def get_container_ids():
    container_ids = []
    with fab.hide('everything'):
        output = sudo('docker ps')
    for line in output.split('\r\n')[1:]:
        id = line.split(' ', 1)[0]
        container_ids.append(id)
    return container_ids

def get_bound_ports(ip, ports):
    with fab.hide('everything'):
        rules = sudo('iptables -t nat -S')
    bound_ports = []
    for port in ports:
        found_port = False
        for rule in rules.split('\r\n'):
            match = re.search('^-A DOCKER -p tcp -m tcp --dport ([0-9]+) -j DNAT --to-destination %s+:%s' % (ip, port), rule)
            if match:
                bound_ports.append((port, match.group(1)))
                found_port = True
        if not found_port:
            bound_ports.append((port, None))
    return bound_ports

@task
def ssh(process, cmd=''):
    ip = get_ip(process)
    fab.local('ssh -A -t -o StrictHostKeyChecking=no -i "%s" %s@%s sshpass -p root ssh -A -t -o StrictHostKeyChecking=no root@%s %s' % (
        env.key_filename, env.user, env.host, ip, cmd))

@task
@autodoc
def ps():
    container_ids = get_container_ids()
    processes = []
    for id in container_ids:
        metadata = get_metadata(id)[0]
        created = dateutil.parser.parse(metadata['Created'])
        name = metadata['Name'][1:]
        ip = metadata['NetworkSettings']['IPAddress']
        ports = [k.split('/')[0] for k in metadata['NetworkSettings']['Ports']]
        bound_ports = get_bound_ports(ip, ports)
        image = metadata['Config']['Image']
        processes.append({
            'Created': created.strftime('%Y-%m-%d %H:%M:%S'),
            'Name': name,
            'IP': ip,
            'Ports': ', '.join(['%s -> %s' % (fr, to) for fr, to in bound_ports]),
            'Image': image,
        })
    print_table(processes, ['Name', 'IP', 'Ports', 'Created', 'Image'])

@task
@fab.parallel
@autodoc
def bind(process, port, bound_port=None):
    if bound_port is None:
        bound_port = port
    ip = get_ip(process)
    unbind(port, bound_port)
    sudo('iptables -t nat -A DOCKER -p tcp --dport %s -j DNAT --to-destination %s:%s' % (bound_port, ip, port))

@task
@fab.parallel
@autodoc
def unbind(port, bound_port=None):
    if bound_port is None:
        bound_port = port
    
    with fab.hide('everything'):
        rules = sudo('iptables -t nat -S')
    for rule in rules.split('\r\n'):
        if re.search('^-A DOCKER -p tcp -m tcp --dport %s -j DNAT --to-destination [^:]+:%s' % (bound_port, port), rule):
            undo_rule = re.sub('-A DOCKER', '-D DOCKER', rule)
            sudo('iptables -t nat %s' % undo_rule)

@task
@fab.parallel
@autodoc
def setup(directory=None, reboot=True):
    # a bit hacky
    if os.path.exists('dot_dockercfg') and not fabric.contrib.files.exists('~/.dockercfg'):
        fab.put('dot_dockercfg', '~/.dockercfg')

    if not fabric.contrib.files.exists('~/.ssh/id_rsa'):
        fab.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')

    with contextlib.nested(fab.hide('everything'), fab.settings(warn_only=True)):
        if not fab.run('which docker').failed:
            return

    sudo('sh -c "wget -qO- https://get.docker.io/gpg | apt-key add -"')
    sudo('sh -c "echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"')
    sudo('apt-get update')
    sudo('DEBIAN_FRONTEND=noninteractive apt-get -y install linux-image-extra-virtual')
    sudo('DEBIAN_FRONTEND=noninteractive apt-get -y install lxc-docker')
    sudo('apt-get -y install sshpass')

    if directory is not None:
        sudo('stop docker')
        parent_dir = '/'.join(directory.split('/')[:-1])
        sudo('mkdir -p "%s"' % parent_dir)
        sudo('mv /var/lib/docker "%s"' % directory)
        sudo('ln -s "%s" /var/lib/docker' % directory)
        sudo('start docker')
    
    if reboot:
        sudo('reboot')

@task
@fab.parallel
@autodoc
def run(image, name=None, cmd=None, ports=None, bound_ports=None, **kwargs):

    if ports and not name:
        abort('The ports flag currently only works if you specify a process name')

    if ports:
        ports = ports.split(',')
        if bound_ports:
            bound_ports = bound_ports.split(',')
            if len(ports) != len(bound_ports):
                abort('bound_ports need to be the same length as ports')
        else:
            bound_ports = ports

    setup()

    parts = ['docker', 'run', '-d']
    if name:
        parts += ['-name', name]
    for key, value in kwargs.items():
        parts += ['-e', '%s=%s' % (key, value)]
    parts += [image]
    if cmd:
        parts += [cmd]
    run_cmd = ' '.join(parts)
    sudo(run_cmd)

    if ports:
        for port, bound_port in zip(ports, bound_ports):
            port = port.strip()
            bound_port = bound_port.strip()
            bind(name, port, bound_port)

@task
@fab.parallel
@autodoc
def kill(process, rm=True):
    sudo('docker kill %s' % process)
    if rm:
        sudo('docker rm %s' % process)

@task
@fab.parallel
@autodoc
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
    fab.put(StringIO(upstart_script), '/etc/init/%s.conf' % name, use_sudo=True)

    if start:
        if n_instances > 1:
            for i in range(n_instances):
                sudo('start %s N=%d' % (name, i))
        else:
            sudo('start %s' % name)

@task
@autodoc
def pull(image):
    sudo('docker pull %s' % image)

@task
@autodoc
def inspect(process):
    sudo('docker inspect %s' % process)
