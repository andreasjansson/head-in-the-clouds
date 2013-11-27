import re
import os
import simplejson as json
import fabric.contrib.files
import fabric.api as fab
import fabric.context_managers
from fabric.api import sudo, env # cause i'm lazy
from tasks import task
from util import autodoc

def inspect(process):
    with fab.hide('everything'):
        result = sudo('docker inspect %s' % process)
    if result.failed:
        return None
    return json.loads(result)

def get_ip(process):
    info = inspect(process)
    ip = info[0]['NetworkSettings']['IPAddress']
    return ip

def inside(process):
    ip = get_ip(process)
    return fabric.context_managers.settings(gateway='%s@%s:%s' % (env.user, env.host, env.port),
                                            host=ip, host_string='root@%s' % ip, user='root',
                                            key_filename=None, password='root', no_keys=True, allow_agent=False)

@task
def ssh(process, cmd=''):
    ip = get_ip(process)
    fab.local('ssh -A -t -o StrictHostKeyChecking=no -i "%s" %s@%s sshpass -p root ssh -A -t -o StrictHostKeyChecking=no root@%s %s' % (
        env.key_filename, env.user, env.host, ip, cmd))

@task
@autodoc
def ps():
    sudo('docker ps')

@task
@fab.parallel
@autodoc
def bind(process, port):
    ip = get_ip(process)
    unbind(port)
    sudo('iptables -t nat -A DOCKER -p tcp --dport %s -j DNAT --to-destination %s:%s' % (port, ip, port))

@task
@fab.parallel
@autodoc
def unbind(port):
    with fab.hide('everything'):
        rules = sudo('iptables -t nat -S')
    for rule in rules.split('\r\n'):
        if re.search('^-A DOCKER -p tcp -m tcp --dport %s' % port, rule):
            undo_rule = re.sub('-A DOCKER', '-D DOCKER', rule)
            sudo('iptables -t nat %s' % undo_rule)

@task
@fab.parallel
@autodoc
def setup():
    # a bit hacky
    if os.path.exists('dot_dockercfg') and not fabric.contrib.files.exists('~/.dockercfg'):
        fab.put('dot_dockercfg', '~/.dockercfg')

    if not fabric.contrib.files.exists('~/.ssh/id_rsa'):
        fab.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')

    with fab.hide('everything'):
        ret = fab.run('which docker')
    if not ret.failed:
        return

    sudo('sh -c "wget -qO- https://get.docker.io/gpg | apt-key add -"')
    sudo('sh -c "echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list"')
    sudo('apt-get update')
    sudo('apt-get -y install linux-image-extra-virtual')
    sudo('apt-get -y install lxc-docker')

    sudo('reboot')

@task
@fab.parallel
@autodoc
def run(image, cmd=None, name=None, ports=None, **kwargs):
    setup()

    parts = ['docker', 'run', '-d']
    if name:
        parts += ['-name', name]
    for key, value in kwargs.items():
        parts += ['-e', '%s=%s' % (key, value)]
    parts += [image]
    if cmd:
        parts += [cmd]
    result = sudo(' '.join(parts))
    process = result.strip()

    if ports:
        ports = ports.split(',')
        for port in ports:
            port = port.strip()
            bind(process, port)

@task
@fab.parallel
@autodoc
def kill(process):
    sudo('docker kill %s' % process)
