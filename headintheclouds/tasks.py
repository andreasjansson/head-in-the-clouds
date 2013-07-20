import sys
from functools import wraps
import collections

from fabric.api import parallel, env, sudo, settings, local, runs_once, run, abort
import fabric.api
import fabric.contrib.project as project

import util

def task(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with _provider_settings():
            func(*args, **kwargs)
    return fabric.api.task(wrapper)

def once_per_provider(func_name, *args, **kwargs):
    def wrapped():
        for p in env.providers:
            provider = sys.modules[p]
            print '\n%s:\n' % provider.provider_name.upper()
            getattr(provider, func_name)(*args, **kwargs)
    wrapped.__name__ = func_name
    setattr(sys.modules[__name__], func_name, runs_once(fabric.api.task(wrapped)))

once_per_provider('pricing')
once_per_provider('nodes')

@task
@parallel
def terminate():
    _provider().terminate()

@task
def uncache():
    util.cache().flush()

@task
def ssh():
    local('ssh -o StrictHostKeyChecking=no -i "%s" root@%s' % (env.key_filename, env.host))

@task
def scp(remote_path, local_path='.'):
    local('scp -C -i %s %s@%s:"%s" %s' % (env.key_filename, env.user, env.host, remote_path, local_path))

@task
@parallel
def build(puppet_dir='puppet', init_filename='init.pp'):
    sudo('dpkg --configure -a')
    sudo('apt-get update')
    sudo('apt-get -y install puppet')
    sudo('chmod 777 /opt')

    if not puppet_dir.endswith('/'):
        puppet_dir += '/'
    remote_puppet_dir = '/opt/puppet'
    sudo('mkdir -p %s' % remote_puppet_dir)
    sudo('chown -R %s %s' % (env.user, remote_puppet_dir))
    project.rsync_project(local_dir=puppet_dir, remote_dir=remote_puppet_dir,
                          ssh_opts='-o StrictHostKeyChecking=no')
    sudo('puppet apply %s/%s' % (remote_puppet_dir, init_filename))

def _provider():
    return sys.modules[env.all_nodes[env.host]['provider']]

def _provider_settings():
    provider_env = _provider().settings
    return settings(**provider_env)

env.disable_known_hosts = True

def setup():
    if not hasattr(env, 'all_nodes'):
        env.all_nodes = {}
    if not hasattr(env, 'providers'):
        env.providers = {}

    env.roledefs = collections.defaultdict(list)
    for node in env.all_nodes.itervalues():
        role = node['role']
        env.roledefs[role].append(node['ip_address'])

    if not env.hosts:
        if env.roles:
            env.hosts = [i for r in env.roles for i in env.roledefs[r]]
        else:
            env.hosts = [x['ip_address'] for x in env.all_nodes.values()]

setup()
