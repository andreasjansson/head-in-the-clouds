import sys
from functools import wraps
import collections
import yaml
from StringIO import StringIO
from collections import defaultdict

from fabric.api import parallel, env, sudo, settings, local, runs_once, run, abort, put
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
@parallel
def rename(role):
    _provider().rename(role)

@task
def uncache():
    util.cache().flush()

@task
def ssh():
    local('ssh -o StrictHostKeyChecking=no -i "%s" %s@%s' % (env.key_filename, env.user, env.host))

@task
def upload(from_path, to_path='.', compress=True):
    options = []
    if str(compress) == 'True':
        options.append('-C')
    options = ' '.join(options)
    local('scp %s -i %s "%s" %s@%s:"%s"' % (options, env.key_filename, from_path, env.user, env.host, to_path))

@task
def download(from_path, to_path='.', compress=True):
    options = []
    if str(compress) == 'True':
        options.append('-C')
    options = ' '.join(options)
    local('scp %s -i %s %s@%s:"%s" "%s"' % (options, env.key_filename, env.user, env.host, from_path, to_path))

@task
@parallel
def test(puppet_dir='puppet'):

    if not puppet_dir.endswith('/'):
        puppet_dir += '/'
    remote_puppet_dir = '/etc/puppet'
    sudo('chown -R %s %s' % (env.user, remote_puppet_dir))
    project.rsync_project(local_dir=puppet_dir, remote_dir=remote_puppet_dir,
                          ssh_opts='-o StrictHostKeyChecking=no')

    sudo('export FACTER_blah="{a => 1, b => 2}"; puppet apply /etc/puppet/init.pp')

@task
@parallel
def build(puppet_dir='puppet', init='init.pp', update=True):
    if str(update) == 'True':
        sudo('dpkg --configure -a')
        sudo('apt-get update')

        sudo('apt-get -y install puppet')
        sudo('chmod 777 /opt')

    if not puppet_dir.endswith('/'):
        puppet_dir += '/'
    remote_puppet_dir = '/etc/puppet'
    sudo('chown -R %s %s' % (env.user, remote_puppet_dir))

    nodes_yaml = yaml.safe_dump(_get_environment())
    put(StringIO(nodes_yaml), remote_puppet_dir + '/nodes.yaml')
    
    project.rsync_project(local_dir=puppet_dir, remote_dir=remote_puppet_dir,
                          ssh_opts='-o StrictHostKeyChecking=no')
    sudo('puppet apply %s/%s' % (remote_puppet_dir, init))

def _get_environment():
    environment = defaultdict(list)
    for p in env.providers:
        if p == env.all_nodes[env.host]['provider']:
            provider_env = sys.modules[p].get_local_environment()
        else:
            provider_env = sys.modules[p].get_remote_environment()
        for role, nodes in provider_env.iteritems():
            environment[role].extend(nodes)
    return dict(environment)

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
