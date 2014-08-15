import os
from glob import glob
import time
from StringIO import StringIO
from functools import wraps
from fabric.api import * # pylint: disable=W0614,W0401
import envtpl

from headintheclouds import provider_settings, provider_by_name, this_provider
from headintheclouds import cache

def cloudtask(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with provider_settings():
            func(*args, **kwargs)
    return task(wrapper)

@task
@runs_once
def nodes():
    '''
    List running nodes on all enabled cloud providers. Automatically flushes caches
    '''
    for name, provider in env.providers.items():
        print name
        provider.nodes()
        print

@task
@runs_once
def create(provider, count=1, name=None, **kwargs):
    r'''
    Create one or more cloud servers

    Args:
        * provider (str): Cloud provider, e.g. ec2, digitalocean
        * count (int) =1: Number of instances
        * name (str) =None: Name of server(s)
        * \**kwargs: Provider-specific flags
    '''
    count = int(count)
    provider = provider_by_name(provider)
    options = provider.create_server_defaults
    options.update(kwargs)
    names = [name] * count
    provider.validate_create_options(**options)
    return provider.create_servers(count, names, **options)

@cloudtask
@parallel
def terminate():
    '''
    Terminate server(s)
    '''
    print 'Sleeping for ten seconds so you can change your mind if you want to!!!'
    time.sleep(10)
    this_provider().terminate()

@cloudtask
@parallel
def reboot():
    '''
    Reboot server(s)
    '''
    this_provider().reboot()

@cloudtask
@parallel
def rename(new_name):
    '''
    Rename server(s)

    Args:
        new_name (str): New name
    '''
    this_provider().rename(new_name)

@task
@runs_once
def uncache():
    '''
    Flush the cache
    '''
    cache.flush()

@cloudtask
def ssh(cmd=''):
    '''
    SSH into the server(s) (sequentially if more than one)

    Args:
        cmd (str) ='': Command to run on the server
    '''
    with settings(warn_only=True):
        local('ssh -A -o StrictHostKeyChecking=no -i "%s" %s@%s "%s"' % (
            env.key_filename, env.user, env.host, cmd))

@cloudtask
def upload(local_path, remote_path):
    '''
    Copy a local file to one or more servers via scp

    Args:
        * local_path (str): Path on the local filesystem
        * remote_path (str): Path on the remote filesystem
    '''
    put(local_path, remote_path)

@task
@runs_once
def pricing(sort='cost'):
    '''
    Print pricing tables for all enabled providers
    '''
    for name, provider in env.providers.items():
        print name
        provider.pricing(sort)
        print

@cloudtask
@parallel
def ping():
    '''
    Ping server(s)
    '''
    local('ping -c1 %s' % env.host)

@cloudtask
@parallel
def bootstrap(directory='bootstrap', use_envtpl=False):
    '''
    Bootstrap a server by uploading files and executing scripts.
    If you have a directory called `bootstrap` (or whatever the
    `directory` argument is), upload everything in bootstrap/files/*
    to that location on the server. For example, you have have
    bootstrap/files/etc/hosts, that will get uploaded to
    /etc/hosts on the remote machine. Any files ending with *.sh
    in bootstrap will be sourced alphabetically.

    Args:
        * directory: Bootstrap directory (default='bootstrap')
        * use_envtpl: Whether to compile files suffixed with .tpl using envtpl
    '''
    for root, dirs, files in os.walk(directory):
        parents = root.split('/')
        if len(parents) > 1 and parents[1] == 'files':
            for filename in files:
                remote_root = '/' + '/'.join(parents[2:])
                sudo('mkdir -p "%s"' % remote_root)

                local_filename = '%s/%s' % (root, filename)
                remote_filename = '%s/%s' % (remote_root, filename)

                if use_envtpl and local_filename.endswith('.tpl'):
                    remote_filename = remote_filename[:-4] # remove .tpl
                    variables = os.environ
                    with open(local_filename, 'r') as f:
                        compiled = envtpl.render(f.read(), variables,
                                                 die_on_missing_variable=True)
                        put(StringIO(compiled), remote_filename, use_sudo=True)
                else:
                    put(local_filename, remote_filename, use_sudo=True)

    scripts = glob('bootstrap/*.sh')
    remote_scripts_directory = '/tmp/bootstrap_scripts'
    sudo('mkdir -p %s' % remote_scripts_directory)
    for path in sorted(scripts):
        filename = os.path.basename(path)
        remote_script = '%s/%s' % (remote_scripts_directory, filename)
        put('bootstrap/%s' % filename, remote_script, use_sudo=True)
        run('source %s' % remote_script)
