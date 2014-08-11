import time
from functools import wraps
from fabric.api import * # pylint: disable=W0614,W0401

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
        local_path (str): Path on the local filesystem
        remote_path (str): Path on the remote filesystem
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
#@parallel
def ping():
    '''
    Ping server(s)
    '''
    local('ping -c1 %s' % env.host)
