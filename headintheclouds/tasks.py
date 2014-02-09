from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

from headintheclouds import cloudtask, provider_by_name, this_provider
from headintheclouds import cache

@task
@runs_once
def pricing():
    for name, provider in env.providers.items():
        print name
        provider.pricing()

@task
@runs_once
def nodes():
    for name, provider in env.providers.items():
        print name
        provider.nodes()

@task
@runs_once
def create(provider, count=1, name=None, **kwargs):
    count = int(count)
    provider = provider_by_name(provider)
    options = provider.create_server_defaults
    options.update(kwargs)
    names = [name] * count
    provider.validate_create_options(**options)
    provider.create_servers(count, names, **options)

@cloudtask
@parallel
def terminate():
    this_provider().terminate()

@cloudtask
@parallel
def reboot():
    this_provider().reboot()

@cloudtask
@parallel
def rename(role):
    this_provider().rename(role)

@cloudtask
def uncache():
    cache.flush()

@cloudtask
def ssh(cmd=''):
    local('ssh -o StrictHostKeyChecking=no -i "%s" %s@%s "%s"' % (
        env.key_filename, env.user, env.host, cmd))

@cloudtask
def mosh():
    local('mosh --ssh="ssh -o StrictHostKeyChecking=no -i \"%s\"" %s@%s' % (
        env.key_filename, env.user, env.host))

@task
@runs_once
def tunnel(local_port, remote_port=None):
    if remote_port is None:
        remote_port = local_port
    local('ssh -o StrictHostKeyChecking=no -i "%(key)s" -f %(user)s@%(host)s -L %(local_port)s:localhost:%(remote_port)s -N' % {
        'key': env.key_filename,
        'user': env.user,
        'host': env.host,
        'local_port': local_port,
        'remote_port': remote_port
    })

@cloudtask
#@parallel
def ping():
    local('ping -c1 %s' % env.host)

