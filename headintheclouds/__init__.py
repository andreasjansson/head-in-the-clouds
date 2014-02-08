import sys
from functools import wraps
import collections

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

env.disable_known_hosts = True
env.node_providers = {}
env.providers = {}
env.roledefs = collections.defaultdict(list)
env.hosts = []

def cloudtask(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with provider_settings():
            func(*args, **kwargs)
    return task(wrapper)

def add_provider(name, readable_name):
    module = sys.modules[name]
    for node in module.all_nodes():
        if not node.get('ip_address', None):
            continue

        ip = node['ip_address']
        role = node['name']
        env.roledefs[role].append(ip)

        if env.roles:
            if role in env.roles:
                env.hosts.append(ip)
        else:
            env.hosts.append(ip)

        env.node_providers[ip] = module

    env.providers[readable_name] = module

def server_provider(name):
    if name is None:
        import unknown_provider
        return unknown_provider
    elif name == 'ec2':
        if 'headintheclouds.ec2' not in sys.modules:
            raise ValueError('Unknown provider "ec2"')
        return sys.modules['headintheclouds.ec2']
    elif name == 'digitalocean':
        if 'headintheclouds.digitalocean' not in sys.modules:
            raise ValueError('Unknown provider "digitalocean"')
        return sys.modules['headintheclouds.digitalocean']
    else:
        raise ValueError('Unknown server provider: "%s"' % name)

def provider_settings():
    if not env.host:
        return fab.settings()

    settings = this_provider().settings
    return fab.settings(**settings)

def this_provider():
    if hasattr(env, 'provider'):
        return server_provider(env.provider)
    else:
        return env.node_providers[env.host]
