import sys
import re
import collections

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

from headintheclouds import unknown_provider

env.disable_known_hosts = True
env.node_providers = {}
env.providers = {}
env.roledefs = collections.defaultdict(list)
env.name_prefix = getattr(env, 'name_prefix', 'HITC-')

# hack tocheck if the user has provided -H option
_has_hosts_option = bool(env.hosts)

def add_provider(name, module):
    for node in module.all_nodes():
        if not node.get('ip', None):
            continue

        ip = node['ip']
        role = re.sub('-[0-9]+$', '', node['name'])
        env.roledefs[role].append(ip)

        if env.roles:
            if role in env.roles:
                env.hosts.append(ip)
        elif not _has_hosts_option:
            env.hosts.append(ip)

        env.node_providers[ip] = module

    env.providers[name] = module

def provider_by_name(name):
    if name is None:
        return unknown_provider
    elif name in env.providers:
        return env.providers[name]
    else:
        raise ValueError('Unknown server provider: "%s"' % name)

def provider_settings():
    if not env.host:
        return fab.settings()

    settings = this_provider().settings
    return fab.settings(**settings)

def this_provider():
    if hasattr(env, 'provider'):
        return provider_by_name(env.provider)
    else:
        if env.host in env.node_providers:
            return env.node_providers[env.host]
        return unknown_provider

def all_nodes():
    nodes = []
    for name, provider in env.providers.items():
        for node in provider.all_nodes():
            node['provider'] = name
            nodes.append(node)
    return nodes
