import os
import sys
from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

import headintheclouds
from headintheclouds import util

__all__ = []

def terminate():
    raise NotImplementedError()

def pricing(sort=None):
    print 'No pricing for boot2docker'

def rename():
    raise NotImplementedError()

def nodes():
    nodes = all_nodes()
    util.print_table(nodes, ['ip'])

def create_servers(*args, **kwargs):
    raise NotImplementedError()

def validate_create_options(ip):
    return {'running': True}

def equivalent_create_options(options1, options2):
    return options1['ip'] == options2['ip']

create_server_defaults = {
    'ip': None
}

def get_boot2docker_ip():
    with hide('everything'):
        return local('boot2docker ip', capture=True)

def get_boot2docker_ssh_port():
    return 22
    # TODO:
    # with hide('everything'):
    #    return local("boot2docker config | awk '/SSHPort/ { print $3 }'", capture=True)

def get_boot2docker_ssh_key():
    with hide('everything'):
        key = local("boot2docker config | awk '/SSHKey/ { print $3 }'", capture=True)
        return key.replace('"', '')

def all_nodes():
    name = 'boot2docker'
    ip = get_boot2docker_ip()
    nodes = [{
        'name': name,
        'ip': ip,
        'internal_address': ip,
        'internal_ip': ip,
        'running': True,
    }]
    return nodes

settings = {
    'user': 'docker',
    'key_filename': get_boot2docker_ssh_key(),
    'port': get_boot2docker_ssh_port(),
    'shell': '/bin/sh -c',
}

headintheclouds.add_provider('boot2docker', sys.modules[__name__])
