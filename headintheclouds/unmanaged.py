import os
import sys
from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

import headintheclouds
from headintheclouds import util

__all__ = []

SERVERS_LIST_FILENAME = 'unmanaged_servers.txt'

def terminate():
    raise NotImplementedError()

def pricing(sort=None):
    print 'No pricing for unmanaged servers'

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
settings = {
    'user': util.env_var('HITC_SSH_USER', 'root'),
    'key_filename': util.env_var('HITC_KEY_FILENAME', os.path.expanduser('~/.ssh/id_rsa')),
}

def all_nodes():
    nodes = []
    with open(SERVERS_LIST_FILENAME, 'r') as f:
        for ip in f:
            ip = ip.strip()
            nodes.append({
                'name': ip,
                'ip': ip,
                'internal_address': ip,
                'internal_ip': ip,
                'running': True,
            })

    return nodes

headintheclouds.add_provider('unmanaged', sys.modules[__name__])
