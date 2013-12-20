import os
import sys
import re
import urllib
import dop.client
from collections import defaultdict

from fabric.api import *
from fabric.contrib.console import confirm

import util
from util import cached, recache, uncache, autodoc

@task
@runs_once
@autodoc
def create(role='idle', size='512MB', count=1):

    image = 'Ubuntu 12.04 x64'
    region = 'New York 1'

    size_id = _get_size_id(size)
    image_id = _get_image_id(image)
    region_id = _get_region_id(region)
    ssh_key_id = str(_get_ssh_key_id(SSH_KEY_NAME))

    for _ in range(int(count)):
        name = '%s%s' % (env.name_prefix, role)
        image = _do().create_droplet(name, size_id, image_id, region_id, [ssh_key_id])

    uncache(_get_all_nodes)

@task
@parallel
def terminate():
    current_node = _host_node()
    if not current_node:
        return
    droplet_id = current_node['id']
    puts('Terminating Digital Ocean droplet %s' % droplet_id)
    _do().destroy_droplet(droplet_id)
    uncache(_get_all_nodes)

@task
@runs_once
def nodes():
    nodes = recache(_get_all_nodes)
    util.print_table(sorted(nodes, key=lambda x: x['name']), ['name', 'size', 'ip_address', 'status'])

@task
@runs_once
def pricing():
    sizes = _get_sizes()
    def get_size(x):
        replacements = {'MB': '', 'GB': '000', 'TB': '000000'}
        for a, b in replacements.iteritems():
            if a in x:
                return int(x.replace(a, b))
    util.print_table([get_node_types()[s] for s in sorted(sizes, key=get_size)],
                     ['memory', 'cores', 'disk', 'transfer', 'cost'])

def rename(role):
    current_node = _host_node()
    name = env.name_prefix + role
    response = _do().request('/droplets/%s/rename?name=%s' % (
        current_node['id'], urllib.quote_plus(name)))
    if response['status'] != 'OK':
        raise Exception('Rename failed: %s' % repr(response))
    uncache(_get_all_nodes)

def get_remote_environment(running_only=False):
    nodes = _get_all_nodes()
    environment = defaultdict(list)
    for node in nodes:
        environment[node['role']].append(node['ip_address'])
    return environment
get_local_environment = get_remote_environment

def _do():
    if not hasattr(_do, 'client'):
        _do.client = dop.client.Client(CLIENT_ID, API_KEY)
    return _do.client

def _host_node():
    nodes = [x for x in _get_all_nodes() if x['ip_address'] == env.host]
    if nodes:
        return nodes[0]
    return None

@cached
def _get_all_nodes():

    def flip_dict(d):
        d = {v: k for k, v in d.items()}
        return d

    def format_node(node):
        node = node.to_json()
        node['region'] = flip_dict(_get_regions())[node['region_id']]
        node['size'] = flip_dict(_get_sizes())[node['size_id']]
        node['name'] = re.sub('^%s' % env.name_prefix, '', node['name'])
        node['role'] = re.sub('^(.+)$', r'\1', node['name'])
#        node['index'] = int(re.sub('^.+-([0-9]+)$',r'\1', node['name']))
        node['provider'] = __name__
        return node

    nodes = [format_node(x) for x in _do().show_active_droplets()
             if x.name.startswith(env.name_prefix)]
    return nodes

@cached
def _get_sizes():
    sizes = [x.to_json() for x in _do().sizes()]
    sizes = {x['name']: x['id'] for x in sizes}
    return sizes

@cached
def _get_images():
    images = [s.to_json() for s in _do().images()]
    images = {s['name']: s['id'] for s in images}
    return images

@cached
def _get_regions():
    regions = [x.to_json() for x in _do().regions()]
    regions = {x['name']: x['id'] for x in regions}
    return regions

@cached
def _get_ssh_keys():
    ssh_keys = [s.to_json() for s in _do().all_ssh_keys()]
    ssh_keys = {s['name']: s['id'] for s in ssh_keys}
    return ssh_keys

def _get_size_id(size):
    sizes = _get_sizes()
    if not size in sizes:
        raise Exception('Unknown size: %s' % size)
    return sizes[size]

def _get_image_id(image):
    images = _get_images()
    if not image in images:
        raise Exception('Unknown image: %s' % image)
    return images[image]

def _get_region_id(region):
    regions = _get_regions()
    if not region in regions:
        raise Exception('Unknown region: %s' % region)
    return regions[region]

def _get_ssh_key_id(ssh_key):
    ssh_keys = _get_ssh_keys()
    if not ssh_key in ssh_keys:
        raise Exception('Unknown ssh_key: %s' % ssh_key)
    return ssh_keys[ssh_key]

CLIENT_ID = util.env_var('DIGITAL_OCEAN_CLIENT_ID')
API_KEY = util.env_var('DIGITAL_OCEAN_API_KEY')
SSH_KEY_FILENAME = util.env_var('DIGITAL_OCEAN_SSH_KEY_FILENAME')
SSH_KEY_NAME = util.env_var('DIGITAL_OCEAN_SSH_KEY_NAME')

if not hasattr(env, 'all_nodes'):
    env.all_nodes = {}
env.all_nodes.update({x['ip_address']: x for x in _get_all_nodes() if x['ip_address']})
if not hasattr(env, 'providers'):
    env.providers = [__name__]
else:
    env.providers.append(__name__)

provider_name = 'Digital Ocean'
settings = {
    'user': 'root',
    'key_filename': SSH_KEY_FILENAME,
}

def get_node_types():
    return {
        '512MB': {
            'cost': '0.007',
            'memory': '512MB',
            'cores': '1',
            'disk': '20GB',
            'transfer': '1TB',
        },
        '1GB': {
            'cost': '0.015',
            'memory': '1GB',
            'cores': '1',
            'disk': '30GB',
            'transfer': '2TB',
        },
        '2GB': {
            'cost': '0.030',
            'memory': '2GB',
            'cores': '2',
            'disk': '40GB',
            'transfer': '3TB',
        },
        '4GB': {
            'cost': '0.060',
            'memory': '4GB',
            'cores': '2',
            'disk': '60GB',
            'transfer': '4TB',
        },
        '8GB': {
            'cost': '0.119',
            'memory': '8GB',
            'cores': '4',
            'disk': '80GB',
            'transfer': '5TB',
        },
        '16GB': {
            'cost': '0.238',
            'memory': '16GB',
            'cores': '8',
            'disk': '160GB',
            'transfer': '6TB',
        },
        '32GB': {
            'cost': '0.476',
            'memory': '32GB',
            'cores': '12',
            'disk': '320GB',
            'transfer': '7TB',
        },
        '48GB': {
            'cost': '0.705',
            'memory': '48GB',
            'cores': '16',
            'disk': '480GB',
            'transfer': '8TB',
        },
        '64GB': {
            'cost': '0.941',
            'memory': '64GB',
            'cores': '20',
            'disk': '640GB',
            'transfer': '9TB',
        },
        '96GB': {
            'cost': '1.411',
            'memory': '96GB',
            'cores': '24',
            'disk': '960GB',
            'transfer': '10TB',
        },
    }
