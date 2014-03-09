import sys
import re
import urllib
import dop.client
import time

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

import headintheclouds
from headintheclouds import util, cache

create_server_defaults = {
    'size': '512MB',
    'placement': 'New York 1',
    'image': 'Ubuntu 12.04.3 x64',
}

__all__ = []

def create_servers(count, names=None, size=None, placement=None, image=None):
    count = int(count)
    assert count == len(names)

    size_id = _get_size_id(size)
    image_id = _get_image_id(image)
    region_id = _get_region_id(placement)
    ssh_key_id = str(_get_ssh_key_id(SSH_KEY_NAME))

    print 'Creating %d Digital Ocean %s droplets' % (count, size)

    droplet_ids = []
    for i in range(count):
        if names:
            name = names[i]
        else:
            name = 'unnamed-%d' % i
        name = '%s%s' % (env.name_prefix, name)
        droplet = _do().create_droplet(
            name, size_id, image_id, region_id, [ssh_key_id])
        droplet_ids.append(droplet.id)

    while True:
        nodes = cache.recache(all_nodes)
        node_map = {n['id']: n for n in nodes}
        n_active = 0
        for id in droplet_ids:
            if (id in node_map
                and node_map[id]['state'] == 'active'
                and node_map[id]['ip'] != -1):
                n_active += 1
        print 'Waiting for droplet%s to start [pending: %d, running: %d]' % (
            's' if count > 1 else '',
            count - n_active, n_active)

        # do the sleeping here instead of after because it takes a couple
        # seconds for ssh to come up. TODO: an actual ssh check
        time.sleep(5)

        if n_active == count:
            break

    return [node_map[id] for id in droplet_ids]

def validate_create_options(size, placement, image):
    _get_size_id(size)
    _get_image_id(image)
    _get_region_id(placement)
    return {}

def terminate():
    current_node = _host_node()
    if not current_node:
        return
    droplet_id = current_node['id']
    puts('Terminating Digital Ocean droplet %s' % droplet_id)
    _do().destroy_droplet(droplet_id)
    cache.uncache(all_nodes)

def nodes():
    nodes = cache.recache(all_nodes)
    util.print_table(nodes, ['name', 'size', 'ip', 'state'], sort='name')

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

def rename(name):
    current_node = _host_node()
    name = env.name_prefix + name
    response = _do().request('/droplets/%s/rename?name=%s' % (
        current_node['id'], urllib.quote_plus(name)))
    if response['status'] != 'OK':
        raise Exception('Rename failed: %s' % repr(response))
    cache.uncache(all_nodes)

def _do():
    if not hasattr(_do, 'client'):
        _do.client = dop.client.Client(CLIENT_ID, API_KEY)
    return _do.client

def _host_node():
    nodes = [x for x in all_nodes() if x['ip'] == env.host]
    if nodes:
        return nodes[0]
    return None

@cache.cached
def all_nodes():
    nodes = [droplet_to_node(x) for x in _do().show_active_droplets()
             if x.name.startswith(env.name_prefix)]
    return nodes

def droplet_to_node(droplet):

    def flip_dict(d):
        d = {v: k for k, v in d.items()}
        return d

    node = {}
    node['id'] = droplet.id
    node['name'] = re.sub('^%s' % env.name_prefix, '', droplet.name)
    node['size'] = flip_dict(_get_sizes())[droplet.size_id]
    node['placement'] = flip_dict(_get_regions())[droplet.region_id]
    try:
        node['image'] = flip_dict(_get_images())[droplet.image_id]
    except Exception:
        deprecated_image_ids = {284203: 'Ubuntu 12.04'}
        node['image'] = deprecated_image_ids.get(droplet.image_id, 'unknown')
    node['ip'] = droplet.ip_address
    node['internal_address'] = droplet.ip_address
    node['internal_ip'] = droplet.ip_address
    node['state'] = droplet.status
    node['running'] = droplet.status == 'active'

    return node

def equivalent_create_options(options1, options2):
    options1 = options1.copy()
    options2 = options2.copy()

    try:
        options1['size'] = _get_sizes()[options1['size']]
    except Exception:
        pass
    try:
        options1['placement'] = _get_regions()[options1['placement']]
    except Exception:
        pass
    try:
        options1['image'] = _get_images()[options1['image']]
    except Exception:
        pass
    try:
        options2['size'] = _get_sizes()[options2['size']]
    except Exception:
        pass
    try:
        options2['placement'] = _get_regions()[options2['placement']]
    except Exception:
        pass
    try:
        options2['image'] = _get_images()[options2['image']]
    except Exception:
        pass

    return (options1['size'] == options2['size']
            and options1['placement'] == options2['placement']
            and options1['image'] == options2['image'])

@cache.cached
def _get_sizes():
    sizes = [x.to_json() for x in _do().sizes()]
    sizes = {x['name']: x['id'] for x in sizes}
    return sizes

@cache.cached
def _get_images():
    images = [s.to_json() for s in _do().images()]
    images = {s['name']: s['id'] for s in images}
    return images

@cache.cached
def _get_regions():
    regions = [x.to_json() for x in _do().regions()]
    regions = {x['name']: x['id'] for x in regions}
    return regions

@cache.cached
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

CLIENT_ID = util.env_var('DIGITAL_OCEAN_CLIENT_ID')
API_KEY = util.env_var('DIGITAL_OCEAN_API_KEY')
SSH_KEY_FILENAME = util.env_var('DIGITAL_OCEAN_SSH_KEY_FILENAME')
SSH_KEY_NAME = util.env_var('DIGITAL_OCEAN_SSH_KEY_NAME')

provider_name = 'Digital Ocean'
settings = {
    'user': 'root',
    'key_filename': SSH_KEY_FILENAME,
}

headintheclouds.add_provider('digitalocean', sys.modules[__name__])
