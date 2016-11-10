import getpass
import random
import time
from collections import Counter
import sys
import dateutil
import re
from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

import headintheclouds
from headintheclouds import util, cache
from headintheclouds.tasks import cloudtask

__all__ = ['create_image']

@cloudtask
def terminate_and_create_image(name):
    '''
    Create an image from a terminated host (with auto_delete_boot_disk=False)

    Args:
        name: The name of the image
    '''
    node = _host_node()
    _gcp().instances().delete(project=DEFAULT_PROJECT, zone=DEFAULT_ZONE,
                              instance=node['real_name']).execute()
    time.sleep(1)
    
    body = {
        'name': name,
        'sourceDisk': node['source_disk'],
    }
    print body

    operation = _gcp().images().insert(project=DEFAULT_PROJECT, body=body).execute()
    while True:
        status = get_global_operation_status(operation=operation)
        if status == 'DONE':
            break

        print 'Creating image [OPERATION %s]' % status
        time.sleep(5)

    print 'Created image: %s' % operation['targetLink']

def pricing(sort):
    types = _gcp().machineTypes().list(project=DEFAULT_PROJECT, zone=DEFAULT_ZONE).execute()['items']

    def gpu_regex(m, g):
        match = re.search(r'^[0-9]+ vCPUs?, [0-9]+ GB RAM, and (?P<gpu_cores>[0-9]+) dies? of (?P<gpu_type>.+) with (?P<gpu_ram>.+) of RAM', m['description'])
        if match:
            return match.group(g)
        return ''

    table = [{'name': m['name'],
              'cpu_cores': m['guestCpus'],
              'ram': '%.2fGB' % (m['memoryMb'] / 1024.0),
              'gpu_type': gpu_regex(m, 'gpu_type'),
              'gpu_cores': gpu_regex(m, 'gpu_cores'),
              'gpu_ram': gpu_regex(m, 'gpu_ram')}
             for m in types]
            
    util.print_table(table, ['name', 'cpu_cores', 'ram', 'gpu_type', 'gpu_cores', 'gpu_ram'], sort='cpu_cores')

def nodes():
    util.print_table(cache.recache(all_nodes),
                     ['name', 'type', 'ip', 'internal_ip', 'status', 'created'], sort='name')

@cache.cached
def all_nodes():
    instances = _gcp().instances().list(project=DEFAULT_PROJECT, zone=DEFAULT_ZONE).execute()['items']
    nodes = [instance_to_node(instance)
             for instance in instances
             if instance['name'].startswith(env.name_prefix)
             and instance['status'] not in ['STOPPING', 'TERMINATED']]
    return nodes

def create_servers(count, names, type, image, network,
                   auto_delete_boot_disk, on_host_maintenance,
                   boot_disk_size_gb):

    count = int(count)
    assert count == len(names)

    if count > 1:
        print 'Creating %d GCP %s instances' % (count, type)
    else:
        print 'Creating GCP %s instance' % type

    names = [name or random_name() for name in names]

    operations = []
    for i, name in enumerate(names):
        body = {
            'name': name_with_prefix(name),
            'machineType': 'zones/%s/machineTypes/%s' % (DEFAULT_ZONE, type),
            'disks': [
                {
                    'boot': True,
                    'autoDelete': auto_delete_boot_disk,
                    'initializeParams': {
                        'sourceImage': image,
                        'diskSizeGb': boot_disk_size_gb,
                    },
                },
            ],
            'networkInterfaces': [
                {
                    'network': network,
                    'accessConfigs': [
                        {
                            'type': 'ONE_TO_ONE_NAT',
                            'name': 'External NAT',
                        }
                    ],
                },
            ],
            'scheduling': {
                'onHostMaintenance': on_host_maintenance,
            }
        }
        operation = _gcp().instances().insert(project=DEFAULT_PROJECT, zone=DEFAULT_ZONE, body=body).execute()
        operations.append(operation)

    while True:
        statuses = ['%s' % get_zone_operation_status(operation=operation)
                    for operation in operations]
        status_counts = Counter(statuses)

        if all(s == 'DONE' for s in statuses):
            break

        if count > 1:
            print 'Waiting for instances to start [%s]' % (
                ', '.join(['OPERATION %s: %d' % (s, c) for s, c in status_counts.most_common()]))
        else:
            print 'Waiting for instance to start [OPERATION %s]' % (statuses[0])

        time.sleep(5)

    wait_for_instances_to_become_accessible(names)

    for name in names:
        local('gcloud compute --project "%s" ssh --zone "%s" "%s" -- exit' %
              (DEFAULT_PROJECT, DEFAULT_ZONE, name_with_prefix(name)))

    nodes = [n for n in all_nodes() if n['name'] in names]
    return nodes

def get_zone_operation_status(operation):
    return _gcp().zoneOperations().get(
        project=DEFAULT_PROJECT, zone=DEFAULT_ZONE, operation=operation['name']).execute()['status']

def get_global_operation_status(operation):
    return _gcp().globalOperations().get(
        project=DEFAULT_PROJECT, operation=operation['name']).execute()['status']

def wait_for_instances_to_become_accessible(names):
    while True:
        nodes_ready = 0
        nodes = cache.recache(all_nodes)
        nodes = [n for n in nodes if n['name'] in names]
        for node in nodes:
            with fab.settings(hide('everything'), warn_only=True):
                result = local('nc -w 5 -zvv %s 22' % node['ip'])
            if result.return_code == 0:
                nodes_ready += 1
        if nodes_ready == len(names):
            return

        print 'Waiting for instance%s to become accessible' % (
            's' if len(names) > 1 else '')
        time.sleep(5)

def validate_create_options(type, image, network,
                            auto_delete_boot_disk, on_host_maintenance,
                            boot_disk_size_gb):
    if type is None:
        raise Exception('You need to specify a type')

    if image is None:
        raise Exception('You need to specify an image')

    return {}

def terminate():
    print 'Terminating GCP instance %s' % _host_node()['real_name']
    _gcp().instances().delete(
        project=DEFAULT_PROJECT, zone=DEFAULT_ZONE,
        instance=_host_node()['real_name']).execute()
    time.sleep(1)
    cache.uncache(all_nodes)

def instance_to_node(instance):
    node = {}
    boot_disk = instance_get_boot_disk(instance)
    node['id'] = instance['id']
    node['name'] = re.sub('^%s' % env.name_prefix, '', instance['name'])
    node['real_name'] = instance['name']
    node['type'] = instance_get_type(instance)
    node['image'] = boot_disk['sourceImage'].replace('https://www.googleapis.com/compute/v1/', '')
    node['status'] = instance['status']
    node['running'] = instance['status'] == 'RUNNING'
    created = dateutil.parser.parse(instance['creationTimestamp'])
    node['created'] = created.astimezone(dateutil.tz.tzlocal())
    node['ip'] = instance['networkInterfaces'][0]['accessConfigs'][0].get('natIP', None)
    node['internal_ip'] = instance['networkInterfaces'][0]['networkIP']
    node['network'] = instance['networkInterfaces'][0]['network']
    node['on_host_maintenance'] = instance['scheduling']['onHostMaintenance']
    node['auto_delete_boot_disk'] = boot_disk['autoDelete']
    node['boot_disk_size_gb'] = float(boot_disk['sizeGb'])
    node['source_disk'] = boot_disk['source']
    return node

def instance_get_boot_disk(instance):
    boot_disk = [d for d in instance['disks'] if d['boot']][0]
    disk_name = boot_disk['source'].split('/')[-1]
    disk = _gcp().disks().get(project=DEFAULT_PROJECT, zone=DEFAULT_ZONE, disk=disk_name).execute()
    boot_disk.update(disk)
    return boot_disk

def instance_get_type(instance):
    return instance['machineType'].split('/')[-1]

def name_with_prefix(name):
    return '%s%s' % (env.name_prefix, name)

def random_name():
    return 'unnamed-%d' % random.randint(0, 2 ** 31)

def equivalent_create_options(options1, options2):
    options1 = options1.copy()
    options2 = options2.copy()

    return (
        options1['image'] == options2['image']
        and options1['type'] == options2['type']
        and options1['network'] == options2['network']
        and options1['auto_delete_boot_disk'] == options2['auto_delete_boot_disk']
        and options1['on_host_maintenance'] == options2['on_host_maintenance']
        and options1['boot_disk_size_gb'] == options2['boot_disk_size_gb']
    )

def _host_node():
    return [x for x in all_nodes() if x['ip'] == env.host][0]

def _gcp():
    if not hasattr(_gcp, 'client'):
        credentials = GoogleCredentials.get_application_default()
        _gcp.client = discovery.build('compute', 'v1', credentials=credentials)
    return _gcp.client

DEFAULT_PROJECT = util.env_var('GCP_DEFAULT_PROJECT')
DEFAULT_ZONE = util.env_var('GCP_DEFAULT_ZONE')
SSH_KEY_FILENAME = util.env_var('GCP_SSH_KEY_FILENAME')

create_server_defaults = {
    'image': None,
    'type': None,
    'network': 'https://www.googleapis.com/compute/v1/projects/%s/global/networks/default' % DEFAULT_PROJECT,
    'auto_delete_boot_disk': True,
    'on_host_maintenance': 'MIGRATE',
    'boot_disk_size_gb': 20
}

settings = {
    'user': getpass.getuser(),
    'key_filename': SSH_KEY_FILENAME,
}

headintheclouds.add_provider('gcp', sys.modules[__name__])
