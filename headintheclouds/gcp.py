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

__all__ = ['foo']

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

    if not network:
        network = 'https://www.googleapis.com/compute/v1/projects/%s/global/networks/default' % DEFAULT_PROJECT

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
        statuses = ['%s' % get_operation_status(operation=operation)
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

@task
@runs_once
def foo():
    gcloud_compute('config-ssh', name_with_prefix('test5'))

def get_operation_status(operation):
    return _gcp().zoneOperations().get(
        project=DEFAULT_PROJECT, zone=DEFAULT_ZONE, operation=operation['name']).execute()['status']

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
    node['id'] = instance['id']
    node['name'] = re.sub('^%s' % env.name_prefix, '', instance['name'])
    node['real_name'] = instance['name']
    node['type'] = instance_get_type(instance)
    node['image'] = instance_get_image(instance)
    node['status'] = instance['status']
    node['running'] = instance['status'] == 'RUNNING'
    created = dateutil.parser.parse(instance['creationTimestamp'])
    node['created'] = created.astimezone(dateutil.tz.tzlocal())
    node['ip'] = instance['networkInterfaces'][0]['accessConfigs'][0].get('natIP', None)
    node['internal_ip'] = instance['networkInterfaces'][0]['networkIP']
    return node

def instance_get_image(instance):
    disk_name = instance['disks'][0]['source'].split('/')[-1]
    disk = _gcp().disks().get(project=DEFAULT_PROJECT, zone=DEFAULT_ZONE, disk=disk_name).execute()
    return disk['sourceImage']

def instance_get_type(instance):
    return instance['machineType'].split('/')[-1]

def name_with_prefix(name):
    return '%s%s' % (env.name_prefix, name)

def random_name():
    return 'unnamed-%d' % random.randint(0, 2 ** 31)

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
    'network': None,
    'auto_delete_boot_disk': True,
    'on_host_maintenance': 'MIGRATE',
    'boot_disk_size_gb': 20
}

settings = {
    'user': getpass.getuser(),
    'key_filename': SSH_KEY_FILENAME,
}

headintheclouds.add_provider('gcp', sys.modules[__name__])
