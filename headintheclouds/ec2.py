import boto.ec2
import boto
from fabric.api import *
import fabric
import datetime
import dateutil.parser
import signal
import time
import sys
import os
import fabric.contrib.project as project
import cPickle
import re
from collections import defaultdict

import util
from util import cached, recache, uncache

@runs_once
@task
def pricing():

    now = datetime.datetime.now()
    one_day_ago = now - datetime.timedelta(days=1)
    price_history = _ec2().get_spot_price_history(
        start_time=one_day_ago.isoformat(),
        end_time=now.isoformat(),
        product_description='Linux/UNIX',
        availability_zone='us-east-1b',
    )

    data = {}
    latest_price = {}
    latest_time = {}
    for item in price_history:
        t = item.instance_type

        if t not in data:
            data[t] = []
            latest_time[t] = item.timestamp
            latest_price[t] = item.price
        else:
            if latest_time[t] < item.timestamp:
                latest_time[t] = item.timestamp
                latest_price[t] = item.price

        data[t].append(item.price)

    table = []
    for t, prices in data.iteritems():
        item = {}
        item['size'] = t
        item['recent'] = '%.3f' % latest_price[t]
        item['median'] = '%.3f' % util.median(prices)
        item['stddev'] = '%.3f' % util.stddev(prices)
        item['max'] = '%.3f' % max(prices)
        item.update(get_node_types()[t])
        item['hourly_cost'] = '%.3f' % item['linux_cost']
        table.append(item)

    table.sort(key=lambda x: x['linux_cost'])

    util.print_table(table, ['size', 'compute_units', 'memory', 'recent',
                             'median', 'stddev', 'max', 'hourly_cost'])
    
@task
@runs_once
def create(role='idle', size='m1.small', count=1):
    image_id = _get_image_id_for_size(size)
    count = int(count)

    reservation = _ec2().run_instances(
        image_id=image_id,
        min_count=count,
        max_count=count,
        security_groups=['default'],
        instance_type=size,
        placement='us-east-1b',
        key_name=KEYPAIR_NAME,
    )

    for instance in reservation.instances:
        _set_instance_name(instance.id, role)

    puts('Created %d %s instance(s)' % (count, size))

@task
@runs_once
def spot(role='idle', size='m1.small', price=0.010, count=1):
    count = int(count)
    price = float(price)

    image_id = _get_image_id_for_size(size)

    puts('Creating spot requests for %d %s instance(s) at $%.3f' % (count, size, price))
    requests = _ec2().request_spot_instances(
        price=price,
        image_id=image_id,
        count=count,
        security_groups=['default'],
        instance_type=size,
        placement='us-east-1b',
        key_name=KEYPAIR_NAME,
    )

    request_ids = [r.id for r in requests]

    def sigint_handler(signum, frame, terminate=True):
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        print 'Caught SIGINT'
        requests = _ec2().get_all_spot_instance_requests(request_ids)
        print 'Cancelling spot requests'
        for r in requests:
            r.cancel()

        instance_ids = [r.instance_id for r in requests if r.instance_id is not None]

        if terminate and instance_ids:
            print 'Terminating instances'
            _ec2().terminate_instances(instance_ids)

        sys.exit(1)

    signal.signal(signal.SIGINT, sigint_handler)

    while True:
        requests = _ec2().get_all_spot_instance_requests(request_ids)
        if not all([r.state == 'open' for r in requests]):
            break

        print 'Waiting for spot requests to be fulfilled [%s]' % (
            ', '.join([r.status.code for r in requests]))
        time.sleep(5)

    print 'Spot request statuses: [%s]' % (
        ', '.join([r.status.code for r in requests]))

    active_requests = [r for r in requests if r.state == 'active']
    if not active_requests:
        print 'No requests succeeded, giving up'

    instance_ids = [r.instance_id for r in active_requests]
    for i, instance_id in enumerate(instance_ids):
        _set_instance_name(instance_id, role)

    uncache(_get_all_nodes)

@task
@parallel
def terminate():
    instance_id = _host_node()['id']
    puts('Terminating EC2 instance %s' % instance_id)
    _ec2().terminate_instances([instance_id])
    uncache(_get_all_nodes)

@task
@runs_once
def nodes():
    nodes = recache(_get_all_nodes)
    util.print_table(nodes, ['name', 'size', 'ip_address', 'status', 'launch_time'])

def rename(role):
    current_node = _host_node()
    _set_instance_name(current_node['id'], role)

def get_local_environment():
    nodes = _get_all_nodes()
    environment = defaultdict(list)
    for node in nodes:
        environment[node['role']].append(node['private_ip_address'])
    return environment

def get_remote_environment():
    nodes = _get_all_nodes()
    environment = defaultdict(list)
    for node in nodes:
        environment[node['role']].append(node['ip_address'])
    return environment

def _ec2():
    if not hasattr(_ec2, 'client'):
        _ec2.client = boto.ec2.connection.EC2Connection(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
    return _ec2.client

@cached
def _get_all_nodes():

    def format_node(node):
        state = node.state
        node = node.__dict__
        del node['groups']
        del node['block_device_mapping']
        launch_time = dateutil.parser.parse(node['launch_time'])
        node['launch_time'] = launch_time.astimezone(dateutil.tz.tzlocal())
        node['name'] = node['tags']['Name']
        node['name'] = re.sub('^%s' % util.NAME_PREFIX, '', node['name'])
        node['role'] = re.sub('^(.+)$', r'\1', node['name'])
        node['size'] = node['instance_type']
        node['state'] = state
        node['status'] = state
        node['provider'] = __name__
        return node

    reservations = _ec2().get_all_instances()
    nodes = [format_node(x) for r in reservations for x in r.instances
             if 'Name' in x.tags and x.tags['Name'].startswith(util.NAME_PREFIX)]
    return nodes

def _host_node():
    return [x for x in _get_all_nodes() if x['ip_address'] == env.host][0]

def _host_role():
    return _host_node()['role']

def _set_instance_name(instance_id, name):
    _ec2().create_tags(instance_id, {'Name': '%s%s' % (util.NAME_PREFIX, name)})

def _get_image_id_for_size(size):
    ubuntu1304_ebs = 'ami-10314d79'
    ubuntu1304_instance_store = 'ami-762d491f'
    ubuntu1304_hvm = 'ami-08345061'

    if size in ['cc2.8xlarge', 'cr1.8xlarge']:
        image_id = ubuntu1304_hvm
    elif size in ['t1.micro']:
        image_id = ubuntu1304_ebs
    else:
        image_id = ubuntu1304_instance_store

    return image_id

ACCESS_KEY_ID = util.env_var('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = util.env_var('AWS_SECRET_ACCESS_KEY')
SSH_KEY_FILENAME = util.env_var('AWS_SSH_KEY_FILENAME')
KEYPAIR_NAME = util.env_var('AWS_KEYPAIR_NAME')

if not hasattr(env, 'all_nodes'):
    env.all_nodes = {}
env.all_nodes.update({x['ip_address']: x for x in _get_all_nodes() if x['ip_address']})
if not hasattr(env, 'providers'):
    env.providers = [__name__]
else:
    env.providers.append(__name__)

provider_name = 'Amazon EC2'
settings = {
    'user': 'ubuntu',
    'key_filename': SSH_KEY_FILENAME,
}

def get_node_types():
    return {
        'm1.small': {
            'name': 'M1 Small',
            'memory': 1.7,
            'compute_units': 1,
            'storage': 160,
            'ioperf': 'Moderate',
            'architecture': '32/64-bit',
            'maxips': 8,
            'linux_cost': 0.060,
            'windows_cost': 0.091,
        },
        'm1.medium': {
            'name': 'M1 Medium',
            'memory': 3.75,
            'compute_units': 2,
            'storage': 410,
            'ioperf': 'Moderate',
            'architecture': '32/64-bit',
            'maxips': 12,
            'linux_cost': 0.12,
            'windows_cost': 0.182,
        },
        'm1.large': {
            'name': 'M1 Large',
            'memory': 7.5,
            'compute_units': 4,
            'storage': 850,
            'ioperf': 'High / 500 Mbps',
            'architecture': '64-bit',
            'maxips': 30,
            'linux_cost': 0.24,
            'windows_cost': 0.364,
        },
        'm1.xlarge': {
            'name': 'M1 Extra Large',
            'memory': 15,
            'compute_units': 8,
            'storage': 1690,
            'ioperf': 'High / 1000 Mbps',
            'architecture': '64-bit',
            'maxips': 60,
            'linux_cost': 0.48,
            'windows_cost': 0.728,
        },
        't1.micro': {
            'name': 'Micro',
            'memory': 0.6,
            'compute_units': 2,
            'storage': 0,
            'ioperf': 'Low',
            'architecture': '32/64-bit',
            'maxips': 1,
            'linux_cost': 0.02,
            'windows_cost': 0.02,
        },
        'm2.xlarge': {
            'name': 'High-Memory Extra Large',
            'memory': 17.10,
            'compute_units': 6.5,
            'storage': 420,
            'ioperf': 'Moderate',
            'architecture': '64-bit',
            'maxips': 60,
            'linux_cost': 0.41,
            'windows_cost': 0.51,
        },
        'm2.2xlarge': {
            'name': 'High-Memory Double Extra Large',
            'memory': 34.2,
            'compute_units': 13,
            'storage': 850,
            'ioperf': 'High',
            'architecture': '64-bit',
            'maxips': 120,
            'linux_cost': 0.82,
            'windows_cost': 1.02,
        },
        'm2.4xlarge': {
            'name': 'High-Memory Quadruple Extra Large',
            'memory': 68.4,
            'compute_units': 26,
            'storage': 1690,
            'ioperf': 'High / 1000 Mbps',
            'architecture': '64-bit',
            'maxips': 240,
            'linux_cost': 1.64,
            'windows_cost': 2.04,
        },
        'm3.xlarge': {
            'name': 'M3 Extra Large',
            'memory': 15,
            'compute_units': 13,
            'storage': 0,
            'ioperf': 'Moderate / 500 Mbps',
            'architecture': '64-bit',
            'maxips': 60,
            'linux_cost': 0.50,
            'windows_cost': 0.78,
        },
        'm3.2xlarge': {
            'name': 'M3 Double Extra Large',
            'memory': 30,
            'compute_units': 26,
            'storage': 0,
            'ioperf': 'High / 1000 Mbps',
            'architecture': '64-bit',
            'maxips': 120,
            'linux_cost': 1.00,
            'windows_cost': 1.56,
        },
        'c1.medium': {
            'name': 'High-CPU Medium',
            'memory': 1.7,
            'compute_units': 5,
            'storage': 350,
            'ioperf': 'Moderate',
            'architecture': '32/64-bit',
            'maxips': 12,
            'linux_cost': 0.145,
            'windows_cost': 0.225,
        },
        'c1.xlarge': {
            'name': 'High-CPU Extra Large',
            'memory': 7,
            'compute_units': 20,
            'storage': 1690,
            'ioperf': 'High / 1000 Mbps',
            'architecture': '64-bit',
            'maxips': 60,
            'linux_cost': 0.58,
            'windows_cost': 0.90,
        },
        'cc1.4xlarge': {
            'name': 'Cluster Compute Quadruple Extra Large',
            'memory': 23,
            'compute_units': 33.5,
            'storage': 1690,
            'ioperf': '',
            'architecture': 'Xeon X5570',
            'maxips': 1,
            'linux_cost': 1.30,
            'windows_cost': 1.61,
        },
        'cc2.8xlarge': {
            'name': 'Cluster Compute Eight Extra Large',
            'memory': 60.5,
            'compute_units': 88,
            'storage': 3370,
            'ioperf': '',
            'architecture': 'Xeon E5-2670',
            'maxips': 240,
            'linux_cost': 2.40,
            'windows_cost': 2.97,
        },
        'cg1.4xlarge': {
            'name': 'Cluster GPU Quadruple Extra Large',
            'memory': 22,
            'compute_units': 33.5,
            'storage': 1690,
            'ioperf': '',
            'architecture': 'Xeon X5570',
            'maxips': 1,
            'linux_cost': 2.10,
            'windows_cost': 2.60,
        },
        'hi1.4xlarge': {
            'name': 'High I/O Quadruple Extra Large',
            'memory': 60.5,
            'compute_units': 35,
            'storage': 2048,
            'ioperf': '',
            'architecture': '64-bit',
            'maxips': 1,
            'linux_cost': 3.10,
            'windows_cost': 3.58,
        },
        'hs1.8xlarge': {
            'name': 'High Storage Eight Extra Large',
            'memory': 117.00,
            'compute_units': 35,
            'storage': 49152,
            'ioperf': '',
            'architecture': '64-bit',
            'maxips': 1,
            'linux_cost': 4.600,
            'windows_cost': 4.931,
        },
        'cr1.8xlarge': {
            'name': 'High Memory Cluster Eight Extra Large',
            'memory': 244.00,
            'compute_units': 88,
            'storage': 240,
            'ioperf': '',
            'architecture': '64-bit',
            'maxips': 1,
            'linux_cost': 3.500,
            'windows_cost': 3.831,
        },
    }
