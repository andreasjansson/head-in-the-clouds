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
import requests

import util
from util import cached, recache, uncache, autodoc

@task
@runs_once
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
    node_types = get_node_types()
    for t, prices in data.iteritems():
        item = {}
        item['size'] = t
        item['recent'] = '%.3f' % latest_price[t]
        item['median'] = '%.3f' % util.median(prices)
        item['stddev'] = '%.3f' % util.stddev(prices)
        item['max'] = '%.3f' % max(prices)
        if t in node_types:
            item.update(node_types[t])
            item['hourly_cost'] = '%.3f' % item['linux_cost']
            table.append(item)

    table.sort(key=lambda x: x['hourly_cost'])

    util.print_table(table, ['size', 'misc', 'cores', 'memory', 'recent',
                             'median', 'stddev', 'max', 'hourly_cost'])
@task
@runs_once
@autodoc
def create(role='idle',
           size='m1.small',
           count=1,
           ubuntu_version='12.04',
           prefer_ebs=False,
           placement='us-east-1b',
           security_group='default'):

    image_id = _get_image_id_for_size(size, ubuntu_version, prefer_ebs)
    count = int(count)
    prefer_ebs = str(prefer_ebs).lower() == 'true'

    reservation = _ec2().run_instances(
        image_id=image_id,
        min_count=count,
        max_count=count,
        security_groups=[security_group],
        instance_type=size,
        placement=placement,
        key_name=KEYPAIR_NAME,
    )

    for instance in reservation.instances:
        _set_instance_name(instance.id, role)

    puts('Created %d %s instance%s' % (count, size, 's' if count > 1 else ''))

@task
@runs_once
@autodoc
def spot(role='idle',
         size='m1.small',
         price=0.010,
         count=1,
         ubuntu_version='12.04',
         prefer_ebs=False,
         placement='us-east-1b',
         security_group='default'):

    count = int(count)
    price = float(price)
    prefer_ebs = str(prefer_ebs).lower() == 'true'

    image_id = _get_image_id_for_size(size, ubuntu_version, prefer_ebs)

    puts('Creating spot requests for %d %s instance%s at $%.3f' % (count, size, 's' if count > 1 else '', price))
    requests = _ec2().request_spot_instances(
        price=price,
        image_id=image_id,
        count=count,
        security_groups=[security_group],
        instance_type=size,
        placement=placement,
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

        statuses = [r.status.code for r in requests]
        status_counts = [(status, statuses.count(status))
                         for status in sorted(set(statuses))]
        print 'Waiting for spot requests to be fulfilled [%s]' % (
            ', '.join(['%s: %d' % s for s in status_counts]))

        if all([status == 'fulfilled' for status in statuses]):
            break

        if all([status == 'price-too-low']):
            abort('Price too low')

        time.sleep(5)
        
    active_requests = [r for r in requests if r.state == 'active']
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
    util.print_table(nodes, ['name', 'size', 'ip_address', 'private_dns_name', 'status', 'launch_time'])

@task
@runs_once
@autodoc
def firewall(open=None, close=None):
    if not open or close:
        raise Exception('Please provide open and/or close arguments')
    if open:
        open = int(open)
        _ec2().authorize_security_group(
            'default', ip_protocol='tcp', from_port=open, to_port=open,
            cidr_ip='0.0.0.0/0')
    if close:
        close = int(close)
        _ec2().revoke_security_group(
            'default', ip_protocol='tcp', from_port=open, to_port=open,
            cidr_ip='0.0.0.0/0')

@task
@runs_once
def hostsfile():
    nodes = [n for n in _get_all_nodes() if n['status'] == 'running']
    util.print_table(nodes, ['ip_address', 'private_dns_name'])
        
def rename(role):
    current_node = _host_node()
    _set_instance_name(current_node['id'], role)

def get_local_environment(running_only=False):
    nodes = _get_all_nodes()
    environment = defaultdict(list)
    for node in nodes:
        if node['status'] == 'running':
            environment[node['role']].append(node['public_dns_name'])
    return environment
get_remote_environment = get_local_environment

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
        node['name'] = re.sub('^%s' % env.name_prefix, '', node['name'])
        node['role'] = re.sub('^(.+)$', r'\1', node['name'])
        node['size'] = node['instance_type']
        node['state'] = state
        node['status'] = state
        node['provider'] = __name__
        return node

    reservations = _ec2().get_all_instances()
    nodes = [format_node(x) for r in reservations for x in r.instances
             if 'Name' in x.tags and x.tags['Name'].startswith(env.name_prefix)]
    return nodes

def _host_node():
    return [x for x in _get_all_nodes() if x['ip_address'] == env.host or x['public_dns_name'] == env.host][0]

def _host_role():
    return _host_node()['role']

def _set_instance_name(instance_id, name):
    _ec2().create_tags(instance_id, {'Name': '%s%s' % (env.name_prefix, name)})

def _get_image_id_for_size(size, ubuntu_version, prefer_ebs=False):
    images = {
        ('12.04', 'ebs'):      'ami-a73264ce',
        ('12.04', 'hvm'):      'ami-b93264d0',
        ('12.04', 'instance'): 'ami-ad3660c4',
        ('12.10', 'ebs'):      'ami-2bc99d42',
        ('12.10', 'hvm'):      'ami-2dc99d44',
        ('12.10', 'instance'): 'ami-a9cf9bc0',
        ('13.04', 'ebs'):      'ami-10314d79',
        ('13.04', 'hvm'):      'ami-e1277b88',
        ('13.04', 'instance'): 'ami-762d491f',
        ('13.10', 'ebs'):      'ami-ad184ac4',
        ('13.10', 'hvm'):      'ami-a1184ac8',
        ('13.10', 'instance'): 'ami-271a484e',
    }

    if size in ['cc2.8xlarge', 'cr1.8xlarge']:
        root_store = 'hvm'
    elif size in ['t1.micro']:
        root_store = 'ebs'
    else:
        root_store = 'ebs' if prefer_ebs else 'instance'

    image_id = images.get((ubuntu_version, root_store))
    if not image_id:
        abort('Unknown Ubuntu version: %s' % ubuntu_version)

    return image_id

ACCESS_KEY_ID = util.env_var('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = util.env_var('AWS_SECRET_ACCESS_KEY')
SSH_KEY_FILENAME = util.env_var('AWS_SSH_KEY_FILENAME')
KEYPAIR_NAME = util.env_var('AWS_KEYPAIR_NAME')

if not hasattr(env, 'all_nodes'):
    env.all_nodes = {}
env.all_nodes.update({x['ip_address']: x for x in _get_all_nodes() if x['ip_address']})
env.all_nodes.update({x['public_dns_name']: x for x in _get_all_nodes() if x['public_dns_name']})
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

    memory_map = {
      'm1.small': 1700, 'm1.medium': 3750, 'm1.large': 7500, 'm1.xlarge': 15000,
      'm2.xlarge': 17100, 'm2.2xlarge': 34200, 'm2.4xlarge': 68400,
      'm3.xlarge': 15000, 'm3.2xlarge': 30000,
      'c1.medium': 1700, 'c1.xlarge': 7000,
      'hi1.4xlarge': 60500,
      'cg1.4xlarge': 22000,
      'cc1.4xlarge': 23000, 'cc2.8xlarge': 60500,
      't1.micro': 1700,
      'm3.xlarge': 15000, 'm3.xlarge': 30000,
      'cr1.8xlarge': 244000,
      'hs1.8xlarge': 117000,
      'g2.2xlarge': 15000,      
      'db.m1.small': 1700, 'db.m1.medium': 3750, 'db.m1.large': 7500, 'db.m1.xlarge': 15000,
      'db.m2.xlarge': 17100, 'db.m2.2xlarge': 34000, 'db.m2.4xlarge': 68000, 'db.cr1.8xlarge': 244000,
      'db.t1.micro': 613,
      'c3.large': 3750, 'c3.xlarge': 7000, 'c3.2xlarge': 15000, 'c3.4xlarge': 30000, 'c3.8xlarge': 60000, 
    }
    disk_map = {
      'm1.small': 160, 'm1.medium': 410, 'm1.large':850, 'm1.xlarge': 1690,
      'm2.xlarge': 420, 'm2.2xlarge': 850, 'm2.4xlarge': 1690,
      'm3.xlarge': 0, 'm3.2xlarge': 0,
      'c1.medium': 350, 'c1.xlarge': 1690,
      'hi1.4xlarge': 2048,
      'cg1.4xlarge': 1690,
      'cc1.4xlarge': 1690, 'cc2.8xlarge': 3370,
      't1.micro': 160,
      'm3.xlarge': 0, 'm3.xlarge': 0,
      'cr1.8xlarge': 240,
      'hs1.8xlarge': 48000,
      'g2.2xlarge': 60,      
      'db.m1.small': 160, 'db.m1.medium': 410, 'db.m1.large':850, 'db.m1.xlarge': 1690,
      'db.m2.xlarge': 420, 'db.m2.2xlarge': 850, 'db.m2.4xlarge': 1690, 'db.cr1.8xlarge': 1690,
      'db.t1.micro': 160,
      'c3.large': 32, 'c3.xlarge': 80, 'c3.2xlarge': 160, 'c3.4xlarge': 320, 'c3.8xlarge': 640, 
    }
    platform_map = {
      'm1.small': 32, 'm1.medium': 32, 'm1.large': 64, 'm1.xlarge': 64,
      'm2.xlarge': 64, 'm2.2xlarge': 64, 'm2.4xlarge': 64,
      'm3.xlarge': 64, 'm3.2xlarge': 64,
      'c1.medium': 32, 'c1.xlarge': 64,
      'hi1.4xlarge': 64,
      'cg1.4xlarge': 64,
      'cc1.4xlarge': 64, 'cc2.8xlarge': 64,
      't1.micro': 32,
      'm3.xlarge': 64, 'm3.xlarge': 64,
      'cr1.8xlarge': 64,
      'hs1.8xlarge': 64,
      'g2.2xlarge': 64,      
      'db.m1.small': 64, 'db.m1.medium': 64, 'db.m1.large': 64, 'db.m1.xlarge': 64,
      'db.m2.xlarge': 64, 'db.m2.2xlarge': 64, 'db.m2.4xlarge': 64, 'db.cr1.8xlarge': 64,
      'db.t1.micro': 64,
      'c3.large': 64, 'c3.xlarge': 64, 'c3.2xlarge': 64, 'c3.4xlarge': 64, 'c3.8xlarge': 64, 
    }
    compute_units_map = {
      'm1.small': 1, 'm1.medium': 2, 'm1.large': 4, 'm1.xlarge': 8,
      'm2.xlarge': 6, 'm2.2xlarge': 13, 'm2.4xlarge': 26,
      'm3.xlarge': 13, 'm3.2xlarge': 26,
      'c1.medium': 5, 'c1.xlarge': 20,
      'hi1.4xlarge': 35,
      'cg1.4xlarge': 34,
      'cc1.4xlarge': 34, 'cc2.8xlarge': 88,
      't1.micro': 2,
      'cr1.8xlarge': 88,
      'hs1.8xlarge': 35,
      'g2.2xlarge': 26,
      'unknown': 0,      
      'db.m1.small': 1, 'db.m1.medium': 2, 'db.m1.large': 4, 'db.m1.xlarge': 8,
      'db.m2.xlarge': 6.5, 'db.m2.2xlarge': 13, 'db.m2.4xlarge': 26, 'db.cr1.8xlarge': 88,
      'db.t1.micro': 1,
      'c3.large': 7, 'c3.xlarge': 14, 'c3.2xlarge': 28, 'c3.4xlarge': 55, 'c3.8xlarge': 108, 
    }
    virtual_cores_map = {
      'm1.small': 1, 'm1.medium': 1, 'm1.large': 2, 'm1.xlarge': 4,
      'm2.xlarge': 2, 'm2.2xlarge': 4, 'm2.4xlarge': 8,
      'm3.xlarge': 4, 'm3.2xlarge': 8,
      'c1.medium': 2, 'c1.xlarge': 8,
      'hi1.4xlarge': 16,
      'cg1.4xlarge': 8,
      'cc1.4xlarge': 8, 'cc2.8xlarge': 16,
      't1.micro': 0,
      'cr1.8xlarge': 16,
      'hs1.8xlarge': 16,
      'g2.2xlarge': 8,
      'unknown': 0,      
      'db.m1.small': 1, 'db.m1.medium': 1, 'db.m1.large': 2, 'db.m1.xlarge': 4,
      'db.m2.xlarge': 2, 'db.m2.2xlarge': 4, 'db.m2.4xlarge': 8, 'db.cr1.8xlarge': 16,
      'db.t1.micro': 0,
      'c3.large': 2, 'c3.xlarge': 4, 'c3.2xlarge': 8, 'c3.4xlarge': 16, 'c3.8xlarge': 32, 
    }
    disk_type_map = {
      'm1.small': 'ephemeral', 'm1.medium': 'ephemeral', 'm1.large': 'ephemeral', 'm1.xlarge': 'ephemeral',
      'm2.xlarge': 'ephemeral', 'm2.2xlarge': 'ephemeral', 'm2.4xlarge': 'ephemeral',
      'm3.xlarge': 'ephemeral', 'm3.2xlarge': 'ephemeral',
      'c1.medium': 'ephemeral', 'c1.xlarge': 'ephemeral',
      'hi1.4xlarge': 'ssd',
      'cg1.4xlarge': 'ephemeral',
      'cc1.4xlarge': 'ephemeral', 'cc2.8xlarge': 'ephemeral',
      't1.micro': 'ebs',
      'cr1.8xlarge': 'ssd',
      'hs1.8xlarge': 'ephemeral',
      'g2.2xlarge': 'ssd',
      'unknown': 'ephemeral',      
      'db.m1.small': 'ephemeral', 'db.m1.medium': 'ephemeral', 'db.m1.large': 'ephemeral', 'db.m1.xlarge': 'ephemeral',
      'db.m2.xlarge': 'ephemeral', 'db.m2.2xlarge': 'ephemeral', 'db.m2.4xlarge': 'ephemeral', 'db.cr1.8xlarge': 'ephemeral',
      'db.t1.micro': 'ebs',
      'c3.large': 'ssd', 'c3.xlarge': 'ssd', 'c3.2xlarge': 'ssd', 'c3.4xlarge': 'ssd', 'c3.8xlarge': 'ssd', 
    }
    misc_map = {
        'hi1.4xlarge': 'ssd 10Gb',
        'hs1.8xlarge': '10Gb',
        'cr1.8xlarge': 'ssd 10Gb',
        'g2.2xlarge': 'ssd',
        'cc2.8xlarge': '10Gb',
        'g2.2xlarge': 'gpu',
        'cg1.4xlarge': 'gpu 10Gb',
        'c3.large': 'ssd', 'c3.xlarge': 'ssd', 'c3.2xlarge': 'ssd', 'c3.4xlarge': 'ssd', 'c3.8xlarge': 'ssd',
    }

    node_types = {}

    r = requests.get('http://aws.amazon.com/ec2/pricing/json/linux-od.json')
    if not r:
        return {}

    data = r.json()
    instance_types = [r['instanceTypes'] for r in data['config']['regions']
                      if r['region'] == 'us-east'][0]
    for instance_type in instance_types:
        for size_block in instance_type['sizes']:
            node_type = {}
            size = size_block['size']
            value_columns = size_block['valueColumns']
            node_type['linux_cost'] = float([c['prices']['USD'] for c in value_columns
                                             if c['name'] == 'linux'][0])
            if size in memory_map:
                node_type['memory'] = memory_map[size] / 1000.0
            if size in disk_map:
                node_type['disk'] = disk_map[size]
            if size in platform_map:
                node_type['architecture'] = platform_map[size]
            if size in compute_units_map:
                node_type['compute_units'] = compute_units_map[size]
            if size in virtual_cores_map:
                node_type['cores'] = virtual_cores_map[size]
            if size in misc_map:
                node_type['misc'] = misc_map[size]

            node_types[size] = node_type

    return node_types
