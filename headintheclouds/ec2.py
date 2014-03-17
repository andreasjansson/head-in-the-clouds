# TODO: get rid of _get_image_id_for_size and just have a map of aliases

import boto.ec2
import boto
import datetime
import dateutil.parser
import time
import sys
import re
import requests

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab

import headintheclouds
from headintheclouds import util, cache

__all__ = ['spot_requests', 'cancel_spot_request']

@task
@runs_once
def spot_requests():
    '''
    List all active spot instance requests.
    '''
    requests = _ec2().get_all_spot_instance_requests()
    util.print_table(requests, ['id', ('bid', 'price'), 'create_time',
                                'state', 'status', 'instance_id'])

@task
@runs_once
def cancel_spot_request(request_id):
    '''
    Cancel a spot instance request.

    Args:
        request_id (str): Request ID
    '''
    _ec2().cancel_spot_instance_requests([request_id])

create_server_defaults = {
    'size': 'm1.small',
    'placement': 'us-east-1b',
    'bid': '',
    'image': 'ubuntu 12.04',
    'security_group': 'default',
}

def create_servers(count, names=None, size=None, placement=None,
                   bid=None, image=None, security_group=None, prefer_ebs=False):
    count = int(count)
    assert count == len(names)

    image = _unalias_image(image)
    
    if bid:
        instance_ids = create_spot_instances(
            count=count, size=size, placement=placement,
            image=image, names=names, bid=bid,
            security_group=security_group)
    else:
        instance_ids = create_on_demand_instances(
            count=count, size=size, placement=placement,
            image=image, names=names,
            security_group=security_group)

    wait_for_instances_to_become_accessible(instance_ids)
    nodes = [n for n in all_nodes() if n['id'] in instance_ids]
    return nodes

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

def terminate():
    instance_id = _host_node()['id']
    print 'Terminating EC2 instance %s' % instance_id
    _ec2().terminate_instances([instance_id])
    cache.uncache(all_nodes)

def reboot():
    instance_id = _host_node()['id']
    print 'Rebooting EC2 instance %s' % instance_id
    _ec2().reboot_instances([instance_id])

def nodes():
    nodes = cache.recache(all_nodes)
    util.print_table(nodes, ['name', 'size', 'ip', 'internal_ip', 'state', 'created'], sort='name')


def wait_for_instances_to_become_accessible(instance_ids):
    while True:
        nodes_ready = 0
        nodes = cache.recache(all_nodes)
        nodes = [n for n in nodes if n['id'] in instance_ids]
        for node in nodes:
            with fab.settings(hide('everything'), warn_only=True):
                result = local('nc -w 5 -zvv %s 22' % node['ip'])
            if result.return_code == 0:
                nodes_ready += 1
        if nodes_ready == len(instance_ids):
            return

        print 'Waiting for instance%s to become accessible' % (
            's' if len(instance_ids) > 1 else '')
        time.sleep(5)

def create_on_demand_instances(count, size, placement, image, names, security_group):
    if count > 1:
        print 'Creating %d EC2 %s instances' % (count, size)
    else:
        print 'Creating EC2 %s instance' % size

    reservation = _ec2().run_instances(
        image_id=image,
        min_count=count,
        max_count=count,
        security_groups=[security_group],
        instance_type=size,
        placement=placement,
        key_name=KEYPAIR_NAME,
    )

    for instance, name in zip(reservation.instances, names):
        _set_instance_name(instance.id, name)

    while any([i.state != 'running' for i in reservation.instances]):
        statuses = [i.state for i in reservation.instances]
        status_counts = [(status, statuses.count(status))
                         for status in sorted(set(statuses))]
        if count > 1:
            print 'Waiting for instances to start [%s]' % (
                ', '.join(['%s: %d' % s for s in status_counts]))
        else:
            print 'Waiting for instance to start [%s]' % (statuses[0])
        
        # TODO: handle error
        time.sleep(5)
        [i.update() for i in reservation.instances]

    return [i.id for i in reservation.instances]

def create_spot_instances(count, size, placement, image, names, bid, security_group):
    bid = float(bid)

    if count > 1:
        print 'Creating spot requests for %d %s instances at $%.3f' % (
            count, size, bid)
    else:
        print 'Creating spot request %s instance at $%.3f' % (size, bid)

    requests = _ec2().request_spot_instances(
        price=bid,
        image_id=image,
        count=count,
        security_groups=[security_group],
        instance_type=size,
        placement=placement,
        key_name=KEYPAIR_NAME,
    )

    request_ids = [r.id for r in requests]

    while True:
        requests = _ec2().get_all_spot_instance_requests(request_ids)

        statuses = [r.status.code for r in requests]
        status_counts = [(status, statuses.count(status))
                         for status in sorted(set(statuses))]

        if count > 1:
            print 'Waiting for spot requests to be fulfilled [%s]' % (
                ', '.join(['%s: %d' % s for s in status_counts]))
        else:
            print 'Waiting for spot request to be fulfilled [%s]' % (
                statuses[0])

        if all([status == 'fulfilled' for status in statuses]):
            break

        if all([status == 'price-too-low']):
            abort('Price too low')

        time.sleep(5)
        
    active_requests = [r for r in requests if r.state == 'active']
    instance_ids = [r.instance_id for r in active_requests]
    for instance_id, name in zip(instance_ids, names):
        _set_instance_name(instance_id, name)

    return instance_ids

def validate_create_options(size, placement, bid, image, security_group, prefer_ebs=False):
    updates = {}

    if size is not None and size not in get_node_types():
        raise Exception('Unknown EC2 instance size: "%s"' % size)

    if size is None:
        raise Exception('You need to specify a size')

    if image is None:
        raise Exception('You need to specify an image')

    updates['image'] = _unalias_image(image)

    return updates

def rename(name):
    current_node = _host_node()
    _set_instance_name(current_node['id'], name)

@cache.cached
def get_node_types():

    memory_map = {
        'm1.small': 1700,
        'm1.medium': 3750,
        'm1.large': 7500,
        'm1.xlarge': 15000,
        'm2.xlarge': 17100,
        'm2.2xlarge': 34200,
        'm2.4xlarge': 68400,
        'm3.xlarge': 15000,
        'm3.2xlarge': 30000,
        'c1.medium': 1700,
        'c1.xlarge': 7000,
        'hi1.4xlarge': 60500,
        'cg1.4xlarge': 22000,
        'cc1.4xlarge': 23000,
        'cc2.8xlarge': 60500,
        't1.micro': 1700,
        'cr1.8xlarge': 244000,
        'hs1.8xlarge': 117000,
        'g2.2xlarge': 15000,      
        'db.m1.small': 1700,
        'db.m1.medium': 3750,
        'db.m1.large': 7500,
        'db.m1.xlarge': 15000,
        'db.m2.xlarge': 17100,
        'db.m2.2xlarge': 34000,
        'db.m2.4xlarge': 68000,
        'db.cr1.8xlarge': 244000,
        'db.t1.micro': 613,
        'c3.large': 3750,
        'c3.xlarge': 7000,
        'c3.2xlarge': 15000,
        'c3.4xlarge': 30000,
        'c3.8xlarge': 60000, 
    }
    disk_map = {
        'm1.small': 160,
        'm1.medium': 410,
        'm1.large':850,
        'm1.xlarge': 1690,
        'm2.xlarge': 420,
        'm2.2xlarge': 850,
        'm2.4xlarge': 1690,
        'm3.xlarge': 0,
        'm3.2xlarge': 0,
        'c1.medium': 350,
        'c1.xlarge': 1690,
        'hi1.4xlarge': 2048,
        'cg1.4xlarge': 1690,
        'cc1.4xlarge': 1690,
        'cc2.8xlarge': 3370,
        't1.micro': 160,
        'cr1.8xlarge': 240,
        'hs1.8xlarge': 48000,
        'g2.2xlarge': 60,      
        'db.m1.small': 160,
        'db.m1.medium': 410,
        'db.m1.large':850,
        'db.m1.xlarge': 1690,
        'db.m2.xlarge': 420,
        'db.m2.2xlarge': 850,
        'db.m2.4xlarge': 1690,
        'db.cr1.8xlarge': 1690,
        'db.t1.micro': 160,
        'c3.large': 32,
        'c3.xlarge': 80,
        'c3.2xlarge': 160,
        'c3.4xlarge': 320,
        'c3.8xlarge': 640, 
    }
    platform_map = {
        'm1.small': 32,
        'm1.medium': 32,
        'm1.large': 64,
        'm1.xlarge': 64,
        'm2.xlarge': 64,
        'm2.2xlarge': 64,
        'm2.4xlarge': 64,
        'm3.xlarge': 64,
        'm3.2xlarge': 64,
        'c1.medium': 32,
        'c1.xlarge': 64,
        'hi1.4xlarge': 64,
        'cg1.4xlarge': 64,
        'cc1.4xlarge': 64,
        'cc2.8xlarge': 64,
        't1.micro': 32,
        'cr1.8xlarge': 64,
        'hs1.8xlarge': 64,
        'g2.2xlarge': 64,      
        'db.m1.small': 64,
        'db.m1.medium': 64,
        'db.m1.large': 64,
        'db.m1.xlarge': 64,
        'db.m2.xlarge': 64,
        'db.m2.2xlarge': 64,
        'db.m2.4xlarge': 64,
        'db.cr1.8xlarge': 64,
        'db.t1.micro': 64,
        'c3.large': 64,
        'c3.xlarge': 64,
        'c3.2xlarge': 64,
        'c3.4xlarge': 64,
        'c3.8xlarge': 64, 
    }
    compute_units_map = {
        'm1.small': 1,
        'm1.medium': 2,
        'm1.large': 4,
        'm1.xlarge': 8,
        'm2.xlarge': 6,
        'm2.2xlarge': 13,
        'm2.4xlarge': 26,
        'm3.xlarge': 13,
        'm3.2xlarge': 26,
        'c1.medium': 5,
        'c1.xlarge': 20,
        'hi1.4xlarge': 35,
        'cg1.4xlarge': 34,
        'cc1.4xlarge': 34,
        'cc2.8xlarge': 88,
        't1.micro': 2,
        'cr1.8xlarge': 88,
        'hs1.8xlarge': 35,
        'g2.2xlarge': 26,
        'unknown': 0,      
        'db.m1.small': 1,
        'db.m1.medium': 2,
        'db.m1.large': 4,
        'db.m1.xlarge': 8,
        'db.m2.xlarge': 6.5,
        'db.m2.2xlarge': 13,
        'db.m2.4xlarge': 26,
        'db.cr1.8xlarge': 88,
        'db.t1.micro': 1,
        'c3.large': 7,
        'c3.xlarge': 14,
        'c3.2xlarge': 28,
        'c3.4xlarge': 55,
        'c3.8xlarge': 108, 
    }
    virtual_cores_map = {
        'm1.small': 1,
        'm1.medium': 1,
        'm1.large': 2,
        'm1.xlarge': 4,
        'm2.xlarge': 2,
        'm2.2xlarge': 4,
        'm2.4xlarge': 8,
        'm3.xlarge': 4,
        'm3.2xlarge': 8,
        'c1.medium': 2,
        'c1.xlarge': 8,
        'hi1.4xlarge': 16,
        'cg1.4xlarge': 8,
        'cc1.4xlarge': 8,
        'cc2.8xlarge': 16,
        't1.micro': 0,
        'cr1.8xlarge': 16,
        'hs1.8xlarge': 16,
        'g2.2xlarge': 8,
        'unknown': 0,      
        'db.m1.small': 1,
        'db.m1.medium': 1,
        'db.m1.large': 2,
        'db.m1.xlarge': 4,
        'db.m2.xlarge': 2,
        'db.m2.2xlarge': 4,
        'db.m2.4xlarge': 8,
        'db.cr1.8xlarge': 16,
        'db.t1.micro': 0,
        'c3.large': 2,
        'c3.xlarge': 4,
        'c3.2xlarge': 8,
        'c3.4xlarge': 16,
        'c3.8xlarge': 32, 
    }
    disk_size_map = {
        'm1.small': 'ephemeral',
        'm1.medium': 'ephemeral',
        'm1.large': 'ephemeral',
        'm1.xlarge': 'ephemeral',
        'm2.xlarge': 'ephemeral',
        'm2.2xlarge': 'ephemeral',
        'm2.4xlarge': 'ephemeral',
        'm3.xlarge': 'ephemeral',
        'm3.2xlarge': 'ephemeral',
        'c1.medium': 'ephemeral',
        'c1.xlarge': 'ephemeral',
        'hi1.4xlarge': 'ssd',
        'cg1.4xlarge': 'ephemeral',
        'cc1.4xlarge': 'ephemeral',
        'cc2.8xlarge': 'ephemeral',
        't1.micro': 'ebs',
        'cr1.8xlarge': 'ssd',
        'hs1.8xlarge': 'ephemeral',
        'g2.2xlarge': 'ssd',
        'unknown': 'ephemeral',      
        'db.m1.small': 'ephemeral',
        'db.m1.medium': 'ephemeral',
        'db.m1.large': 'ephemeral',
        'db.m1.xlarge': 'ephemeral',
        'db.m2.xlarge': 'ephemeral',
        'db.m2.2xlarge': 'ephemeral',
        'db.m2.4xlarge': 'ephemeral',
        'db.cr1.8xlarge': 'ephemeral',
        'db.t1.micro': 'ebs',
        'c3.large': 'ssd',
        'c3.xlarge': 'ssd',
        'c3.2xlarge': 'ssd',
        'c3.4xlarge': 'ssd',
        'c3.8xlarge': 'ssd', 
    }
    misc_map = {
        'hi1.4xlarge': 'ssd 10Gb',
        'hs1.8xlarge': '10Gb',
        'cr1.8xlarge': 'ssd 10Gb',
        'g2.2xlarge': 'gpu ssd',
        'cc2.8xlarge': '10Gb',
        'cg1.4xlarge': 'gpu 10Gb',
        'c3.large': 'ssd',
        'c3.xlarge': 'ssd',
        'c3.2xlarge': 'ssd',
        'c3.4xlarge': 'ssd',
        'c3.8xlarge': 'ssd',
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
            node_size = {}
            size = size_block['size']
            value_columns = size_block['valueColumns']
            node_size['linux_cost'] = float([c['prices']['USD'] for c in value_columns
                                             if c['name'] == 'linux'][0])
            if size in memory_map:
                node_size['memory'] = memory_map[size] / 1000.0
            if size in disk_map:
                node_size['disk'] = disk_map[size]
            if size in platform_map:
                node_size['architecture'] = platform_map[size]
            if size in compute_units_map:
                node_size['compute_units'] = compute_units_map[size]
            if size in virtual_cores_map:
                node_size['cores'] = virtual_cores_map[size]
            if size in misc_map:
                node_size['misc'] = misc_map[size]

            node_types[size] = node_size

    return node_types

@cache.cached
def all_nodes():
    reservations = _ec2().get_all_instances()
    nodes = [instance_to_node(x)
             for r in reservations for x in r.instances
             if 'Name' in x.tags
             and x.tags['Name'].startswith(env.name_prefix)
             and x.state not in ('terminated', 'shutting-down')]
    return nodes

def instance_to_node(instance):
    node = {}
    node['id'] = instance.id
    node['name'] = re.sub('^%s' % env.name_prefix, '', instance.tags['Name'])
    node['size'] = instance.instance_type
    node['security_group'] = instance.groups[0].name
    node['placement'] = instance.placement
    node['image'] = instance.image_id
    node['state'] = instance.state
    node['running'] = instance.state == 'running'
    created = dateutil.parser.parse(instance.launch_time)
    node['created'] = created.astimezone(dateutil.tz.tzlocal())
    node['ip'] = instance.ip_address
    node['internal_address'] = instance.private_dns_name
    node['internal_ip'] = instance.private_ip_address
    return node

def equivalent_create_options(options1, options2):
    options1 = options1.copy()
    options2 = options2.copy()

    options1['image'] = _unalias_image(options1['image'])
    options2['image'] = _unalias_image(options2['image'])

    return (options1['size'] == options2['size']
            and options1['placement'] == options2['placement']
            and options1['security_group'] == options2['security_group']
            and options1['image'] == options2['image'])

def _ec2():
    if not hasattr(_ec2, 'client'):
        _ec2.client = boto.ec2.connection.EC2Connection(ACCESS_KEY_ID, SECRET_ACCESS_KEY)
    return _ec2.client

def _host_node():
    return [x for x in all_nodes() if x['ip'] == env.host][0]

def _host_role():
    return _host_node()['role']

def _set_instance_name(instance_id, name):
    _ec2().create_tags(instance_id, {'Name': '%s%s' % (env.name_prefix, name)})

def _unalias_image(image):
    return IMAGE_ALIASES.get(image.lower(), image)

IMAGE_ALIASES = {
    ('ubuntu 12.04'):      'ami-ad3660c4',
    ('ubuntu 12.04 ebs'):  'ami-a73264ce',
    ('ubuntu 12.04 hvm'):  'ami-b93264d0',
    ('ubuntu 12.10'):      'ami-a9cf9bc0',
    ('ubuntu 12.10 ebs'):  'ami-2bc99d42',
    ('ubuntu 12.10 hvm'):  'ami-2dc99d44',
    ('ubuntu 13.04'):      'ami-762d491f',
    ('ubuntu 13.04 ebs'):  'ami-10314d79',
    ('ubuntu 13.04 hvm'):  'ami-e1277b88',
    ('ubuntu 13.10'):      'ami-271a484e',
    ('ubuntu 13.10 ebs'):  'ami-ad184ac4',
    ('ubuntu 13.10 hvm'):  'ami-a1184ac8',
}

ACCESS_KEY_ID = util.env_var('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = util.env_var('AWS_SECRET_ACCESS_KEY')
SSH_KEY_FILENAME = util.env_var('AWS_SSH_KEY_FILENAME')
KEYPAIR_NAME = util.env_var('AWS_KEYPAIR_NAME')

settings = {
    'user': 'ubuntu',
    'key_filename': SSH_KEY_FILENAME,
}

headintheclouds.add_provider('ec2', sys.modules[__name__])
