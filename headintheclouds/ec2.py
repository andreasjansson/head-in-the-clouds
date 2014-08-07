import boto.ec2
import boto
import datetime
import dateutil.parser
import time
import sys
import re
import requests
import yaml

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
    'image': 'ubuntu 14.04',
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

def pricing(sort='cost'):
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
    for t, node_type in node_types.items():
        item = node_type.copy()
        item['size'] = t
        if t in data:
            prices = data[t]
            item['recent'] = round(latest_price[t], 3)
            item['median'] = round(util.median(prices), 3)
            item['stddev'] = round(util.stddev(prices), 3)
            item['max'] = round(max(prices), 3)
        table.append(item)

    util.print_table(table, ['size', 'memory', 'cores', 'storage', 'gpu', 'recent',
                             'median', 'stddev', 'max', ('cost', 'linux_cost')], 
                     sort=sort, default_sort='memory')

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
        time.sleep(5)
        
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

    active_requests = [r for r in requests if r.state == 'active']
    instance_ids = [r.instance_id for r in active_requests]
    for instance_id, name in zip(instance_ids, names):
        _set_instance_name(instance_id, name)

    return instance_ids

def validate_create_options(size, placement, bid, image, security_group, prefer_ebs=False):
    updates = {}

    # don't validate size for now, sizes get out of date really quickly
    # if size is not None and size not in get_node_types():
    #     raise Exception('Unknown EC2 instance size: "%s"' % size)

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
    node_types = {}

    r = requests.get('http://a0.awsstatic.com/pricing/1.0.19/ec2/linux-od.min.js')
    if not r:
        return {}

    data = parse_jsonp(r.content)

    instance_types = [r['instanceTypes'] for r in data['config']['regions']
                      if r['region'] == 'us-east'][0]
    for instance_type in instance_types:
        for size_block in instance_type['sizes']:
            node_type = {}
            size = size_block['size']
            value_columns = size_block['valueColumns']
            linux_columns = [c for c in value_columns if c['name'] in ('linux', 'os')]
            linux_column = linux_columns[0]
            node_type['linux_cost'] = float(linux_column['prices']['USD'])
            node_type['memory'] = float(size_block['memoryGiB'])
            node_type['storage'] = size_block['storageGB']
            node_type['cores'] = size_block['vCPU']
            node_type['compute_units'] = size_block['ECU']
            node_type['gpu'] = 'gpu' if instance_type['type'].startswith('gpu') else ''

            node_types[size] = node_type

    return node_types

def parse_jsonp(content):
    insides = content.split('callback(', 1)[1].rsplit(');')[0]
    yamlable = insides.replace(':', ': ')
    return yaml.load(yamlable)

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
    'ubuntu 12.04':      'ami-ad3660c4',
    'ubuntu 12.04 ebs':  'ami-a73264ce',
    'ubuntu 12.04 hvm':  'ami-b93264d0',
    'ubuntu 12.10':      'ami-a9cf9bc0',
    'ubuntu 12.10 ebs':  'ami-2bc99d42',
    'ubuntu 12.10 hvm':  'ami-2dc99d44',
    'ubuntu 13.04':      'ami-762d491f',
    'ubuntu 13.04 ebs':  'ami-10314d79',
    'ubuntu 13.04 hvm':  'ami-e1277b88',
    'ubuntu 13.10':      'ami-271a484e',
    'ubuntu 13.10 ebs':  'ami-ad184ac4',
    'ubuntu 13.10 hvm':  'ami-a1184ac8',
    'ubuntu 14.04':      'ami-1e917676',
    'ubuntu 14.04 ebs':  'ami-e4f7108c',
    'ubuntu 14.04 hvm':  'ami-b6f710de',
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
