import os
os.environ['HITC_NO_CACHE'] = 'true'

import uuid
from testconfig import config
from headintheclouds import digitalocean
from headintheclouds import ec2
from headintheclouds.ensemble import parse, dependency, create
from fabric.api import *
import fabric.api as fab
import yaml

def get_server():
    if config.get('ip'):
        return config.get('ip')

    nodes = digitalocean.create_servers(
        count=1, size='512MB', image=digitalocean.create_server_defaults['image'],
        names=['unit-test-server'], placement='New York 1'
    )

    return nodes[0]['ip']

def done_with_server(ip):
    if not config.get('ip'):
        with settings(ip):
            digitalocean.terminate()

def settings(ip, **other_settings):
    all_other_settings = digitalocean.settings
    all_other_settings.update(other_settings)
    return fab.settings(host_string=ip, host=ip, provider='digitalocean', **all_other_settings)



def create_instance(conf):
    servers = parse.parse_config(yaml.load(conf))
    name = servers.keys()[0]
    server = servers.values()[0]
    provider = {'ec2': ec2, 'digitalocean': digitalocean}[server.provider]
    existing_servers = create.find_existing_servers(servers.keys())
    dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)
    thing_index = create.create_things(servers, dependency_graph, changes['changing_servers'],
                                       changes['changing_containers'], changes['absent_containers'])
    thing = thing_index[('SERVER', name)]
    ip = thing.get_ip()
    return TestInstance(ip, provider, server.provider)

def check_changes(conf):
    servers = parse.parse_config(yaml.load(conf))
    existing_servers = create.find_existing_servers(servers.keys())
    _, changes = dependency.process_dependencies(servers, existing_servers)
    return changes

def make_changes(conf):
    servers = parse.parse_config(yaml.load(conf))
    existing_servers = create.find_existing_servers(servers.keys())
    dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)
    create.create_things(servers, dependency_graph, changes['changing_servers'],
                         changes['changing_containers'], changes['absent_containers'])
    
class TestInstance(object):

    def __init__(self, ip, provider, provider_name):
        self.ip = ip
        self.provider = provider
        self.provider_name = provider_name
        self.running = True

    def settings(self):
        return fab.settings(host_string=self.ip, host=self.ip, provider=self.provider_name, **self.provider.settings)

    def is_pingable(self):
        with self.settings():
            with fab.settings(hide('everything'), warn_only=True):
                return not local('ping -w1 -c1 %s' % self.ip).failed

    def has_open_port(self, port):
        with self.settings():
            with fab.settings(hide('everything'), warn_only=True):
                return not local('nc -w1 -z %s %d' % (self.ip, port)).failed

    def call_port(self, port, message):
        with self.settings():
            with fab.settings(hide('everything'), warn_only=True):
                ret = local('nc -w3 %s %d' % (self.ip, port), capture=True)
                if ret.failed:
                    return None
                return ret.strip()

    def terminate(self):
        with self.settings():
            self.provider.terminate()
        self.running = False

    def netcat_listen(self, port):
        with self.settings():
            sudobg('nc -l %d' % port)

def sudobg(cmd):
    sockname = 'dtach.%s' % uuid.uuid4()
    with fab.settings(hide('everything'), warn_only=True):
        if run('which dtach').failed:
            sudo('apt-get install -y dtach')
    
    return sudo('dtach -n `mktemp -u /tmp/%s.XXXX` %s'  % (sockname, cmd))

