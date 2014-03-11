import sys
import os
import yaml

from fabric.api import * # pylint: disable=W0614,W0401

from headintheclouds.ensemble import parse
from headintheclouds.ensemble import dependency
from headintheclouds.ensemble import create
from headintheclouds.ensemble import exceptions

@runs_once
@task
def up(name, filename=None):
    if filename is None:
        filename = '%s.yml' % name
    if not os.path.exists(filename):
        abort('No such file: %s' % filename)
    with open(filename, 'r') as f:
        config = yaml.load(f)

    do_up(config)

def do_up(config):
    servers = parse.parse_config(config)

    sys.stdout.write('Calculating changes...')
    sys.stdout.flush()

    existing_servers = create.find_existing_servers(servers.keys())
    dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

    cycle_node = dependency_graph.find_cycle()
    if cycle_node:
        raise exceptions.ConfigException('Cycle detected')

    print ''

    create.confirm_changes(changes)
    create.create_things(servers, dependency_graph, changes['changing_servers'],
                         changes['changing_containers'], changes['absent_containers'])

