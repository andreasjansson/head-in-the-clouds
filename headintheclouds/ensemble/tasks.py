import sys
import os
import yaml

from fabric.api import * # pylint: disable=W0614,W0401

from headintheclouds.tasks import uncache
from headintheclouds.ensemble import parse
from headintheclouds.ensemble import dependency
from headintheclouds.ensemble import create
from headintheclouds.ensemble import exceptions

@runs_once
@task
def up(name):
    '''
    Create servers and containers as required to meet the configuration
    specified in _name_.

    Args:
        * name: The name of the yaml config file (you can omit the .yml extension for convenience)

    Example:
        fab ensemble.up:wordpress
    '''
    filenames_to_try = [
        name,
        '%s.yml' % name,
        '%s.yaml' % name,
    ]
    for filename in filenames_to_try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                config = yaml.load(f)
            break
    else:
        abort('Ensemble manifest not found: %s' % name)

    uncache()
    try:
        do_up(config)
    except exceptions.ConfigException, e:
        abort('Config error: ' + str(e))

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

