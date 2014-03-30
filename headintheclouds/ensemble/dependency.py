import re
import collections
import simplejson as json
import multiprocessing
import time
import random
import fabric

from headintheclouds.ensemble.dependencygraph import DependencyGraph
from headintheclouds.ensemble.exceptions import ConfigException
from headintheclouds.ensemble.container import Container
from headintheclouds.ensemble.server import Server
from headintheclouds.ensemble.firewall import Firewall
from headintheclouds.ensemble import thingindex

MULTI_THREADED = True

class FieldPointer(object):

    def __init__(self, dependent_field_index,
                 dependent_substring, depends_field_index):
        self.dependent_field_index = dependent_field_index
        self.dependent_substring = dependent_substring
        self.depends_field_index = depends_field_index

    def resolve(self, dependent, depends):
        depends_value = depends.fields[self.depends_field_index]
        if depends_value is None:
            return False

        resolved_value = dependent.fields[self.dependent_field_index].replace(
            self.dependent_substring, str(depends_value))
        dependent.fields[self.dependent_field_index] = resolved_value

        return True

class ActivePointer(object):

    def resolve(self, dependent, depends):
        return depends.is_active()

def process_dependencies(servers, existing_servers):
    new_index = thingindex.build_thing_index(servers)
    existing_index = thingindex.build_thing_index(existing_servers)

    dependency_graph = get_raw_dependency_graph(servers)
    cycle_node = dependency_graph.find_cycle()
    if cycle_node:
        raise ConfigException('Cycle detected')

    changing_things = set()
    new_things = set()

    queue = multiprocessing.Queue()

    remaining = set(new_index)
    while remaining:
        free_nodes = dependency_graph.get_free_nodes(remaining)
        if free_nodes:
            next_things = free_nodes
        else:
            next_things = remaining

        remaining = remaining - next_things

        for thing_name in next_things:
            process = DependencyProcess(
                new_index[thing_name], existing_index.get(thing_name), queue)
            process.start()
            # race conditions fml
            time.sleep(random.random() * .5)

        for _ in next_things:
            new_thing, existing_thing, is_changing, is_new, exception = queue.get()
            if exception:
                raise exception

            if not is_new and not is_changing:
                new_index[new_thing.thing_name()] = new_thing
                thingindex.refresh_thing_index(new_index)
                existing_index[existing_thing.thing_name()] = existing_thing
                thingindex.refresh_thing_index(existing_index)

            new_thing = new_index[new_thing.thing_name()]

            if is_changing:
                changing_things.add(new_thing)
            elif is_new:
                new_things.add(new_thing)

            resolve_dependents(dependency_graph, new_thing, new_index)

    changes = collections.defaultdict(set)

    for t in changing_things:
        if isinstance(t, Container):
            changes['changing_containers'].add(t)
        elif isinstance(t, Server):
            changes['changing_servers'].add(t)
        elif isinstance(t, Firewall):
            changes['changing_firewalls'].add(t)

    for t in new_things:
        if isinstance(t, Container):
            changes['new_containers'].add(t)
        elif isinstance(t, Server):
            changes['new_servers'].add(t)
        elif isinstance(t, Firewall):
            changes['new_firewalls'].add(t)

    for server in existing_servers.values():
        for container in server.containers.values():
            if container.thing_name() not in new_index:
                changes['absent_containers'].add(container)

    thingindex.refresh_servers(servers, new_index)

    return dependency_graph, changes

def get_raw_dependency_graph(servers):
    dependency_graph = DependencyGraph()

    for server in servers.values():
        for field_index, value in server.fields.indexed_items():
            new_value = resolve_or_add_dependency(server, field_index, value, servers, dependency_graph)
            if new_value:
                server.fields[field_index] = new_value

        for container in server.containers.values():
            for field_index, value in container.fields.indexed_items():
                new_value = resolve_or_add_dependency(container, field_index, value, servers, dependency_graph)
                if new_value:
                    container.fields[field_index] = new_value

            # dependency so that containers need to wait for the server to start
            dependency_graph.add(container.thing_name(), ActivePointer(), server.thing_name())

        if server.firewall:
            for field_index, value in server.firewall.fields.indexed_items():
                new_value = resolve_or_add_dependency(server.firewall, field_index, value, servers, dependency_graph)
                if new_value:
                    server.firewall.fields[field_index] = new_value

            # dependency so that firewall need to wait for the server to start
            dependency_graph.add(server.firewall.thing_name(), ActivePointer(), server.thing_name())

    return dependency_graph

def resolve_or_add_dependency(dependent, dependent_field_index, value, servers, dependency_graph):
    if value == '$servers':
        value = get_servers_parameterised_json(servers)

    if value == '$internal_ips':
        value = get_parameterised_internal_ips(servers)

    variables = parse_variables(value)
    for var_string, var in variables.items():
        parts = split_variable(var)
        depends, depends_field_index = get_variable_depends(dependent, servers, parts)
        pointer = FieldPointer(dependent_field_index, var_string, depends_field_index)

        dependency_graph.add(dependent.thing_name(), pointer, depends.thing_name())

    return value

def get_variable_depends(dependent, servers, parts):
    if parts[0] == 'host':
        server = dependent.host
    else:
        server_name = parts[0]
        if server_name not in servers:
            raise ConfigException('Unknown server: %s' % parts[0])

        server = servers[server_name]
    if parts[1] == 'containers':
        container_name = parts[2]
        if container_name not in server.containers:
            raise ConfigException('Unknown container: %s' % parts[2])

        depends = server.containers[container_name]
        field, index = parse_index(parts[3])
        # TODO: validate fields, somehow
    elif parts[1] == 'firewall':
        field, index = parse_index(parts[2])
        depends = server.firewall
    else:
        depends = server
        field, index = parse_index(parts[1])
        # TODO: validate

    return depends, (field, index)

def parse_index(part):
    split = part.split('[', 1)
    if len(split) == 1:
        return split[0], None

    name, index_string = split
    if not index_string.endswith(']'):
        raise ConfigException('Syntax error in %s' % part)

    return name, index_string.split('][')

class DependencyProcess(multiprocessing.Process):

    def __init__(self, new_thing, existing_thing, queue):
        super(DependencyProcess, self).__init__()
        self.new_thing = new_thing
        self.existing_thing = existing_thing
        self.queue = queue

        if not MULTI_THREADED:
            self.start = self.run

    def run(self):
        if MULTI_THREADED:
            fabric.network.disconnect_all()

        is_changing = is_new = False
        exception = None
        try:
            if self.existing_thing:
                if self.existing_thing.is_equivalent(self.new_thing):
                    self.new_thing.update(self.existing_thing)
                else:
                    is_changing = True
            else:
                is_new = True
        except Exception, e:
            exception = e

        self.queue.put((self.new_thing, self.existing_thing, is_changing, is_new, exception))

def resolve_existing(thing_name, new_index, existing_index, dependency_graph):
    changing_things = set()
    new_things = set()

    new = new_index[thing_name]
    if thing_name in existing_index:
        existing = existing_index[thing_name]
        if existing.is_equivalent(new):
            new.update(existing)
        else:
            changing_things.add(new)
    else:
        new_things.add(new)

    return changing_things, new_things

def resolve_dependents(dependency_graph, depends, thing_index):
    dependents = dependency_graph.get_dependents(depends.thing_name())

    for dependent_name, pointers in dependents.items():
        dependent = thing_index[dependent_name]
        for pointer in pointers:
            resolved = pointer.resolve(dependent, depends)
            if resolved:
                dependency_graph.remove(dependent_name, pointer, depends.thing_name())

def parse_variables(value):
    variables = {} # dict {var_string: variable}
    while '$' in str(value):
        start = value.index('$')
        if start + 1 >= len(value):
            raise ConfigException('Syntax error in variable')
        elif value[start + 1] == '{':
            if '}' not in value:
                raise ConfigException('Syntax error in variable')
            end = value[start + 1:].index('}') + start + 2
            var = value[start + 2:end - 1]
        else:
            match = re.search('[^a-zA-Z0-9_]', value[start + 1:])
            if match:
                end = match.span()[0] + start + 1
            else:
                end = len(value)
            var = value[start + 1:end]

        if var == '':
            raise ConfigException('Syntax error in variable')

        var_string = value[start:end]
        variables[var_string] = var

        value = value[end:]

    return variables

def split_variable(var):
    # TODO: hack (for ips (v4 only..))
    ip_match = re.match(r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}', var)
    if ip_match:
        parts = [ip_match.group()]
        parts.extend(var[ip_match.end() + 1:].split('.'))
    else:
        parts = var.split('.')
    return parts

def get_servers_parameterised_json(servers):
    server_dicts = {}
    for server_name, server in servers.items():
        server_dicts[server_name] = {}
        interesting_fields = {'ip', 'internal_ip', 'name'}
        for key in interesting_fields:
            server_dicts[server_name][key] = '${%s.%s}' % (server_name, key)
    return json.dumps(server_dicts)

def get_parameterised_internal_ips(servers):
    internal_ips = []
    for server_name in servers:
        internal_ips.append('${%s.internal_ip}' % server_name)
    return ','.join(internal_ips)
