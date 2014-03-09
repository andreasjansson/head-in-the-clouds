import re
import collections
import simplejson as json

from headintheclouds.ensemble.dependencygraph import DependencyGraph
from headintheclouds.ensemble.exceptions import ConfigException
from headintheclouds.ensemble.container import Container
from headintheclouds.ensemble import thingindex

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
            self.dependent_substring, depends_value)
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

    all_changing_things = set()
    all_new_things = set()

    remaining = set(new_index)
    while True:
        free_nodes = dependency_graph.get_free_nodes(remaining)
        if not free_nodes:
            break

        remaining -= free_nodes

        for thing_name in free_nodes:
            changing_things, new_things = resolve_existing(thing_name, new_index, existing_index, dependency_graph)
            all_changing_things |= changing_things
            all_new_things |= new_things

    for thing_name in remaining:
        changing_things, new_things = resolve_existing(thing_name, new_index, existing_index, dependency_graph)
        all_changing_things |= changing_things
        all_new_things |= new_things

    changes = collections.defaultdict(set)

    for t in all_changing_things:
        if isinstance(t, Container):
            changes['changing_containers'].add(t)
        else:
            changes['changing_servers'].add(t)

    for t in all_new_things:
        if isinstance(t, Container):
            changes['new_containers'].add(t)
        else:
            changes['new_servers'].add(t)

    for server in existing_servers.values():
        for container in server.containers.values():
            if container.thing_name() not in new_index:
                changes['absent_containers'].add(container)

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

#        for rule in server.firewall_rules:
#            dependency_graph.add(rule.thing_name

    return dependency_graph

def resolve_or_add_dependency(dependent, dependent_field_index, value, servers, dependency_graph):
    if value == '$servers':
        value = get_servers_parameterised_json(servers)

    variables = parse_variables(value)
    for var_string, var in variables.items():
        parts = split_variable(var)
        depends, depends_field_index = get_variable_depends(dependent, servers, parts)
        pointer = FieldPointer(dependent_field_index, var_string, depends_field_index)

        dependency_graph.add(dependent.thing_name(), pointer, depends.thing_name())

    return value

def get_variable_depends(dependent, servers, parts):

    # TODO: still default to first!
    
    if parts[0] == 'host':
        server = dependent.host
    else:
        # if we can't find it we assume it's because we're missing the -index,
        # and default to first. this is pretty naive.
        server_name = parts[0]
        if server_name not in servers:
            server_name += '-0'
        server = servers[server_name]
    if parts[1] == 'containers':
        container_name = parts[2]

        # same here, default to first
        if container_name not in server.containers:
            container_name += '-0'

        depends = server.containers[container_name]
        field, index = parse_index(parts[3])
        # TODO: validate
    elif parts[1] == 'firewall':
        field, index = parse_index(parts[2])
        depends = server.firewall_rules[index[0]]
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

    resolve_dependents(dependency_graph, new, new_index)

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
        for key in server.fields:
            server_dicts[server_name][key] = '${%s.%s}' % (server_name, key)
    return json.dumps(server_dicts)
