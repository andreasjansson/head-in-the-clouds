import os
import yaml
import re
import collections
import multiprocessing
from fabric.api import * # pylint: disable=W0614,W0401

from headintheclouds import ec2, do, docker

@task
def up(name, filename=None):
    if filename is None:
        filename = '%s.yml' % name
    if not os.path.exists(filename):
        abort('No such file: %s' % filename)
    with open(filename, 'r') as f:
        config = yaml.load(f)

    try:
        servers, dependency_graph = parse_config(config)
    except ConfigException, e:
        if e.container_name:
            abort('Configuration error in server "%s", container "%s": %s' % (
                e.server_name, e.container_name, str(e)))
        else:
            abort('Configuration error in server "%s": %s' % (e.server_name, str(e)))

    existing_servers, existing_containers = find_existing_things()
    (servers_to_delete, servers_to_start,
     containers_to_kill, containers_to_start) = get_changes(
         servers, existing_servers, existing_containers)
    confirm_changes(servers_to_delete, servers_to_start,
                    containers_to_kill, containers_to_start)

    resolve_existing(dependency_graph, servers, existing_servers)
    create_things(servers, dependency_graph)

def find_existing_things():
    return [], []

def get_changes(servers, existing_servers, existing_containers):
    return [], [], servers, []

def confirm_changes(servers_to_delete, servers_to_start,
                    containers_to_kill, containers_to_start):
    pass

def resolve_existing(servers, dependency_graph, existing_servers):
    def resolve_thing(existing_thing):
        dependents = dependency_graph.get_dependents(existing_thing.get_thing_name())
        for (server_name, container_name), attr_is in dependents.items():
            if container_name:
                dependent = servers[server_name].containers[container_name]
            else:
                dependent = servers[server_name]
            for attr, i in attr_is:
                dependent.resolve(existing_thing, attr, i)

    for existing_server in existing_servers.values():
        resolve_thing(existing_server)
        for existing_container in existing_server.containers.values():
            resolve_thing(existing_container)
            
def create_things(servers, dependency_graph):
    # TODO: handle errors

    queue = multiprocessing.Queue()
    processes = make_processes(servers, dependency_graph, queue)
    runnable_processes = [p for p in processes.values() if p.is_resolved()]
    for process in runnable_processes:
        process.start()

    n_completed = 0
    while n_completed < len(processes):
        completed_things = queue.get()
        n_completed += 1
        for thing in completed_things:
            thing.refresh()
            dependents = dependency_graph.get_dependents(thing.get_thing_name())
            for (server_name, container_name), attr_is in dependents.items():
                if container_name:
                    dependent = servers[server_name].containers[container_name]
                else:
                    dependent = servers[server_name]
                process = processes[dependent]
                process.to_resolve -= 1
                for attr_i in attr_is:
                    if attr_i:
                        attr, i = attr_i
                        dependent.resolve(thing, attr, i) 
                if process.is_resolved():
                    process.start()

def make_processes(servers, dependency_graph, queue):
    processes = {}
    resolved_servers = collections.defaultdict(list)

    for server in servers.values():
        depends = dependency_graph.get_depends(server.get_thing_name())
        if len(depends) == 0:
            resolved_servers[(server.provider, server.type, server.bid)].append(server)
        else:
            processes[server] = UpProcess(server, len(depends), queue)
        for container in server.containers.values():
            depends = dependency_graph.get_depends(container.get_thing_name())
            processes[container] = UpProcess(container, len(depends), queue)

    for servers in resolved_servers.values():
        server_group = ServerCreateGroup(servers)
        processes[server_group] = UpProcess(server_group, 0, queue)

    return processes

class UpProcess(multiprocessing.Process):

    def __init__(self, thing, to_resolve, queue):
        super(UpProcess, self).__init__()
        # works because we don't need to mutate thing anymore once we've forked
        self.thing = thing
        self.to_resolve = to_resolve
        self.queue = queue

    def run(self):
        self.thing.create()
        if isinstance(self.thing, ServerCreateGroup):
            self.queue.put(self.thing.servers)
        else:
            self.queue.put([self.thing])

    def is_resolved(self):
        return self.to_resolve == 0

def parse_config(config):
    if '$templates' in config:
        templates = config['$templates']
        del config['$templates']
    else:
        templates = {}

    all_servers = {}

    for server_name, spec in config.items():
        servers = parse_server(server_name, spec, templates)

        if 'containers' in spec:
            for server in servers.values():
                server.containers = parse_containers(
                    spec['containers'], server, templates, server_name)

        all_servers.update(servers)

    dependency_graph = get_dependencies(servers)
    cycle_node = dependency_graph.find_cycle()
    if cycle_node:
        server, container = cycle_node
        raise ConfigException('Cycle detected', server.name, container.name)

    return all_servers, dependency_graph

def parse_server(server_name, spec, templates):
    expand_template(spec, templates, server_name)
    servers = {}
    
    valid_fields = set(['$count', 'containers'])

    count = spec.get('$count', 1)
    for i in range(count):
        if count > 1:
            server = Server('%s-%d' % (server_name, i))
        else:
            server = Server(server_name)

        if 'provider' in spec:
            valid_fields |= set(['provider', 'type', 'bid'])

            server.provider = spec['provider']
            if 'type' not in spec:
                raise ConfigException('"type" missing', server_name)
            server.type = spec['type']
            if 'bid' in spec:
                server.bid = spec['bid']
                valid_fields.add('bid')

        elif 'ip' in spec:
            if count != 1:
                raise ConfigException('IP-based servers must be singletons', server_name)

            server.ip = spec['ip']
            valid_fields.add('ip')
        else:
            raise ConfigException('Missing "provider" or "ip"', server_name)

        if set(spec) - valid_fields:
            raise ConfigException('Invalid fields' % (
                ', '.join(set(spec) - valid_fields)), server_name)

        servers[server.name] = server

    return servers

def parse_containers(specs, server, templates, server_name):
    containers = {}

    for container_name, spec in specs.items():
        expand_template(spec, templates, server_name, container_name)

        valid_fields = set(['$count', 'image', 'environment', 'command',
                            'ports', 'volumes'])
        if set(spec) - valid_fields:
            raise ConfigException(
                'Invalid fields: %s' %
                ', '.join(set(spec) - valid_fields), server_name, container_name)

        count = spec.get('$count', 1)
        for i in range(count):
            if count > 1:
                container = Container('%s-%d' % (container_name, i), server)
            else:
                container = Container(container_name, server)

            if 'image' not in spec:
                raise ConfigException('"image" missing', server_name, container_name)
            container.image = spec['image']
            container.command = spec.get('command', None)
            container.volumes = spec.get('volumes', [])
            if 'environment' in spec:
                container.environment = [list(x) for x in spec.environment.items()]

            for port_spec in spec.get('ports', []):
                split = str(port_spec).split(':', 2)
                if len(split) == 1:
                    fr = to = split[0]
                else:
                    fr, to = split
                container.ports.append((fr, to))

            containers[container.name] = container

    return containers

def expand_template(spec, templates, server, container=None):
    if '$template' in spec:
        template = spec['$template']
        del spec['$template']

        if template not in templates:
            raise ConfigException('Missing template: %s' % template, server, container)

        for k, v in templates[template].items():
            if k not in spec:
                spec[k] = v

def get_dependencies(servers):
    dependency_graph = DependencyGraph()

    for server in servers.values():
        for value, attr in all_field_attrs(server):
            add_variable_dependency(value, attr, servers, dependency_graph, server)

        for container in server.containers.values():
            for value, attr in all_field_attrs(container):
                add_variable_dependency(value, attr, dependency_graph, server, container)

            # need its own server to start first
            dependency_graph.add(container.get_thing_name(), None, server.get_thing_name())

    return dependency_graph

def add_variable_dependency(value, attr, dependency_graph, server, container=None):
    # TODO: validate that it's actually possible to get the value
    # (e.g. host.asdf, host.container.blah.bid neither make sense)

    variables, _ = parse_variables(value)
    for i, var in enumerate(variables):
        parts = var.split('.')
        if parts[0] == 'host':
            depends_server = server.name
        else:
            depends_server = parts[0]
        if parts[1] == 'containers':
            if not container:
                raise ConfigException(
                    'Server to container dependencies are currently unsupported', server.name)
            depends_container = parts[2]
        else:
            depends_container = None

        dependency_graph.add((server.name, container.name if container else None), (attr, i), (depends_server, depends_container))

def parse_variables(value):
    variables = []
    var_strings = []
    while '$' in str(value):
        start = value.index('$')
        if start + 1 >= len(value):
            raise ConfigException('Syntax error in variable')
        elif value[start + 1] == '{':
            if '}' not in value:
                raise ConfigException('Syntax error in variable')
            end = value.index('}') + 1
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

        variables.append(var)
        var_strings.append(value[start:end])
        value = value[end:]
    return variables, var_strings

def resolve(value, thing, i):
    variables, var_strings = parse_variables(value)
    resolved_value = get_resolved_value(variables[i], thing)
    return value.replace(var_strings[i], str(resolved_value))

def get_resolved_value(variable, thing):
    parts = variable.split('.')
    if parts[1] == 'containers':
        prop = parts[3]
    else:
        prop = parts[1]
    return thing.__dict__[prop]
        
def get_variable_expression(thing, attr):
    parts = attr.split(':')
    field = parts.pop(0)
    if field not in thing.fields:
        raise ConfigException('Invalid attribute: %s' % field)

    indices = []
    for part in parts:
        try:
            indices.append(int(part))
        except ValueError:
            raise ConfigException('Invalid attribute index: %s' % part)

    return field + ''.join(['[%d]' % i for i in indices])

def get_field_value(thing, attr):
    variable_expression = get_variable_expression(thing, attr)
    return eval('thing.%s' % variable_expression)

def update_field(thing, attr, new_value): # pylint: disable=W0613
    variable_expression = get_variable_expression(thing, attr)
    update_expression = 'thing.%s = new_value' % variable_expression
    exec update_expression # pylint: disable=W0122

def all_field_attrs(thing):
    def recurse(value, indices):
        ret = []
        if isinstance(value, (list, tuple)):
            for i, x in enumerate(value):
                ret += recurse(x, indices + [i])
            return ret
        else:
            return [(value, indices)]

    for field in thing.fields:
        if not thing.__dict__[field]:
            continue

        attr = field
        value = thing.__dict__[attr]
        for x, indices in recurse(value, []):
            yield x, attr + ''.join([':%d' % i for i in indices])

class Thing(object):

    def resolve(self, thing, attr, i):
        new_value = resolve(get_field_value(self, attr), thing, i)
        update_field(self, attr, new_value)

class Server(Thing):

    fields = ['provider', 'type', 'bid', 'ip']

    def __init__(self, name, provider=None, type=None, bid=None,
                 ip=None, containers=None):
        self.name = name
        self.provider = provider
        self.type = type
        self.bid = bid
        self.ip = ip
        if containers is None:
            self.containers = {}
        else:
            self.containers = containers

    def resolve(self, thing, attr, i):
        if attr == 'provider':
            self.provider = resolve(self.provider, thing, i)
        if attr == 'type':
            self.type = resolve(self.type, thing, i)
        if attr == 'bid':
            self.bid = resolve(self.bid, thing, i)
        if attr == 'ip':
            self.ip = resolve(self.ip, thing, i)

    def get_thing_name(self):
        return (self.name, None)

    def __repr__(self):
        return '<Server: %s>' % self.name

class Container(Thing):

    fields = ['image', 'command', 'environment', 'ports', 'volumes', 'ip']

    def __init__(self, name, host, image=None, command=None, environment=None,
                 ports=None, volumes=None, ip=None):
        self.name = name
        self.host = host
        self.image = image
        self.command = command
        if environment is None:
            self.environment = []
        else:
            self.environment = environment
        if ports is None:
            self.ports = []
        else:
            self.ports = ports
        if volumes is None:
            self.volumes = []
        else:
            self.volumes = volumes
        self.ip = ip

    def get_thing_name(self):
        return (self.host.name, self.name)

    def __repr__(self):
        return '<Container: %s (%s)>' % (self.name, self.host.name)

class ServerCreateGroup(object):

    def __init__(self, servers):
        first = servers[0]
        assert all([s.provider == first.provider for s in servers])
        assert all([s.type == first.type for s in servers])
        assert all([s.bid == first.bid for s in servers])

        self.servers = servers

    def create(self):
        pass

class DependencyGraph(object):

    def __init__(self):
        self.graph = collections.defaultdict(set)
        self.inverse_graph = collections.defaultdict(set)
        self.dependent_attrs = collections.defaultdict(
            lambda: collections.defaultdict(set))

    def add(self, dependent, attr_i, depends):
        self.graph[depends].add(dependent)
        self.inverse_graph[dependent].add(depends)
        self.dependent_attrs[depends][dependent].add(attr_i)

    def get_depends(self, dependent):
        return self.inverse_graph[dependent]

    def get_dependents(self, depends):
        return self.dependent_attrs[depends]

    def find_cycle(self):
        nodes = set([])
        for depends, dependent_list in self.graph.items():
            nodes.add(depends)
            nodes |= dependent_list

        graph = dict(self.graph)

        # guido's algorithm
        def dfs(node):
            if node in graph:
                for neighbour in graph[node]:
                    yield neighbour
                    dfs(neighbour)

        todo = set(nodes)
        while todo:
            node = todo.pop()
            stack = [node]
            while stack:
                top = stack[-1]
                for node in dfs(top):
                    if node in stack:
                        return stack[stack.index(node):]
                    if node in todo:
                        stack.append(node)
                        todo.remove(node)
                        break
                else:
                    node = stack.pop()

        return None

class ConfigException(Exception):
    def __init__(self, message, server_name=None, container_name=None):
        self.server_name = server_name
        self.container_name = container_name
        super(ConfigException, self).__init__(message)
