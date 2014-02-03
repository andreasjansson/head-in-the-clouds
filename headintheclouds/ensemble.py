import os
import yaml
import re
import collections
import multiprocessing
import Queue
from fabric.api import *

from headintheclouds import ec2, do, docker

@task
def up(name):
    filename = '%s.yml' % name
    if not os.path.exists(filename):
        abort('No such file: %s' % filename)

    try:
        servers, dependency_graph = parse_config_file(filename)
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
    for existing_name, existing_server in existing_servers.items():
        dependents = dependency_graph.get_dependents((existing_name, None))
        for (server_name, container_name), attr_is in dependents.items():
            if container_name:
                dependent = servers[server_name][container_name]
            else:
                dependent = servers[server_name]
            for attr, i in attr_is:
                dependent.resolve(existing_server, attr, i)
        for existing_container_name, existing_container in existing_server.containers.items():
            dependents = dependency_graph.get_dependents((existing_name, existing_container_name))
            for (server_name, container_name), attr_is in dependents.items():
                if container_name:
                    dependent = servers[server_name].containers[container_name]
                else:
                    dependent = servers[server_name]
                for attr, i in attr_is:
                    dependent.resolve(existing_container, attr, i)
            
def create_things(servers, dependency_graph):
    # TODO: handle errors

    queue = Queue.Queue()
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
            dependents = dependency_graph.get_dependents(
                (thing.host.name, thing.name) if isinstance(thing, Container) else (thing.name, None))
            for (server_name, container_name), attr_is in dependents.items():
                if container_name:
                    dependent = servers[server_name].containers[container_name]
                else:
                    dependent = servers[server_name]
                process = processes[dependent]
                process.to_resolve -= 1
                for attr, i in attr_is:
                    dependent.resolve(thing, attr, i)
                if process.is_resolved():
                    process.start()

def make_processes(servers, dependency_graph, queue):
    processes = {}
    resolved_servers = collections.defaultdict(list)

    for server in servers.values():
        depends = dependency_graph.get_depends((server.name, None))
        if len(depends) == 0:
            resolved_servers[(server.provider, server.type, server.bid)].append(server)
        else:
            processes[server] = UpProcess(server, len(depends), queue)
        for container in server.containers.values():
            depends = dependency_graph.get_depends((server.name, container.name))
            processes[server] = UpProcess(container, len(depends), queue)

    for servers in resolved_servers.values():
        server_group = ServerCreateGroup(servers)
        processes[server_group] = UpProcess(server_group, 0, queue)

    return processes

class UpProcess(multiprocessing.Process):

    def __init__(self, thing, to_resolve, queue):
        multiprocessing.Process.__init__(self)
        # works because we don't need to mutate thing anymore once we've forked
        self.thing = thing
        self.to_resolve = to_resolve
        self.queue = queue

    def run(self):
        print self.thing.__dict__

    def is_resolved(self):
        return self.to_resolve == 0

def parse_config_file(filename):
    with open(filename, 'r') as f:
        raw = yaml.load(f)

    if '$templates' in raw:
        templates = raw['$templates']
        del raw['$templates']
    else:
        templates = {}

    all_servers = {}

    for server_name, spec in raw.items():
        servers = parse_server(server_name, spec, templates)

        if 'containers' in spec:
            # TODO: optimise so we don't have to do all this duplicate work
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
            container.environment = spec.get('environment', {}).items()

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
        avd = lambda value, attr: add_variable_dependency(
            value, attr, servers, dependency_graph, server)
        avd(server.provider, 'provider')
        avd(server.type, 'type')
        avd(server.bid, 'bid')
        avd(server.ip, 'ip')

        for container in server.containers.values():
            avd = lambda value, attr: add_variable_dependency(
                value, attr, servers, dependency_graph, server, container)
            avd(container.image, 'image')
            avd(container.command, 'command')
            for i, (k, v) in enumerate(container.environment):
                avd(k, 'env-key:%d' % i)
                avd(v, 'env:value:%d' % i)
            for i, (fr, to) in enumerate(container.ports):
                avd(fr, 'port-from:%d' % i)
                avd(to, 'port-to:%d' % i)
            for i, volume in enumerate(container.volumes):
                avd(volume, 'volume:%d' % i)

            # need its own server to start first
            dependency_graph.add((server.name, container.name), None, (server.name, None))

    return dependency_graph

def add_variable_dependency(value, attr, servers, dependency_graph, server, container=None):
    # TODO: validate that it's actually possible to get the value
    # (e.g. host.asdf, host.container.blah.bid neither make sense)

    variables, var_strings = parse_variables(value)
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

        dependency_graph.add(server.name, container.name if container else None, (attr, i), depends_server, depends_container)

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
        
class Server(object):

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

    def __repr__(self):
        return '<Server: %s>' % self.name

class Container(object):

    def __init__(self, name, host, image=None, command=None, environment=None,
                 ports=None, volumes=None):
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

    def resolve(self, thing, attr, i):
        if attr == 'image':
            self.image = resolve(self.image, thing, i)
        elif attr == 'command':
            self.command = resolve(self.command, thing, i)
        elif attr.startswith('env-key:'):
            n = int(attr.split(':')[1])
            self.environment[n] = (resolve(self.environment[n][0], thing, i), self.environment[n][1])
        elif attr.startswith('env-value:'):
            n = int(attr.split(':')[1])
            self.environment[n] = (self.environment[n][0], resolve(self.environment[n][1], thing, i))
        elif attr.startswith('port-from:'):
            n = int(attr.split(':')[1])
            self.ports[n] = (resolve(self.ports[n][0], thing, i), self.ports[n][1])
        elif attr.startswith('port-to:'):
            n = int(attr.split(':')[1])
            self.ports[n] = (self.ports[n][0], resolve(self.ports[n][1], thing, i))
        elif attr.startswith('volume:'):
            n = int(attr.split(':')[1])
            self.volumes[n] = resolve(self.volumes[n], thing, i)

    def __repr__(self):
        return '<Container: %s (%s)>' % (self.name, self.host.name)

class ServerCreateGroup(object):

    def __init__(self, servers):
        first = servers[0]
        assert all([s.provider == first.provider for s in servers])
        assert all([s.type == first.type for s in servers])
        assert all([s.bid == first.bid for s in servers])

        self.servers = servers

class DependencyGraph(object):

    def __init__(self):
        self.graph = collections.defaultdict(set)
        self.inverse_graph = collections.defaultdict(set)
        self.dependent_attrs = collections.defaultdict(
            lambda: collections.defaultdict(set))

    def add(self, dependent, attr_i, depends):
        self.graph[depends].add(dependent)
        self.inverse_graph[dependent].add(depends)
        if attr_i:
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
