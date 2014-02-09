import os
import yaml
import re
import collections
import multiprocessing
import uuid

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab
from fabric.colors import green, red
from fabric.contrib.console import confirm

import headintheclouds
from headintheclouds import docker

__all__ = ['up']

@task
@runs_once
def up(name, filename=None):
    if filename is None:
        filename = '%s.yml' % name
    if not os.path.exists(filename):
        abort('No such file: %s' % filename)
    with open(filename, 'r') as f:
        config = yaml.load(f)

    servers = parse_config(config)

    existing_servers = find_existing_servers(servers.keys())
    changing_servers, changing_containers = update_servers_with_existing(
        servers, existing_servers)

    dependency_graph = resolve_and_get_dependencies(servers)
    cycle_node = dependency_graph.find_cycle()
    if cycle_node:
        raise ConfigException('Cycle detected')

    if changing_servers or changing_containers:
        confirm_changes(changing_servers, changing_containers)

    create_things(servers, dependency_graph)

def confirm_changes(changing_servers, changing_containers):
    if changing_servers:
        print red('The following servers are about to change:')
        for existing_server, server in changing_servers:
            print '%s [%s]' % (existing_server.name, existing_server.ip)
    if changing_containers:
        print red('The following containers are about to change:')
        for existing_container, container in changing_containers:
            print '%s (%s [%s])' % (existing_container.name,
                                    existing_container.host.name,
                                    existing_container.host.ip)
    if not confirm('Do you wish to continue?'):
        abort('Aborted')

def update_servers_with_existing(servers, existing_servers):
    changing_servers = []
    changing_containers = []

    for server_name, server in servers.items():
        if server_name in existing_servers:
            existing_server = existing_servers[server_name]

            if existing_server.is_equivalent(server):
                server.update(existing_server.properties())
                existing_containers = existing_server.containers

                for container_name, container in server.containers.items():
                    if container_name in existing_server.containers:
                        existing_container = existing_server.containers[container_name]
                        if existing_container.is_equivalent(container):
                            container.update(existing_container.properties())
                        else:
                            changing_containers.append((existing_container, container))
            else:
                changing_servers.append((existing_server, server))

    return changing_servers, changing_containers

def find_existing_servers(names):
    servers = {}
    for node in headintheclouds.all_nodes():
        if node['name'] in names:
            server = Server(active=True, **node)
            with host_settings(server):
                containers = docker.get_containers()
            for container in containers:
                container = Container(host=server, active=True, **container)
                server.containers[container.name] = container
            servers[server.name] = server
    return servers

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
            if isinstance(thing, Server):
                servers[thing.name] = thing
            dependents = dependency_graph.get_dependents(thing.thing_name())
            for (server_name, container_name), attr_is in dependents.items():
                if container_name:
                    dependent = servers[server_name].containers[container_name]
                else:
                    dependent = servers[server_name]
                process = processes[dependent.thing_name()]
                process.to_resolve -= 1
                for attr_i in attr_is:
                    if attr_i:
                        attr, i = attr_i
                        dependent.resolve(thing, attr, i) 
                if process.is_resolved():
                    print '--------->>>>>>>>>>>> starting %s' % process.thing
                    process.start()

def make_processes(servers, dependency_graph, queue):
    processes = {}
    resolved_servers = collections.defaultdict(list)

    for server in servers.values():
        if not server.active:
            depends = dependency_graph.get_depends(server.thing_name())
            if False and len(depends) == 0: # disable grouping for now.
                for group_server, group in resolved_servers.items():
                    if group_server.is_equivalent(server):
                        group.append(server)
                        break
                else:
                    resolved_servers[server].append(server)
            else:
                processes[server.thing_name()] = UpProcess(server, len(depends), queue)
        for container in server.containers.values():
            if not container.active:
                depends = dependency_graph.get_depends(container.thing_name())
                processes[container.thing_name()] = UpProcess(container, len(depends), queue)

    for servers in resolved_servers.values():
        if len(servers) > 1:
            server_group = ServerCreateGroup(servers)
            processes[server_group.thing_name()] = UpProcess(server_group, 0, queue)
        else:
            server = servers[0]
            processes[server.thing_name()] = UpProcess(server, 0, queue)

    return processes

class UpProcess(multiprocessing.Process):

    def __init__(self, thing, to_resolve, queue):
        super(UpProcess, self).__init__()
        # works because we don't need to mutate thing anymore once we've forked
        self.thing = thing
        self.to_resolve = to_resolve
        self.queue = queue

    def run(self):
        global env
        env = env.copy()

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

    for server_name, server_spec in config.items():
        try:
            servers = parse_server(server_name, server_spec, templates)
        except ConfigException, e:
            raise ConfigException(e.message, server_name)

        if 'containers' in server_spec:
            for server in servers.values():
                server.containers = {}
                for container_name, container_spec in server_spec['containers'].items():
                    try:
                        containers = parse_container(
                            container_name, container_spec, server, templates)
                    except ConfigException, e:
                        raise ConfigException(e.message, server_name, container_name)
                    server.containers.update(containers)

        all_servers.update(servers)

    return all_servers

def parse_server(server_name, spec, templates):
    expand_template(spec, templates)
    servers = {}
    
    count = spec.get('$count', 1)
    if '$count' in spec:
        del spec['$count']
    for i in range(count):
        server = Server('%s-%d' % (server_name, i), **spec)
        server.validate()
        servers[server.name] = server

    return servers

def parse_container(container_name, spec, server, templates):
    containers = {}
    expand_template(spec, templates)

    valid_fields = set(['$count']) | set(Container.fields)
    if set(spec) - valid_fields:
        raise ConfigException(
            'Invalid fields: %s' % ', '.join(set(spec) - valid_fields))

    count = spec.get('$count', 1)
    if '$count' in spec:
        del spec['$count']
    for i in range(count):
        container = Container('%s-%d' % (container_name, i), server)

        for field, value_parser in Container.fields.items():
            if field in spec:
                value = spec[field]
                value = value_parser(value)
                container.__dict__[field] = value

        containers[container.name] = container

    return containers

def expand_template(spec, templates):
    if '$template' in spec:
        template = spec['$template']
        del spec['$template']

        if template not in templates:
            raise ConfigException('Missing template: %s' % template)

        for k, v in templates[template].items():
            if k not in spec:
                spec[k] = v

def resolve_and_get_dependencies(servers):
    dependency_graph = DependencyGraph()

    for server in servers.values():
        for value, attr in all_field_attrs(server):
            resolve_or_add_dependency(value, attr, servers, dependency_graph, server)

        for container in server.containers.values():
            for value, attr in all_field_attrs(container):
                resolve_or_add_dependency(value, attr, dependency_graph, server, container)

            # need its own server to start first
            if not server.active:
                dependency_graph.add(container.thing_name(), None, server.thing_name())

    return dependency_graph

def resolve_or_add_dependency(value, attr, servers, dependency_graph, server, container=None):
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
                    'Server to container dependencies are currently unsupported')
            depends_container = parts[2]
        else:
            depends_container = None

        if container:
            dependent = container
        else:
            dependent = server
        if depends_container:
            depends = servers[depends_server].containers[depends_container]
        else:
            depends = servers[depends_server]

        could_resolve = dependent.resolve(depends, attr, i)
        if not could_resolve:
            dependency_graph.add((server.name, container.name if container else None),
                                 (attr, i), (depends_server, depends_container))

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
    if field not in thing.get_create_options():
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

    for field in thing.get_create_options():
        if not hasattr(thing, field):
            continue

        attr = field
        value = getattr(thing, attr)
        for x, indices in recurse(value, []):
            yield x, attr + ''.join([':%d' % i for i in indices])

def parse_string(value):
    if not isinstance(value, basestring):
        raise ConfigException('Value is not a string: "%s"' % value)
    return value

def parse_provider(value):
    known_providers = ['ec2', 'digitalocean']
    if value not in known_providers:
        raise ConfigException('Invalid provider: "%s", valid providers are %s' %
                              (value, known_providers))
    return value

def parse_float(value):
    try:
        return float(value)
    except ValueError:
        raise ConfigException('Value is not a float: "%s"' % value)

def parse_list(value):
    if not isinstance(value, list):
        raise ConfigException('Value is not a list: "%s"' % value)
    return value

def parse_ports(value):
    error = ConfigException(
        '"ports" should be a list of colon-separated integers: %s' % value)
    if not isinstance(value, list):
        raise error
    ports = []
    for x in value:
        split = x.split(':')
        if len(split) == 1:
            fr = to = split[0]
        elif len(split) == 2:
            fr, to = split
        else:
            raise error
        try:
            fr = int(fr)
            to = int(to)
        except ValueError:
            raise error
        ports.append([fr, to])
    return ports

def parse_environment(value):
    if not isinstance(value, dict):
        raise ConfigException(
            '"environment" should be a dictionary: %s' % value)
    return [list(x) for x in value.items()]

def host_settings(server):
    settings = {
        'provider': server.provider,
        'host': server.ip,
        'host_string': server.ip,
    }
    settings.update(server.server_provider().settings)
    return fab.settings(**settings)

class Thing(object):

    def resolve(self, thing, attr, i):
        new_value = resolve(get_field_value(self, attr), thing, i)
        update_field(self, attr, new_value)

    def update(self, props):
        for prop, value in props.items():
            setattr(self, prop, value)

class Server(Thing):

    def __init__(self, name, provider=None, containers=None,
                 ip=None, internal_address=None, active=None,
                 **kwargs):
        self.name = name
        self.provider = provider
        self.ip = ip
        self.internal_address = internal_address
        self.__dict__.update(kwargs)
        if containers is None:
            self.containers = {}
        else:
            self.containers = containers
        self.active = active

    def create(self):
        create_options = self.get_create_options()
        node = self.server_provider().create_servers(
            names=[self.name], count=1, **create_options)[0]
        self.update(node)
        self.post_create()

    def post_create(self):
        if self.containers:
            with host_settings(self):
                docker.setup()

    def thing_name(self):
        return (self.name, None)

    def validate(self):
        valid_options = self.server_provider().create_server_defaults.keys()
        given_options = {k: v for k, v in self.__dict__.items()
                         if k not in ('name', 'provider', 'containers')
                         and v is not None}
        invalid_options = set(given_options) - set(valid_options)
        if invalid_options:
            raise ConfigException('Invalid options: %s' % invalid_options)

        create_options = self.get_create_options()
        try:
            self.server_provider().validate_create_options(**create_options)
        except ValueError, e:
            raise ConfigException('Invalid options: %s' % e)

    def is_equivalent(self, other):
        return (self.provider == other.provider
                and hasattr(self.server_provider(), 'equivalent_create_options')
                and self.server_provider().equivalent_create_options(
                    self.get_create_options(), other.get_create_options()))

    def get_create_options(self):
        create_options = self.server_provider().create_server_defaults.copy()
        create_options.update({k: v for k, v in self.__dict__.items()
                               if k in create_options
                               and v is not None})
        return create_options

    def properties(self):
        return {k: v for k, v in self.__dict__.items()
                if k not in ('containers')
                and v is not None}

    def server_provider(self):
        return headintheclouds.provider_by_name(self.provider)

    def __repr__(self):
        return '<Server: %s>' % self.name

class Container(Thing):

    fields = {
        'image': parse_string,
        'command': parse_string,
        'environment': parse_environment,
        'ports': parse_ports,
        'volumes': parse_list,
        'ip': parse_string
    }
    
    def __init__(self, name, host, image=None, command=None, environment=None,
                 ports=None, volumes=None, ip=None, active=None, state=None,
                 created=None):
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
        self.active = active
        self.state = state
        self.created = created

    def get_create_options(self):
        return self.fields.keys()

    def thing_name(self):
        return (self.host.name, self.name)

    def create(self):
        with host_settings(self.host):
            print '--------->>>>>>>>>>>> run_container %s' % self.host
            docker.run_container(
                image=self.image, name=self.name,
                command=self.command, environment=self.environment,
                ports=self.ports, volumes=self.volumes)

    def is_equivalent(self, other):
        return (self.name == other.name
                and self.image == other.image
                and self.command == other.command
                and self.is_equivalent_environment(other)
                and set(self.ports) == set(other.ports)
                and set(self.volumes) == set(other.volumes))

    def is_equivalent_environment(self, other):
        ignored_keys = set(['HOME', 'PATH']) # for now
        this_dict = {k: v for k, v in self.environment}
        other_dict = {k: v for k, v in other.environment}
        for k in set(this_dict) | set(other_dict):
            if k in ignored_keys:
                continue
            if this_dict[k] != other_dict[k]:
                return False
        return True

    def properties(self):
        return {k: v for k, v in self.__dict__.items()
                if k not in ('host')
                and v is not None}

    def __repr__(self):
        return '<Container: %s (%s)>' % (self.name, self.host.name if self.host else None)

class ServerCreateGroup(object):

    def __init__(self, servers):
        self.servers = servers

    def create(self):
        first = self.servers[0]
        create_options = first.get_create_options()
        names = [s.name for s in self.servers]
        nodes = first.server_provider().create_servers(
            count=len(self.servers), names=names, **create_options)
        for server in self.servers:
            node = [n for n in nodes if n['name'] == server.name][0]
            server.update(node)
            server.post_create()

    def thing_name(self):
        return uuid.uuid4() # never used, just random

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
