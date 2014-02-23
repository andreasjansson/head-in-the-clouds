# BUGS:
#
# * pulling image just for equivalence check is slow, there must be a
#   way to get that through an api
#   - make this parallel

# TODO:
#
# * restart_strategy: {restart_before (default), restart_after}
#
# * refactor and document and make nice
#   - especially "stupid_json_hack" and stuff around bid
#
# * support explicit $depends clause?
#   - might be a use case with containers waiting for other containers 
#     to start before they can
#
# * the ability to "up" a single server/container
#   - would ensemble.up:production,serverX update the serverX-0 or
#     every serverX? need to be able to support both cases. I guess
#     serverX would be the group and serverX-0 the instance.

import os
import sys
import yaml
import re
import collections
import multiprocessing
import simplejson as json

from fabric.api import * # pylint: disable=W0614,W0401
import fabric.api as fab
import fabric.network
from fabric.colors import yellow, red
from fabric.contrib.console import confirm

import headintheclouds
from headintheclouds import docker

__all__ = ['up']

MULTI_THREADED = True

IS_ACTIVE = ('IS_ACTIVE', None)

@runs_once
@task
def up(name, filename=None):
    if filename is None:
        filename = '%s.yml' % name
    if not os.path.exists(filename):
        abort('No such file: %s' % filename)
    with open(filename, 'r') as f:
        config = yaml.load(f)

    servers = parse_config(config)

    sys.stdout.write('Calculating changes...')
    sys.stdout.flush()

    existing_servers = find_existing_servers(servers.keys())
    dependency_graph, changes = process_dependencies(servers, existing_servers)

    cycle_node = dependency_graph.find_cycle()
    if cycle_node:
        raise ConfigException('Cycle detected')

    # import ipdb; ipdb.set_trace()

    print ''
    
    confirm_changes(changes)
    create_things(servers, dependency_graph, changes['changing_servers'],
                  changes['changing_containers'], changes['absent_containers'])

def process_dependencies(servers, existing_servers):
    new_index = build_thing_index(servers)
    existing_index = build_thing_index(existing_servers)

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

    for thing in all_changing_things:
        if isinstance(thing, Container):
            changes['changing_containers'].add(thing)
        else:
            changes['changing_servers'].add(thing)

    for thing in all_new_things:
        if isinstance(thing, Container):
            changes['new_containers'].add(thing)
        else:
            changes['new_servers'].add(thing)

    for server in existing_servers.values():
        for container in server.containers.values():
            if container.thing_name() not in new_index:
                changes['absent_containers'].add(container)

    return dependency_graph, changes

def resolve_existing(thing_name, new_index, existing_index, dependency_graph):
    changing_things = set()
    new_things = set()

    new = new_index[thing_name]
    if thing_name in existing_index:
        existing = existing_index[thing_name]
        if existing.is_equivalent(new):
            new.update(existing.properties())
        else:
            changing_things.add(new)
    else:
        new_things.add(new)

    resolve_dependents(dependency_graph, new, new_index)

    return changing_things, new_things

def resolve_dependents(dependency_graph, depends, thing_index):
    dependents = dependency_graph.get_dependents(depends.thing_name())

    for dependent_name, attr_is in dependents.items():
        dependent = thing_index[dependent_name]
        for attr, i in attr_is:
            resolved = dependent.resolve(depends, attr, i)
            if resolved:
                dependency_graph.remove(dependent_name, (attr, i), depends.thing_name())

def confirm_changes(changes):
    if changes.get('new_servers', None):
        print yellow('The following servers will be created:')
        for server in changes['new_servers']:
            print '%s' % server.name
    if changes.get('new_containers', None):
        print yellow('The following containers will be created:')
        for container in changes['new_containers']:
            print '%s (%s)' % (container.name, container.host.name)
    if changes.get('changing_servers', None):
        print red('The following servers will restart:')
        for server in changes['changing_servers']:
            print '%s' % server.name
    if changes.get('changing_containers', None):
        print red('The following containers will restart:')
        for container in changes['changing_containers']:
            print '%s (%s)' % (container.name, container.host.name)

    if changes:
        if not confirm('Do you wish to continue?'):
            abort('Aborted')

def find_existing_servers(names):
    servers = {}
    for node in headintheclouds.all_nodes():
        if node['name'] in names and node['running']:
            server = Server(active=True, **node)
            with host_settings(server):
                containers = docker.get_containers()
            for container in containers:
                container = Container(host=server, active=True, **container)
                server.containers[container.name] = container
            servers[server.name] = server
        sys.stdout.write('.')
        sys.stdout.flush()

    return servers

def create_things(servers, dependency_graph, changing_servers, changing_containers, absent_containers):
    # TODO: handle errors

    things_to_delete = {thing.thing_name(): thing
                         for thing in changing_servers | changing_containers}

    thing_index = build_thing_index(servers)

    queue = multiprocessing.Queue()
    processes = make_processes(servers, queue, things_to_delete)

    for container in absent_containers:
        container.terminate()

    remaining = set(processes)
    while remaining:
        free_nodes = dependency_graph.get_free_nodes(remaining)
        remaining -= free_nodes
        
        for thing_name in free_nodes:
            processes[thing_name].thing = thing_index[thing_name]
            processes[thing_name].start()

        completed_things = queue.get()
        for thing in completed_things:
            thing_index[thing.thing_name()] = thing
            refresh_thing_index(thing_index)

            resolve_dependents(dependency_graph, thing, thing_index)

def build_thing_index(servers):
    thing_index = {}
    for server in servers.values():
        thing_index[server.thing_name()] = server
        for container in server.containers.values():
            thing_index[container.thing_name()] = container
    return thing_index

def refresh_thing_index(thing_index):
    # TODO this starting to get really ugly. need to refactor
    for thing_name, thing in thing_index.items():
        if isinstance(thing, Server):
            for container_name, container in thing.containers.items():
                thing.containers[container_name] = thing_index[container.thing_name()]
        elif isinstance(thing, Container):
            thing.host = thing_index[thing.host.thing_name()]

def make_processes(servers, queue, things_to_delete):
    processes = {}

    for server in servers.values():
        if not server.active:
            process = UpProcess(server.thing_name(), queue)
            process.thing_to_delete = things_to_delete.get(server.thing_name(), None)
            processes[server.thing_name()] = process

        for container in server.containers.values():
            if not container.active:
                process = UpProcess(container.thing_name(), queue)
                process.thing_to_delete = things_to_delete.get(container.thing_name(), None)
                processes[container.thing_name()] = process

    return processes

class UpProcess(multiprocessing.Process):

    def __init__(self, thing_name, queue, thing_to_delete=None):
        super(UpProcess, self).__init__()
        # works because we don't need to mutate thing anymore once we've forked
        self.thing_name = thing_name
        self.queue = queue
        self.thing = None
        self.thing_to_delete = thing_to_delete

        if not MULTI_THREADED:
            self.start = self.run

    def run(self):
        print '>>>>>>>>>>>>>>>>>>>>>>>>> starting %s' % self.thing

        if MULTI_THREADED:
            fabric.network.disconnect_all()

        if self.thing_to_delete:
            self.thing_to_delete.delete()

        created_things = self.thing.create()
        self.queue.put(created_things)

def parse_config(config):
    if '$templates' in config:
        templates = config['$templates']
        del config['$templates']
    else:
        templates = {}

    all_servers = {}

    for server_name, server_spec in config.items():
        if not server_spec:
            server_spec = {}

        try:
            servers = parse_server(server_name, server_spec, templates)
        except ConfigException, e:
            raise ConfigException(e.message, server_name)

        if 'containers' in server_spec:
            for server in servers.values():
                server.containers = {}
                for container_name, container_spec in server_spec['containers'].items():
                    if not container_spec:
                        container_spec = {}

                    try:
                        containers = parse_container(
                            container_name, container_spec, server, templates)
                    except ConfigException, e:
                        raise ConfigException(e.message, server_name, container_name)
                    server.containers.update(containers)

        all_servers.update(servers)

    return all_servers

def parse_server(server_name, spec, templates):

    # TODO: switching on provider is hack. fix

    expand_template(spec, templates)
    servers = {}

    provider = spec.get('provider', 'unmanaged')
    spec['provider'] = provider

    if '$count' in spec:
        if provider == 'unmanaged':
            raise ConfigException('$count requires a provider')
        count = spec['$count']
        del spec['$count']
    else:
        count = 1

    for i in range(count):

        if provider == 'unmanaged':
            name = spec['ip'] = server_name
            if 'ip' in spec and spec['ip'] != name:
                raise ConfigException('No need to specify ip for unmanaged servers, but if you do, the ip must match the name of the server')
            spec['active'] = True
        else:
            name = '%s-%d' % (server_name, i)

        server = Server(name, **spec)
        server.validate()
        servers[server.name] = server

    return servers

def parse_container(container_name, spec, server, templates):
    containers = {}
    expand_template(spec, templates)

    valid_fields = {'$count'} | set(Container.fields)
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

def get_raw_dependency_graph(servers):
    dependency_graph = DependencyGraph()

    for server in servers.values():
        for value, attr in all_field_attrs(server):
            new_value = resolve_or_add_dependency(value, attr, servers, dependency_graph, server)
            if new_value:
                update_field(server, attr, new_value)

        for container in server.containers.values():
            for value, attr in all_field_attrs(container):
                new_value = resolve_or_add_dependency(value, attr, servers, dependency_graph, server, container)
                if new_value:
                    update_field(container, attr, new_value)

            # dependency so that containers need to wait for the server to start
            dependency_graph.add(container.thing_name(), IS_ACTIVE, server.thing_name())

    return dependency_graph

def get_servers_parameterised_json(servers):
    server_dicts = {}
    for server_name, server in servers.items():
        keys = server.possible_options(stupid_json_hack=True)
        server_dicts[server_name] = {}
        for key in keys:
            server_dicts[server_name][key] = '${%s.%s}' % (server_name, key)
    return json.dumps(server_dicts)

def resolve_or_add_dependency(value, attr, servers, dependency_graph, server, container=None):
    if value == '$servers':
        value = get_servers_parameterised_json(servers)

    variables = parse_variables(value)
    for var_string, var in variables.items():
        parts = var.split('.')
        if parts[0] == 'host':
            depends_server = server.name
        else:
            depends_server = parts[0]
            if depends_server not in servers:
                depends_server += '-0' # default to first

        if parts[1] == 'containers':
            if not container:
                raise ConfigException(
                    'Server to container dependencies are currently unsupported')
            depends_container = parts[2]
            if depends_container not in servers[depends_server].containers:
                depends_container += '-0' # default to first
        else:
            depends_container = None

        dependency_graph.add((server.name, container.name if container else None),
                             (attr, var_string), (depends_server, depends_container))

    return value

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

def resolve(value, thing, var_string):
    variables = parse_variables(value)
    resolved_value = get_resolved_value(variables[var_string], thing)
    if resolved_value is None:
        return None
    return value.replace(var_string, str(resolved_value))

def get_resolved_value(variable, thing):
    parts = variable.split('.')
    if parts[1] == 'containers':
        prop = parts[3]
    else:
        prop = parts[1]
    return getattr(thing, prop, None)
        
def get_variable_expression(thing, attr):
    parts = attr.split(':')
    field = parts.pop(0)
    if field not in thing.possible_options():
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

    for field in thing.properties():
        attr = field
        value = getattr(thing, attr)
        if attr != 'name' and value is not None:
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
        '"ports" should be a list in the format "FROM[:TO][/udp]": %s' % value)
    if not isinstance(value, list):
        raise error
    ports = []
    for x in value:
        x = str(x)
        try:
            fr, to, protocol = docker.parse_port_spec(x)
        except ValueError:
            raise error

        ports.append([fr, to, protocol])
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
        if (attr, i) == IS_ACTIVE:
            return thing.active

        new_value = resolve(get_field_value(self, attr), thing, i)
        if new_value is None:
            return False
        update_field(self, attr, new_value)
        return True

    def update(self, props):
        for prop, value in props.items():
            setattr(self, prop, value)

class Server(Thing):

    # TODO: use node['running'] instead of 'active'

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
        self.active = True
        self.post_create()
        return [self]

    def post_create(self):
        if self.containers:
            with host_settings(self):
                docker.setup()

    def delete(self):
        with host_settings(self):
            self.server_provider().terminate()

    def thing_name(self):
        return (self.name, None)

    def validate(self):
        valid_options = self.server_provider().create_server_defaults.keys()
        given_options = {k: v for k, v in self.__dict__.items()
                         if k not in ('name', 'provider', 'containers', 'active')
                         and v is not None}
        invalid_options = set(given_options) - set(valid_options)
        if invalid_options:
            raise ConfigException('Invalid options: %s' % invalid_options)

        create_options = self.get_create_options()
        try:
            new_options = self.server_provider().validate_create_options(**create_options)
        except ValueError, e:
            raise ConfigException('Invalid options: %s' % e)

        # ec2 image can be a short hand, need to normalise it to real ami id
        # for equivalence comparisons to work
        for param, value in new_options.items():
            self.__dict__[param] = value

    def is_equivalent(self, other):
        return (self.provider == other.provider
                and self.server_provider().equivalent_create_options(
                    self.get_create_options(), other.get_create_options()))

    def get_create_options(self):
        create_options = self.server_provider().create_server_defaults.copy()
        create_options.update({k: v for k, v in self.__dict__.items()
                               if k in create_options
                               and v is not None})
        return create_options

    def possible_options(self, stupid_json_hack=False):
        options = (set(self.get_create_options()) |
                   set(self.__dict__) - {'containers'})

        if stupid_json_hack:
            # some arbitrary constraints. shouldn't be here
            # TODO: clean up this
            if self.provider == 'ec2':
                options -= {'bid'}
            elif self.provider == 'digitalocean':
                options -= {'internal_address'}

        return options

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
                 ports=None, volumes=None, ip=None, active=None, 
                 state=None, created=None):
        self.name = name
        self.host = host
        self.image = image
        self.command = command
        self.environment = environment or []
        self.ports = ports or []
        self.volumes = volumes or []
        self.ip = ip
        self.active = active
        self.state = state
        self.created = created

    def get_create_options(self):
        return self.fields.keys()

    def possible_options(self):
        return (set(self.fields) |
                set(self.__dict__))

    def thing_name(self):
        return (self.host.name, self.name)

    def create(self):
        with host_settings(self.host):
            docker.pull_image(self.image)
            container = docker.run_container(
                image=self.image, name=self.name,
                command=self.command, environment=self.environment,
                ports=self.ports, volumes=self.volumes)
            self.update(container)
        return [self]

    def delete(self):
        with host_settings(self.host):
            docker.kill(self.name)

    def is_equivalent(self, other):
        return (self.host.is_equivalent(other.host)
                and self.name == other.name
                and self.is_equivalent_command(other)
                and self.is_equivalent_environment(other)
                and self.are_equivalent_ports(other)
                and set(self.volumes) == set(other.volumes)
                and self.is_equivalent_image(other))

    def is_equivalent_image(self, other):
        if self.image != other.image:
            return False

        if self.image == other.image:
            with host_settings(self.host):
                with settings(hide('everything')):
                    pulled_image_id = docker.pull_image(other.image)
                    other_image_id = docker.get_image_id(other.name)

            sys.stdout.write('.')
            sys.stdout.flush()

            return pulled_image_id == other_image_id

    def is_equivalent_command(self, other):
        # can't know for sure, so playing safe
        # self will be the remote machine!
        return (other.command is None
                or self.command == other.command)

    def are_equivalent_ports(self, other):
        # same here, can't know for sure,
        # self will be the remote machine!
        public_ports = []
        for fr, to, protocol in self.ports:
            if to is not None:
                public_ports.append([fr, to, protocol])
        return sorted(public_ports) == sorted(other.ports)

    def is_equivalent_environment(self, other):
        ignored_keys = {'HOME', 'PATH'} # for now
        this_dict = {k: v for k, v in self.environment}
        other_dict = {k: v for k, v in other.environment}
        for k in set(this_dict) | set(other_dict):
            if k in ignored_keys:
                continue

            # compare apples with 'apples'
            if str(this_dict.get(k, None)) != str(other_dict.get(k, None)):
                return False
        return True

    def properties(self):
        return {k: v for k, v in self.__dict__.items()
                if k not in ('host')
                and v is not None}

    def __repr__(self):
        return '<Container: %s (%s)>' % (self.name, self.host.name if self.host else None)

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

    def remove(self, dependent, attr_i, depends):
        self.dependent_attrs[depends][dependent] = self.dependent_attrs[depends][dependent] - {attr_i}
        if not self.dependent_attrs[depends][dependent]:
            del self.dependent_attrs[depends][dependent]

            self.graph[depends] = self.graph[depends] - {dependent}
            if not self.graph[depends]:
                del self.graph[depends]

            self.inverse_graph[dependent] = self.inverse_graph[dependent] - {depends}
            if not self.inverse_graph[dependent]:
                del self.inverse_graph[dependent]

            if not self.dependent_attrs[depends]:
                del self.dependent_attrs[depends]

    def get_depends(self, dependent):
        return self.inverse_graph[dependent]

    def get_dependents(self, depends):
        return self.dependent_attrs[depends]

    def find_cycle(self):
        nodes = set()
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

    def get_free_nodes(self, all_nodes):
        return all_nodes - set(self.inverse_graph)

class ConfigException(Exception):
    def __init__(self, message, server_name=None, container_name=None):
        self.server_name = server_name
        self.container_name = container_name
        super(ConfigException, self).__init__(message)
