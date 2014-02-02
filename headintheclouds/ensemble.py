import os
import yaml
import re
import collections
from fabric.api import *

from headintheclouds import ec2, do, docker

@task
def up(name):
    filename = '%s.yml' % name
    if not os.path.exists(filename):
        abort('No such file: %s' % filename)

    try:
        server_confs, dependency_graph = parse_config_file(filename)
    except ConfigException, e:
        abort(str(e))

    import ipdb; ipdb.set_trace()

def parse_config_file(filename):
    with open(filename, 'r') as f:
        raw = yaml.load(f)

    if '$templates' in raw:
        templates = raw['$templates']
        del raw['$templates']
    else:
        templates = {}

    dependency_graph = DependencyGraph()
    server_confs = {}

    for server, spec in raw.items():
        server_conf = parse_server(server, spec, templates)

        if 'containers' in spec:
            server_conf.containers = parse_containers(spec['containers'], server, templates)

        server_confs[server] = server_conf

    add_dependencies(server_confs, dependency_graph)
    cycle_node = dependency_graph.find_cycle()
    if cycle_node:
        server, container = cycle_node
        raise ConfigException('Cycle detected in <%s>' % (server if container is None else '%s %s' % (server, container)))

    return server_confs, dependency_graph

def parse_server(server, spec, templates):
    expand_template(spec, templates, server)
    
    count = spec.get('$count', 1)
    server_conf = ServerConf(name=server, count=count)

    valid_fields = set(['$count', 'containers'])

    if 'provider' in spec:
        valid_fields |= set(['provider', 'type', 'bid'])

        server_conf.provider = spec['provider']
        if 'type' not in spec:
            raise ConfigException('"type" missing in <%s>' % server)
        server_conf.type = spec['type']
        if 'bid' in spec:
            server_conf.bid = spec['bid']
            valid_fields.add('bid')

    elif 'ip' in spec:
        server_conf.ip = spec['ip']
        valid_fields.add('ip')
    else:
        raise ConfigException('Missing "provider" or "ip" in <%s>' % server)

    if set(spec) - valid_fields:
        raise ConfigException('Invalid fields in <%s>: %s' % (
            server, ', '.join(set(spec) - valid_fields)))

    return server_conf

def parse_containers(specs, server, templates):
    container_confs = {}
    for container, spec in specs.items():
        expand_template(spec, templates, server)

        valid_fields = set(['$count', 'image', 'environment', 'command',
                            'ports', 'volumes'])
        if set(spec) - valid_fields:
            raise ConfigException('Invalid fields in <%s %s>: %s' % (
                server, container, ', '.join(set(spec) - valid_fields)))

        count = spec.get('$count', 1)
        name = container
        for i in range(count):
            if count > 1:
                container = '%s-%d' % (name, i)
            container_conf = ContainerConf(container)

            if 'image' not in spec:
                raise ConfigException('"image" missing in <%s %s>' % (server, container))
            container_conf.image = spec['image']
            container_conf.command = spec.get('command', None)
            container_conf.volumes = spec.get('volumes', [])
            container_conf.environment = spec.get('environment', {})

            for port_spec in spec.get('ports', []):
                split = str(port_spec).split(':', 2)
                if len(split) == 1:
                    fr = to = split[0]
                else:
                    fr, to = split
                container_conf.ports.append((fr, to))

            container_confs[container] = container_conf

    return container_confs

def expand_template(spec, templates, server):
    if '$template' in spec:
        template = spec['$template']
        del spec['$template']

        if template not in templates:
            raise ConfigException('Missing template in <%s>: %s' % server, template)

        for k, v in templates[template].items():
            if k not in spec:
                spec[k] = v

def add_dependencies(server_confs, dependency_graph):
    for server, server_conf in server_confs.items():
        ad = lambda value, attr: add_dependency(value, attr, dependency_graph, server)
        ad(server_conf.provider, 'provider')
        ad(server_conf.type, 'type')
        ad(server_conf.bid, 'bid')
        ad(server_conf.ip, 'ip')

        for container, container_conf in server_conf.containers.items():
            ad = lambda value, attr: add_dependency(value, attr, dependency_graph, server, container)
            ad(container_conf.image, 'image')
            ad(container_conf.command, 'command')
            for k, v in container_conf.environment.items():
                ad(k, 'env')
                ad(v, 'env:%s' % k)
            for i, (fr, to) in enumerate(container_conf.ports):
                ad(fr, 'port:%d:from' % i)
                ad(to, 'port:%d:from' % i)
            for i, volume in enumerate(container_conf.volumes):
                ad(volume, 'volume:%d' % i)

def add_dependency(value, attr, dependency_graph, server, container=None):
    variables, var_strings = parse_variables(value)
    for var in variables:
        parts = var.split('.')
        if parts[0] == 'host':
            depends_server = server
        else:
            depends_server = parts[0]
        if parts[1] == 'containers':
            depends_container = parts[2]
        else:
            depends_container = None
        dependency_graph.add(server, container, attr, depends_server, depends_container)

def parse_variables(value):
    variables = []
    var_strings = []
    while '$' in str(value):
        start = value.index('$')
        if start == value.index('${'):
            end = value.index('}')
            var = value[start + 2:end - 1]
        else:
            match = re.search('[^a-zA-Z0-9_]', value)
            if match:
                end = match.group(1).span[0]
            else:
                end = len(value)
            var = value[start + 1:end]
        variables.append(var)
        var_strings.append(value[start:end])
        value = value[end:]
    return variables, var_strings
        
class ServerConf(object):

    def __init__(self, name, provider=None, type=None, bid=None,
                 ip=None, containers=None, count=None):
        self.name = name
        self.provider = provider
        self.type = type
        self.bid = bid
        self.ip = ip
        if containers is None:
            self.containers = {}
        else:
            self.containers = containers
        self.count = count

class ContainerConf(object):

    def __init__(self, name, image=None, command=None, environment=None,
                 ports=None, volumes=None):
        self.name = name
        self.image = image
        self.command = command
        if environment is None:
            self.environment = {}
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

class DependencyGraph(object):

    def __init__(self):
        self.graph = collections.defaultdict(list)

    def add(self, server, container, attr, server_depends, container_depends):
        self.graph[(server_depends, container_depends)].append((server, container, attr))

    def find_cycle(self):
        nodes = set([])
        for depends, dependent_list in self.graph.items():
            nodes.add(depends)
            for attr, server_dependent, container_dependent in dependent_list:
                nodes.add((server_dependent, container_dependent))

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
                print stack
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
    pass
