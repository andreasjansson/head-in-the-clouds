from headintheclouds.ensemble.exceptions import ConfigException
from headintheclouds.ensemble.server import Server
from headintheclouds.ensemble.container import Container
from headintheclouds.ensemble.firewall import Firewall

def parse_config(config):
    templates = config.pop('templates', {})

    all_servers = {}

    for server_name, server_spec in config.items():
        if not server_spec:
            server_spec = {}

        try:
            servers = parse_server(server_name, server_spec, templates)
        except ConfigException, e:
            raise ConfigException(e.message, server_name)

        if 'firewall' in server_spec:
            for server in servers.values():
                server.firewall = parse_firewall(server_spec['firewall'], server, templates)

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

    if 'count' in spec:
        if 'provider' not in spec:
            raise ConfigException('count requires a provider')
        count = spec.pop('count')
    else:
        count = 1

    if 'provider' not in spec:
        spec['provider'] = 'unmanaged'

    for i in range(count):
        if spec['provider'] == 'unmanaged':
            name = spec['ip'] = server_name
            if 'ip' in spec and spec['ip'] != name:
                raise ConfigException('No need to specify ip for unmanaged servers, but if you do, the ip must match the name of the server')
        else:
            if i == 0:
                name = server_name
            else:
                name = '%s-%d' % (server_name, i)

        server = Server(name, **spec)
        server.validate()
        servers[server.name] = server

    return servers

def parse_container(container_name, spec, server, templates):
    containers = {}
    expand_template(spec, templates)

    if 'environment' in spec:
        expand_template(spec['environment'], templates)

    count = spec.pop('count', 1)

    valid_fields = set(Container.field_parsers)
    invalid_fields = set(spec) - valid_fields

    if invalid_fields:
        raise ConfigException(
            'Invalid fields: %s' % ', '.join([str(x) for x in invalid_fields]))

    if 'image' not in spec:
        raise ConfigException('Containers require an image')

    for i in range(count):
        if i == 0:
            name = container_name
        else:
            name = '%s-%d' % (container_name, i)
        container = Container(name, server, **spec)
        for field, value_parser in Container.field_parsers.items():
            if field in spec:
                value = spec[field]
                value = value_parser(value)
                container.fields[field] = value

        containers[container.name] = container

    return containers

def parse_firewall(spec, server, templates):
    expand_template(spec, templates)

    rules = {}
    for port, addresses in spec.items():
        split = str(port).split('/', 1)
        if len(split) == 1:
            port = split[0]
            protocols = ['tcp']
        else:
            port, protocol = split
            if protocol == '*':
                protocols = ['tcp', 'udp']
            else:
                protocols = [protocol]

        if port == '*':
            port = None

        if addresses == '*':
            addresses = None

        for protocol in protocols:
            rules[(port, protocol)] = {'port': port, 'protocol': protocol, 'addresses': addresses}

    firewall = Firewall(server, rules)

    return firewall

def expand_template(spec, templates):
    if 'template' in spec:
        template = spec.pop('template')

        if template not in templates:
            raise ConfigException('Missing template: %s' % template)

        for k, v in templates[template].items():
            spec.setdefault(k, v)
