from headintheclouds.ensemble.exceptions import ConfigException
from headintheclouds.ensemble import Server, Container, FirewallRule

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

        if 'firewall' in server_spec:
            for server in servers.values():
                server.firewall_rules = parse_firewall(server_spec['firewall'])

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

    if '$count' in spec:
        if 'provider' not in spec:
            raise ConfigException('$count requires a provider')
        count = spec['$count']
        del spec['$count']
    else:
        count = 1

    if 'provider' not in spec:
        spec['provider'] = 'unmanaged'

    for i in range(count):

        if spec['provider'] == 'unmanaged':
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

    count = spec.get('$count', 1)
    if '$count' in spec:
        del spec['$count']

    valid_fields = set(Container.field_parsers)
    if set(spec) - valid_fields:
        raise ConfigException(
            'Invalid fields: %s' % ', '.join(set(spec) - valid_fields))

    for i in range(count):
        container = Container('%s-%d' % (container_name, i), server, **spec)

        for field, value_parser in Container.field_parsers.items():
            if field in spec:
                value = spec[field]
                value = value_parser(value)
                container.__dict__[field] = value

        containers[container.name] = container

    return containers

def parse_firewall(spec, templates):
    rules = []
    expand_template(spec, templates)

    for port, host in rules:
        if isinstance(host, list):
            for h in host:
                rules.append(Rule(host=h, port=port))
        else:
            rules.append(Rule(host=host, port=port))

    return rules

def expand_template(spec, templates):
    if '$template' in spec:
        template = spec['$template']
        del spec['$template']

        if template not in templates:
            raise ConfigException('Missing template: %s' % template)

        for k, v in templates[template].items():
            if k not in spec:
                spec[k] = v

