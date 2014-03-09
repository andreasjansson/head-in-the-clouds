import sys
import fabric.api as fab

import headintheclouds
from headintheclouds import docker
from headintheclouds.ensemble.server import Server
from headintheclouds.ensemble.container import Container

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

def host_settings(server):
    settings = {
        'provider': server.provider,
        'host': server.get_ip(),
        'host_string': server.get_ip(),
    }
    settings.update(server.server_provider().settings)
    return fab.settings(**settings)

