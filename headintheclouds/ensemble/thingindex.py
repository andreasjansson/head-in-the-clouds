from headintheclouds.ensemble.container import Container
from headintheclouds.ensemble.server import Server
from headintheclouds.ensemble.firewall import Firewall

def build_thing_index(servers):
    thing_index = {}
    for server in servers.values():
        thing_index[server.thing_name()] = server
        for container in server.containers.values():
            thing_index[container.thing_name()] = container
        if server.firewall:
            thing_index[server.firewall.thing_name()] = server.firewall
    return thing_index

def refresh_thing_index(thing_index):
    # TODO this starting to get really ugly. need to refactor
    for thing in thing_index.values():
        if isinstance(thing, Server):
            for container_name, container in thing.containers.items():
                thing.containers[container_name] = thing_index[container.thing_name()]
        elif isinstance(thing, Container):
            thing.host = thing_index[thing.host.thing_name()]
        elif isinstance(thing, Firewall):
            thing.host = thing_index[thing.host.thing_name()]

def refresh_servers(servers, thing_index):
    for server_name, server in servers.items():
        updated = servers[server_name] = thing_index[server.thing_name()]
        for container_name, container in server.containers.items():
            updated.containers[container_name] = thing_index[container.thing_name()]
        if server.firewall:
            updated.firewall = thing_index[server.firewall.thing_name()]
