import sys
import multiprocessing
import fabric
from fabric.colors import yellow, red
import fabric.api as fab
from fabric.contrib.console import confirm

import headintheclouds
from headintheclouds import docker
from headintheclouds.ensemble import remote
from headintheclouds.ensemble import dependency
from headintheclouds.ensemble import thingindex
from headintheclouds.ensemble.server import Server
from headintheclouds.ensemble.container import Container
from headintheclouds.ensemble import firewall
from headintheclouds.ensemble import exceptions

MULTI_THREADED = True

def create_things(servers, dependency_graph, changing_servers, changing_containers, absent_containers):
    # TODO: handle errors

    things_to_change = {t.thing_name(): t for t in changing_servers | changing_containers}

    thing_index = thingindex.build_thing_index(servers)

    queue = multiprocessing.Queue()
    processes = make_processes(servers, queue, things_to_change)
    n_completed = 0

    for container in absent_containers:
        container.delete()

    remaining = set(processes)
    while remaining:
        free_nodes = dependency_graph.get_free_nodes(remaining)
        remaining -= free_nodes
        
        for thing_name in free_nodes:
            processes[thing_name].thing = thing_index[thing_name]
            processes[thing_name].start()

            if not free_nodes:
                raise exceptions.RuntimeException('No free nodes in the dependency graph!')

        completed_things, exception = queue.get()
        n_completed += 1

        if exception:
            raise exception

        for t in completed_things:
            thing_index[t.thing_name()] = t
            thingindex.refresh_thing_index(thing_index)

            dependency.resolve_dependents(dependency_graph, t, thing_index)

            # TODO: raise exception if no things can be resolved (instead of stalling (shouldn't be possible but could repro if sestting an env var to ${host.internal_ip} (instead of internal_ip) due to another bug))

    while n_completed < len(processes):
        completed_things, exception = queue.get()
        n_completed += 1

        if exception:
            raise exception

        for t in completed_things:
            thing_index[t.thing_name()] = t
            thingindex.refresh_thing_index(thing_index)
            dependency.resolve_dependents(dependency_graph, t, thing_index)

    return thing_index

def make_processes(servers, queue, things_to_delete):
    processes = {}

    for server in servers.values():
        if not server.is_active():
            process = UpProcess(server.thing_name(), queue)
            process.thing_to_delete = things_to_delete.get(server.thing_name(), None)
            processes[server.thing_name()] = process

        for container in server.containers.values():
            if not container.is_active():
                process = UpProcess(container.thing_name(), queue)
                process.thing_to_delete = things_to_delete.get(container.thing_name(), None)
                processes[container.thing_name()] = process

        if server.firewall:
            if not server.firewall.is_active():
                process = UpProcess(server.firewall.thing_name(), queue)
                processes[server.firewall.thing_name()] = process

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

        exception = None
        created_things = None
        try:
            self.thing.pre_create()

            if self.thing_to_delete:
                self.thing_to_delete.delete()

            created_things = self.thing.create()
        except Exception, e:
            exception = e

        self.queue.put((created_things, exception))

def confirm_changes(changes):
    if changes.get('new_servers', None):
        print yellow('The following servers will be created:')
        for server in changes['new_servers']:
            print '%s' % server.name

    if changes.get('new_containers', None):
        print yellow('The following containers will be created:')
        for container in changes['new_containers']:
            print '%s (%s)' % (container.name, container.host.name)

    if changes.get('new_firewalls', None):
        print yellow('The following servers will get new firewalls:')
        for fw in changes['new_firewalls']:
            print '%s (%s rules total)' % (fw.host.name, len(fw.fields['rules']))

    if changes.get('changing_servers', None):
        print red('The following servers will restart:')
        for server in changes['changing_servers']:
            print '%s' % server.name

    if changes.get('changing_containers', None):
        print red('The following containers will restart:')
        for container in changes['changing_containers']:
            print '%s (%s)' % (container.name, container.host.name)

    if changes.get('changing_firewalls', None):
        print red('The following servers will get updated firewalls:')
        for fw in changes['changing_firewalls']:
            print '%s (%s rules total)' % (fw.host.name, len(fw.fields['rules']))

#    if changes.get('absent_containers', None):
#        print red('The following containers will be deleted:')
#        for container in changes['absent_containers']:
#           print '%s (%s)' % (container.name, container.host.name)

    if set(changes) - {'absent_containers'}:
        if not confirm('Do you wish to continue?'):
            fab.abort('Aborted')

def find_existing_servers(names):
    servers = {}

    queue = multiprocessing.Queue()
    processes = []

    for node in headintheclouds.all_nodes():
        if node['name'] in names and node['running']:
            server = Server(**node)
            processes.append(DiscoverProcess(server, queue))

    for process in processes:
        process.start()

    responses = 0
    while responses < len(processes):
        server, exception = queue.get()
        if exception:
            raise exception

        servers[server.name] = server
        responses += 1

        sys.stdout.write('.')
        sys.stdout.flush()

    return servers

class DiscoverProcess(multiprocessing.Process):

    def __init__(self, server, queue):
        super(DiscoverProcess, self).__init__()
        # works because we don't need to mutate thing anymore once we've forked
        self.server = server
        self.queue = queue

        if not MULTI_THREADED:
            self.start = self.run

    def run(self):
        if MULTI_THREADED:
            fabric.network.disconnect_all()

        exception = None

        try:
            with remote.host_settings(self.server):
                containers = docker.get_containers()
            for container in containers:
                container = Container(host=self.server, **container)
                self.server.containers[container.name] = container
            if firewall.exists(self.server):
                self.server.firewall = firewall.Firewall(self.server)
                self.server.firewall.fields['active'] = True
        except Exception, e:
            exception = e

        self.queue.put((self.server, exception))
