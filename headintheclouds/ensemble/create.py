import multiprocessing
import fabric
from fabric.colors import yellow, red
import fabric.api as fab
from fabric.contrib.console import confirm

from headintheclouds.ensemble import thing
from headintheclouds.ensemble import dependency

MULTI_THREADED = True

def create_things(servers, dependency_graph, changing_servers, changing_containers, absent_containers):
    # TODO: handle errors

    things_to_delete = {t.thing_name(): t for t in changing_servers | changing_containers}

    thing_index = thing.build_thing_index(servers)

    queue = multiprocessing.Queue()
    processes = make_processes(servers, queue, things_to_delete)

#    for container in absent_containers:
#        container.delete()

    remaining = set(processes)
    while remaining:
        free_nodes = dependency_graph.get_free_nodes(remaining)
        remaining -= free_nodes
        
        for thing_name in free_nodes:
            processes[thing_name].thing = thing_index[thing_name]
            processes[thing_name].start()

        completed_things = queue.get()
        for t in completed_things:
            thing_index[t.thing_name()] = t
            thing.refresh_thing_index(thing_index)

            dependency.resolve_dependents(dependency_graph, t, thing_index)

            # TODO: raise exception if no things can be resolved (instead of stalling (shouldn't be possible but could repro if sestting an env var to ${host.internal_ip} (instead of internal_address) due to another bug))

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
#    if changes.get('absent_containers', None):
#        print red('The following containers will be deleted:')
#        for container in changes['absent_containers']:
#           print '%s (%s)' % (container.name, container.host.name)

    if changes:
        if not confirm('Do you wish to continue?'):
            fab.abort('Aborted')

