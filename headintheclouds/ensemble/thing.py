class Thing(object):

    def __init__(self):
        self.fields = FieldList()

    def update(self, props):
        for prop, value in props.items():
            setattr(self, prop, value)

class FieldList(dict):

    def __getitem__(self, field_index):
        name, index = field_index
        value = super(FieldList, self).__getitem__(name)
        if not value:
            return None
        if not index:
            return value

        for i in index:
            if not isinstance(value, list):
                raise ValueError('%s is not a list' % value)
            value = value[i]

        return value

    def __setitem__(self, field_index, value):
        name, index = field_index
        if not index:
            super(FieldList, self).__setitem__(name, value)

        current_value = self[field_index]
        for i in index[:-1]:
            if not isinstance(current_value, list):
                raise ValueError('%s is not a list' % current_value)
            current_value = current_value[i]
        current_value[index[-1]] = value

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
