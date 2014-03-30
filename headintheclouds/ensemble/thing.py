class Thing(object):

    def __init__(self):
        self.fields = FieldList()

    def update(self, other):
        if isinstance(other, Thing):
            fields = other.fields.items()
        else:
            fields = other.items()
        for prop, value in fields:
            self.fields[prop] = value

    def pre_create(self):
        pass

class FieldList(dict):

    def __getitem__(self, field_index):
        if isinstance(field_index, (list, tuple)):
            name, index = field_index
        else:
            name = field_index
            index = None

        if name in self:
            value = super(FieldList, self).__getitem__(name)
        else:
            value = None

        if not value:
            return value
        if not index:
            return value

        for i in index:
            value = value[i]

        return value

    def __setitem__(self, field_index, value):
        if isinstance(field_index, (list, tuple)):
            name, index = field_index
        else:
            name = field_index
            index = None

        if not index:
            return super(FieldList, self).__setitem__(name, value)

        current_value = self[name]
        for i in index[:-1]:
            current_value = current_value[i]
        current_value[index[-1]] = value

    def indexed_items(self):
        for name, value in self.items():
            for x in walk_field(name, value, []):
                yield x

def walk_field(name, value, index):
    if isinstance(value, (list, tuple)):
        for i, x in enumerate(value):
            for yielded in walk_field(name, x, index + [i]):
                yield yielded
    elif isinstance(value, dict):
        for i, x in value.items():
            for yielded in walk_field(name, x, index + [i]):
                yield yielded
    else:
        yield (name, index), value
