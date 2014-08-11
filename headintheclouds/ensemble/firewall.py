from headintheclouds import firewall
from headintheclouds.ensemble.thing import Thing
from headintheclouds.ensemble.remote import host_settings

class Firewall(Thing):

    def __init__(self, host, rules=None):
        super(Firewall, self).__init__()
        self.host = host
        self.fields['rules'] = rules or {}

    def thing_name(self):
        return ('FIREWALL', self.host.name)

    def create(self):
        with host_settings(self.host):
            firewall.set_rules(self.get_open_list(), ('FORWARD', 'INPUT'))
        return [self]

    def is_equivalent(self, other):
        with host_settings(self.host):
            return firewall.rules_are_active(other.get_open_list(), ('FORWARD', 'INPUT'))

    def get_open_list(self):
        return [(None, r['port'], r['protocol'], r['addresses'])
                for r in self.fields['rules'].values()]

    def is_active(self):
        return self.fields['active']

def exists(server):
    with host_settings(server):
        return firewall.has_chain()
