import headintheclouds
from headintheclouds import docker
from headintheclouds.ensemble import remote
from headintheclouds.ensemble.exceptions import ConfigException
from headintheclouds.ensemble.thing import Thing

class Server(Thing):

    def __init__(self, name, provider=None, containers=None, firewall=None, **kwargs):
        super(Server, self).__init__()

        self.name = name
        self.provider = provider

        self.fields.update(kwargs)
        self.fields['name'] = name

        if containers is None:
            self.containers = {}
        else:
            self.containers = containers

        self.firewall = firewall

    def get_ip(self):
        return self.fields['ip']

    def is_active(self):
        return self.fields['running', False]

    def create(self):
        create_options = self.get_create_options()
        node = self.server_provider().create_servers(
            names=[self.name], count=1, **create_options)[0]
        self.update(node)
        self.post_create()
        return [self]

    def post_create(self):
        if self.containers:
            with remote.host_settings(self):
                docker.setup()

    def delete(self):
        with remote.host_settings(self):
            self.server_provider().terminate()

    def validate(self):
        valid_options = self.server_provider().create_server_defaults.keys() + ['name']
        given_options = {k for k, v in self.fields.items()
                         if v is not None}
        invalid_options = given_options - set(valid_options)
        if invalid_options:
            raise ConfigException('Invalid options: %s' % invalid_options)

        create_options = self.get_create_options()
        try:
            new_options = self.server_provider().validate_create_options(**create_options)
        except ValueError, e:
            raise ConfigException('Invalid options: %s' % e)

        # ec2 image can be a short hand, need to normalise it to real ami id
        # for equivalence comparisons to work
        for param, value in new_options.items():
            self.fields[param] = value

    def is_equivalent(self, other):
        return (self.provider == other.provider
                and self.server_provider().equivalent_create_options(
                    self.get_create_options(), other.get_create_options()))

    def get_create_options(self):
        create_options = self.server_provider().create_server_defaults.copy()
        create_options.update({k: v for k, v in self.fields.items()
                               if k in create_options and v is not None})
        return create_options

    def server_provider(self):
        return headintheclouds.provider_by_name(self.provider)

    def thing_name(self):
        return ('SERVER', self.name)

    def __repr__(self):
        return '<Server: %s>' % self.name

