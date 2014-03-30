class ConfigException(Exception):

    def __init__(self, message, server_name=None, container_name=None):
        self.server_name = server_name
        self.container_name = container_name
        super(ConfigException, self).__init__(message)

    def __str__(self):
        s = self.message
        if self.server_name:
            s += ' (in %s' % self.server_name
            if self.container_name:
                s += '.containers.%s' % self.container_name
            s += ')'
        return s

class RuntimeException(Exception):
    pass
