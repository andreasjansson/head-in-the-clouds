class ConfigException(Exception):
    def __init__(self, message, server_name=None, container_name=None):
        self.server_name = server_name
        self.container_name = container_name
        super(ConfigException, self).__init__(message)
