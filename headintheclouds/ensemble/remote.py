import fabric.api as fab

def host_settings(server):
    settings = {
        'provider': server.provider,
        'host': server.get_ip(),
        'host_string': server.get_ip(),
    }
    settings.update(server.server_provider().settings)
    return fab.settings(**settings)

