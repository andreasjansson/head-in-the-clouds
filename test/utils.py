from testconfig import config
from headintheclouds import digitalocean
import fabric.api as fab

def get_server():
    if config.get('ip'):
        return config.get('ip')

    nodes = digitalocean.create_servers(
        count=1, size='512MB', image=digitalocean.create_server_defaults['image'],
        names=['unit-test-server'], placement='New York 1'
    )

    return nodes[0]['ip']

def done_with_server(ip):
    if not config.get('ip'):
        with settings(ip):
            digitalocean.terminate()

def settings(ip, **other_settings):
    all_other_settings = digitalocean.settings
    all_other_settings.update(other_settings)
    return fab.settings(host_string=ip, host=ip, provider='digitalocean', **all_other_settings)
