from testconfig import config
from headintheclouds import digitalocean
import fabric.api as fab

def get_server():
    if config.get('ip'):
        return config.get('ip')

    nodes = digitalocean.create_servers(
        count=1, size='512MB', image='Ubuntu 12.04.3 x64',
        names=['unit-test-server'], placement='New York 1'
    )
    return nodes[0]['ip']

def done_with_server(ip):
    if not config.get('ip'):
        with settings(ip):
            digitalocean.terminate()

def settings(ip):
    return fab.settings(host_string=ip, host=ip, **digitalocean.settings)
