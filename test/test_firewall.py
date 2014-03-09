import uuid
import unittest2 as unittest
from fabric.api import * # pylint: disable=W0614,W0401
from headintheclouds import firewall
import utils
import requests

server_ip = None
def setUpModule():
    global server_ip
    server_ip = utils.get_server()

def tearDownModule():
    utils.done_with_server(server_ip)

class TestFirewall(unittest.TestCase):

    def setUp(self):
        with utils.settings(server_ip):
            if firewall.has_chain():
                with utils.settings(server_ip):
                    iptables(firewall.flush_chain)
            else:
                with utils.settings(server_ip):
                    iptables(firewall.make_chain)
                    iptables(firewall.jump_to_chain())

    def test_has_chain(self):
        with utils.settings(server_ip):
            iptables(firewall.delete_jump())
            iptables(firewall.delete_chain)
            self.assertFalse(firewall.has_chain())
            iptables(firewall.make_chain)
            iptables(firewall.jump_to_chain())
            self.assertTrue(firewall.has_chain())

    def test_inbound(self):
        with utils.settings(server_ip):
            firewall.set_rules([
                (None, 22, None, None),
                (None, 10000, None, None),
            ])

            sudobg('nc -l 10000')
            sudobg('nc -l 10001')

        self.assertTrue(is_accessible(server_ip, 22))
        self.assertTrue(is_accessible(server_ip, 10000))
        self.assertFalse(is_accessible(server_ip, 10000))

    def test_outbound(self):
        with utils.settings(server_ip):
            firewall.set_rules([
                (None, 22, None, None),
            ])

            sudobg('nc -l 80')

        self.assertFalse(is_accessible(server_ip, 80))
        self.assertTrue(is_accessible_from_inside('google.com', 80))

    def test_my_ip(self):
        with utils.settings(server_ip):
            firewall.set_rules([
                (None, 22, None, None),
                (None, None, None, [get_my_ip()]),
            ])

            sudobg('nc -l 80')

        self.assertTrue(is_accessible(server_ip, 80))

    def test_other_ip(self):
        with utils.settings(server_ip):
            firewall.set_rules([
                (None, 22, None, None),
                (None, None, None, ['1.2.3.4']),
            ])

            sudobg('nc -l 80')

        self.assertFalse(is_accessible(server_ip, 80))

    def test_get_rules(self):
        rules = [
            (None, 22, None, None),
            (None, None, None, ['1.2.3.4']),
            (None, 12345, 'udp', ['1.2.3.4', '5.6.7.8']),
            (None, 5678, None, ['1.2.3.4,5.6.7.8']),
        ]

        with utils.settings(server_ip):
            firewall.set_rules(rules)

            new_rules = firewall.make_rules(rules)
            existing_rules = firewall.get_rules()

        new_rules = [r for r in new_rules if r != firewall.flush_chain]

        self.assertEquals(new_rules, existing_rules)

def sudobg(cmd):
    sockname = 'dtach.%s' % uuid.uuid4()
    with settings(hide('everything'), warn_only=True):
        if local('which dtach').failed:
            sudo('apt-get install -y dtach')
    
    return sudo('dtach -n `mktemp -u /tmp/%s.XXXX` %s'  % (sockname, cmd))

def iptables(cmd):
    sudo('iptables ' + cmd)

def is_accessible(ip, port):
    with settings(hide('everything'), warn_only=True):
        return not local('timeout 1 nc -z %s %d' % (ip, port)).failed

def is_accessible_from_inside(ip, port):
    with settings(hide('everything'), warn_only=True):
        with utils.settings(server_ip):
            return not run('timeout 1 nc -z %s %d' % (ip, port)).failed

def get_my_ip():
    return requests.get('http://httpbin.org/ip').json()['origin']
