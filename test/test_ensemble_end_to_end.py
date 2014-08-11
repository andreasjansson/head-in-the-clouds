import unittest2 as unittest
from fabric.api import * # pylint: disable=W0614,W0401
from utils import create_instance, check_changes, make_changes
import uuid
import time

class TestEnsembleEndToEnd(unittest.TestCase):

    def setUp(self):
        self.name = 'test-%s' % uuid.uuid4()
        self.instance = None

    def tearDown(self):
        if self.instance and self.instance.running:
            self.instance.terminate()

    def test_digitalocean(self):
        config = '''
%(name)s:
  provider: digitalocean
  image: Ubuntu 14.04 x64
  size: 512MB
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.assertTrue(self.instance.is_pingable())
        self.assertTrue(self.instance.has_open_port(22))
        self.instance.terminate()
        time.sleep(5)
        self.assertFalse(self.instance.is_pingable())

    def test_ec2(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: t1.micro
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.assertTrue(self.instance.is_pingable())
        self.assertTrue(self.instance.has_open_port(22))
        self.instance.terminate()
        time.sleep(5)
        self.assertFalse(self.instance.is_pingable())

    def test_instance_firewall(self):
        config = '''
%(name)s:
  provider: digitalocean
  image: Ubuntu 14.04 x64
  size: 512MB
  firewall:
    81: "*"
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.instance.netcat_listen(80)
        self.instance.netcat_listen(81)
        self.assertTrue(self.instance.is_pingable())
        self.assertFalse(self.instance.has_open_port(80))
        self.assertTrue(self.instance.has_open_port(81))
        self.instance.terminate()
        
    def test_container_firewall(self):
        config = '''
%(name)s:
  provider: digitalocean
  image: Ubuntu 14.04 x64
  size: 512MB
  containers:
    test80:
      image: andreasjansson/hello
      ports:
        - 80
      environment:
        PORT: 80
        MESSAGE: foo
    test81:
      image: andreasjansson/hello
      ports:
        - 81
      environment:
        PORT: 81
        MESSAGE: bar
  firewall:
    81: "*"
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.assertEquals(self.instance.call_port(80, "baz"), None)
        self.assertEquals(self.instance.call_port(81, "baz"), "bar")
        self.instance.terminate()
        
    def test_modify_firewall(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: t1.micro
  firewall:
    12345: "*"
        ''' % {'name': self.name}
        self.instance = create_instance(config)
        self.assertEquals(check_changes(config), {})
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: t1.micro
  firewall:
    12345: 1.1.1.1
        ''' % {'name': self.name}
        self.assertEquals(list(check_changes(config)['changing_firewalls'])[0].host.name, self.name)
        make_changes(config)
        self.assertEquals(check_changes(config), {})

        # TODO: handle removing firewalls
