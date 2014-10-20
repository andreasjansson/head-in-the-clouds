import unittest2 as unittest
from fabric.api import * # pylint: disable=W0614,W0401
from utils import create_instance, check_changes, make_changes
import uuid
import time

class TestEnsemble(unittest.TestCase):

    def setUp(self):
        test_id = self.id().split('.')[-1].replace('_', '-')
        self.name = 'test-%s-%s' % (uuid.uuid4(), test_id)
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
        time.sleep(20)
        self.assertFalse(self.instance.is_pingable())

    def test_ec2(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.assertTrue(self.instance.is_pingable())
        self.assertTrue(self.instance.has_open_port(22))
        self.instance.terminate()
        time.sleep(20)
        self.assertFalse(self.instance.is_pingable())

    def test_instance_firewall(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  firewall:
    8080: "*"
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.instance.netcat_listen(80)
        self.instance.netcat_listen(8080)
        self.assertTrue(self.instance.is_pingable())
        self.assertFalse(self.instance.has_open_port(80))
        self.assertTrue(self.instance.has_open_port(8080))
        self.instance.terminate()
        
    def test_container_firewall(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  containers:
    test80:
      image: andreasjansson/hello
      ports:
        - 80
      environment:
        PORT: 80
        MESSAGE: foo
    test8080:
      image: andreasjansson/hello
      ports:
        - 8080
      environment:
        PORT: 8080
        MESSAGE: bar
  firewall:
    8080: "*"
        ''' % {
            'name': self.name
        }
        self.instance = create_instance(config)
        self.assertEquals(self.instance.call_port(80), None)
        self.assertEquals(self.instance.call_port(8080), "bar")
        self.instance.terminate()
        
    def test_modify_firewall(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  firewall:
    12345: "*"
        ''' % {'name': self.name}
        self.instance = create_instance(config)
        self.assertEquals(check_changes(config), {})
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  firewall:
    12345: 1.1.1.1
        ''' % {'name': self.name}
        self.assertEquals(list(check_changes(config)['changing_firewalls'])[0].host.name, self.name)
        make_changes(config)
        self.assertEquals(check_changes(config), {})

        # TODO: handle removing firewalls

    def test_modify_container(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  containers:
    test:
      image: andreasjansson/hello
      ports:
        - 80
      environment:
        PORT: 80
        MESSAGE: foo
        ''' % {'name': self.name}
        self.instance = create_instance(config)
        self.assertEquals(check_changes(config), {})

        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  containers:
    test:
      image: andreasjansson/hello
      ports:
        - 80
      environment:
        PORT: 80
        MESSAGE: bar
        ''' % {'name': self.name}
        self.assertEquals(list(check_changes(config)['changing_containers'])[0].host.name, self.name)
        make_changes(config)
        self.assertEquals(check_changes(config), {})
        
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  containers:
    test:
      image: andreasjansson/hello
      ports:
        - 80
        - 81
      environment:
        PORT: 80
        MESSAGE: bar
        ''' % {'name': self.name}
        self.assertEquals(list(check_changes(config)['changing_containers'])[0].host.name, self.name)
        make_changes(config)
        self.assertEquals(check_changes(config), {})
        
    def test_container_dependencies(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
  containers:
    test1:
      image: andreasjansson/hello
      ports:
        - 80
      environment:
        PORT: 80
        MESSAGE: foo
    test2:
      image: andreasjansson/hello
      ports:
        - 8080
      environment:
        PORT: 8080
        MESSAGE: ${host.containers.test1.ip}
        ''' % {'name': self.name}

        self.instance = create_instance(config)
        test1_ip = self.instance.containers['test1'].fields['ip']
        self.assertEquals(self.instance.call_port(8080), test1_ip)
        self.assertEquals(check_changes(config), {})

    def test_modify_server(self):
        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.small
        ''' % {'name': self.name}

        self.instance = create_instance(config)
        self.assertEquals(check_changes(config), {})

        config = '''
%(name)s:
  provider: ec2
  image: ami-a427efcc
  size: m1.medium
        ''' % {'name': self.name}
        self.assertEquals(list(check_changes(config)['changing_servers'])[0].name, self.name)
        make_changes(config)
        self.assertEquals(check_changes(config), {})
