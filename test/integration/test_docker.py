import time
import unittest2 as unittest
from fabric.api import * # pylint: disable=W0614,W0401
from headintheclouds import docker
import utils

server_ip = None
def setUpModule():
    global server_ip
    server_ip = utils.get_server()

def tearDownModule():
    utils.done_with_server(server_ip)

class TestDocker(unittest.TestCase):

    def setUp(self):
        with utils.settings(server_ip):
            docker.setup()
            which_docker = run('which docker')
            self.assertEquals(which_docker, '/usr/bin/docker')

    def tearDown(self):
        with utils.settings(server_ip, warn_only=True):
            sudo('rm -rf /docker_vol')

    def test_ports(self):
        command = 'nc -l 8000'
        name = random_name()

        with utils.settings(server_ip):
            docker.run(
                image='ubuntu',
                name=name,
                command=command,
                ports='8000:8080,1000/udp,2000:3000/udp,4000/tcp',
            )

            container = docker.get_container(name)
            self.assertEquals(container['name'], name)
            self.assertEquals(container['command'], command)
            ports = set(tuple(p) for p in container['ports'])
            self.assertEquals(ports, {
                (8000, 8080, 'tcp'),
                (1000, 1000, 'udp'),
                (2000, 3000, 'udp'),
                (4000, 4000, 'tcp'),
            })
            self.assertEquals(container['state'], 'running')
            self.assertTrue(container['running'])

            contents = 'hello world'
            local('echo "%s" | nc -w 2 %s 8080' % (contents, server_ip))

            self.assertEquals(sudo('docker logs %s' % name), contents)

            container = docker.get_container(name)
            self.assertFalse(container['running'])

    def test_image(self):
        name = random_name()
        image = 'andreasjansson/redis'

        with utils.settings(server_ip):
            docker.run(
                image=image,
                name=name,
                environment='MAXMEMORY=100M',
                ports='6379',
            )

            container = docker.get_container(name)
            self.assertEquals(container['image'], image)
            ports = set(tuple(p) for p in container['ports'])
            self.assertEquals(ports, {
                (6379, 6379, 'tcp'),
            })
            self.assertTrue(container['running'])

            def redis_query(query):
                return local('(echo "%s"; sleep 1) | nc -C -w 2 %s 6379' % (query, server_ip), capture=True)

            redis_query('set foo bar')
            self.assertEquals(redis_query('get foo').splitlines()[-1], 'bar')

            docker.kill(name)
            container = docker.get_container(name)
            self.assertIsNone(container)

    def test_environment(self):
        command = 'sleep 1000'
        name = random_name()

        with utils.settings(server_ip):
            docker.run(
                image='ubuntu',
                name=name,
                command=command,
                environment='FOO=bar,HELLO=world',
            )

            container = docker.get_container(name)
            self.assertEquals(container['environment']['FOO'], 'bar')
            self.assertEquals(container['environment']['HELLO'], 'world')
            self.assertTrue(container['running'])

            docker.kill(name)
            container = docker.get_container(name)
            self.assertIsNone(container)

    def test_command(self):
        command = 'sleep 1000'
        name = random_name()

        with utils.settings(server_ip):
            docker.run(
                image='ubuntu',
                name=name,
                command=command,
            )

            container = docker.get_container(name)
            self.assertEquals(container['name'], name)
            self.assertEquals(container['command'], command)
            self.assertEquals(container['state'], 'running')
            self.assertTrue(container['running'])

            docker.kill(name)
            container = docker.get_container(name)
            self.assertIsNone(container)

    def test_volumes(self):
        files = ['foo', 'bar', 'baz']
        command = 'touch ' + ' '.join(['/tmp/%s' % f for f in files])
        name = random_name()
        host_vol = '/docker_vol/tmp'

        with utils.settings(server_ip):
            docker.run(
                image='ubuntu',
                name=name,
                command=command,
                volumes='%s:/tmp' % host_vol,
            )

            container = docker.get_container(name)
            self.assertEquals(container['volumes'], {host_vol: '/tmp'})
            self.assertFalse(container['running'])

            ls = run('ls -1 %s' % host_vol)
            self.assertEquals(set(ls.splitlines()), set(files))


random_t = time.time()
random_i = 0
def random_name():
    global random_i
    name = '%s_%s' % (int(random_t), random_i)
    random_i += 1
    return name
