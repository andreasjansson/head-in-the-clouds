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

    def test_run(self):
        command = 'nc -l 8000'
        name = 'test'

        with utils.settings(server_ip):
            docker.run(
                image='ubuntu',
                name=name,
                command=command,
                environment='FOO=bar,HELLO=world',
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
            self.assertEquals(container['image'], 'ubuntu')
            self.assertEquals(container['environment']['FOO'], 'bar')
            self.assertEquals(container['environment']['HELLO'], 'world')
            self.assertEquals(container['state'], 'running')
            self.assertTrue(container['running'])

            contents = 'hello world'
            local('echo "%s" | nc -w 2 %s 8080' % (contents, server_ip))

            self.assertEquals(sudo('docker logs %s' % name), contents)

            container = docker.get_container('test')
            self.assertFalse(container['running'])

    def test_run_volumes(self):
        files = ['foo', 'bar', 'baz']
        command = 'touch ' + ' '.join(['/tmp/%s' % f for f in files])
        name = 'test'
        host_vol = '/docker_vol/tmp'

        with utils.settings(server_ip):
            sudo('rm -rf %s' % host_vol)
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
