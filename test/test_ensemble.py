import time
import unittest2 as unittest

from headintheclouds import ensemble

ensemble.Server.__eq__ = lambda self, other: self.__dict__ == other.__dict__
ensemble.Container.__eq__ = lambda self, other: self.__dict__ == other.__dict__

class TestVariables(unittest.TestCase):

    def test_good_parse_variables(self):
        self.assertEquals(ensemble.parse_variables('$a'), (['a'], ['$a']))
        self.assertEquals(ensemble.parse_variables('${a}'), (['a'], ['${a}']))
        self.assertEquals(ensemble.parse_variables('foo$bar.baz'), (['bar'], ['$bar']))
        self.assertEquals(ensemble.parse_variables('foo${bar.baz}'), (['bar.baz'], ['${bar.baz}']))
        self.assertEquals(ensemble.parse_variables('$foo${bar.baz}'), (['foo', 'bar.baz'], ['$foo', '${bar.baz}']))
        self.assertEquals(ensemble.parse_variables('a$foo-a${bar.baz}a'), (['foo', 'bar.baz'], ['$foo', '${bar.baz}']))
        self.assertEquals(ensemble.parse_variables(''), ([], []))
        self.assertEquals(ensemble.parse_variables('foo'), ([], []))

    def test_parse_variables_bad(self):
        self.assertRaises(ensemble.ConfigException, ensemble.parse_variables, '$')
        self.assertRaises(ensemble.ConfigException, ensemble.parse_variables, '$$')
        self.assertRaises(ensemble.ConfigException, ensemble.parse_variables, '${}')
        self.assertRaises(ensemble.ConfigException, ensemble.parse_variables, '${')
        self.assertRaises(ensemble.ConfigException, ensemble.parse_variables, '${aaa')

    def test_resolve_thing(self):
        thing = ensemble.Server(name='foo', provider='ec2', type='m1.small',
                                bid=0.3, ip='123.123.123.123')
        self.assertEquals(ensemble.resolve('${host.ip}', thing, 0), '123.123.123.123')
        self.assertEquals(ensemble.resolve('$foo${host.type}', thing, 1), '$foom1.small')
        self.assertEquals(ensemble.resolve('${host.provider} $foo', thing, 0), 'ec2 $foo')
        self.assertEquals(ensemble.resolve('${host.bid}', thing, 0), '0.3')

    def test_resolve_server(self):
        thing = ensemble.Server(name='foo', provider='ec2',
                                type='m1.small', bid=0.3, ip='123.123.123.123')
        server = ensemble.Server(name='bar', provider='${foo.provider}',
                                 type='${foo.ip} ${foo.bid} def')
        server.resolve(thing, 'provider', 0)
        self.assertEquals(server.provider, 'ec2')
        server.resolve(thing, 'type', 1)
        self.assertEquals(server.type, '${foo.ip} 0.3 def')
        server.resolve(thing, 'type', 0)
        self.assertEquals(server.type, '123.123.123.123 0.3 def')

    def test_resolve_container(self):
        thing = ensemble.Container(name='foo', host=None, image='image-foo', command='cmd')
        container = ensemble.Container(name='bar', host=None,
                                       command='a ${foo.containers.c1.image} b',
                                       environment=[['${foo.containers.c1.image}', 'bar'],
                                                    ['foo', '${foo.containers.c1.command}']])
        container.resolve(thing, 'command', 0)
        self.assertEquals(container.command, 'a image-foo b')
        container.resolve(thing, 'environment:0:0', 0)
        self.assertEquals(container.environment, [
            ['image-foo', 'bar'], ['foo', '${foo.containers.c1.command}']])
        container.resolve(thing, 'environment:1:1', 0)
        self.assertEquals(container.environment, [['image-foo', 'bar'], ['foo', 'cmd']])

    def test_resolve_existing(self):
        existing_servers = {
            's1': ensemble.Server(name='s1', type='blah'),
            's2': ensemble.Server(name='s2', provider='foo')
        }
        existing_servers['s2'].containers = {
            'c5': ensemble.Container('c5', existing_servers['s2'], command='bbbbaaz')
        }

        servers = {
            's3': ensemble.Server(name='s3', provider='p-${s1.type}'),
            's4': ensemble.Server(name='s4', provider='baz'),
        }
        servers['s4'].containers = {
            'c1': ensemble.Container('c1', servers['s4'], image='${s2.containers.c5.command}')
        }

        graph = ensemble.DependencyGraph()
        graph.add(('s3', None), ('provider', 0), ('s1', None))
        graph.add(('s4', 'c1'), ('image', 0), ('s2', 'c5'))

        ensemble.resolve_existing(servers, graph, existing_servers)
        self.assertEquals(servers['s3'].provider, 'p-blah')
        self.assertEquals(servers['s4'].containers['c1'].image, 'bbbbaaz')

    def test_all_field_attrs(self):
        container = ensemble.Container(name='c1',
                                       host=None,
                                       image='blah',
                                       ports=[[80, 80], [1000, 1001]],
                                       volumes=['vol1', 'vol2', 'vol3'],
                                       environment=[['foo', 'bar'], ['baz', 'qux']])
        expected = [
            ('blah', 'image'),
            ('foo', 'environment:0:0'),
            ('bar', 'environment:0:1'),
            ('baz', 'environment:1:0'),
            ('qux', 'environment:1:1'),
            ('vol1', 'volumes:0'),
            ('vol2', 'volumes:1'),
            ('vol3', 'volumes:2'),
            (80, 'ports:0:0'),
            (80, 'ports:0:1'),
            (1000, 'ports:1:0'),
            (1001, 'ports:1:1'),
        ]
        self.assertEquals(list(ensemble.all_field_attrs(container)), expected)

class TestDependencyGraph(unittest.TestCase):

    def test_find_cycle_positive(self):
        graph = ensemble.DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('b', None, 'a')
        self.assertIsNotNone(graph.find_cycle())

        graph = ensemble.DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('b', None, 'c')
        graph.add('b', None, 'd')
        graph.add('c', None, 'a')
        self.assertIsNotNone(graph.find_cycle())

        graph = ensemble.DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('a', None, 'c')
        graph.add('b', None, 'd')
        graph.add('c', None, 'd')
        graph.add('d', None, 'c')
        self.assertIsNotNone(graph.find_cycle())

    def test_find_cycle_negative(self):
        graph = ensemble.DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('a', None, 'c')
        graph.add('b', None, 'd')
        graph.add('c', None, 'd')
        graph.add('b', None, 'c')
        self.assertIsNone(graph.find_cycle())

class TestExpandTemplate(unittest.TestCase):

    def test_no_template(self):
        config = {
            'foo': 'bar',
            'baz': 'qux',
        }
        expected = config.copy()
        templates = {
            'a': {
                'foo': '123',
                'bar': '456',
            }
        }
        ensemble.expand_template(config, templates)
        self.assertEquals(config, expected)

    def test_overwrite(self):
        config = {
            'foo': 'bar',
            'baz': 'qux',
            '$template': 'a',
        }
        templates = {
            'a': {
                'foo': '123',
                'bar': '456',
            },
            'b': {
                'foo': '789',
            },
        }
        expected = {
            'foo': 'bar',
            'baz': 'qux',
            'bar': '456',
        }
        ensemble.expand_template(config, templates)
        self.assertEquals(config, expected)

    def test_missing_template(self):
        config = {
            'foo': 'bar',
            'baz': 'qux',
            '$template': 'a',
        }
        templates = {
            'b': {
                'foo': '789',
            },
        }
        self.assertRaises(ensemble.ConfigException, ensemble.expand_template,
                          config, templates)

class TestParseServer(unittest.TestCase):

    def test_fields(self):
        server_name = 'serv'
        config = {
            'provider': 'ec2',
            'type': 'm1.large',
            'image': 'foobar',
            'os': 'ubuntu',
            'region': 'us-east',
            'bid': 0.2,
            'internal_ip': '10.0.0.1',
            'ip': '50.0.0.1',
        }
        expected = {
            server_name: ensemble.Server(
                name=server_name,
                provider='ec2',
                type='m1.large',
                image='foobar',
                os='ubuntu',
                region='us-east',
                bid=0.2,
                internal_ip='10.0.0.1',
                ip='50.0.0.1',
            )
        }
        self.assertEquals(ensemble.parse_server(server_name, config, {}),
                          expected)

class TestParseContainer(unittest.TestCase):

    def test_fields(self):
        container_name = 'cont'
        server = ensemble.Server('s1')
        config = {
            'image': 'foo',
            'command': 'bar',
            'environment': {
                'FOO': 123,
                'BAR': 'BAZ',
            },
            'ports': [
                '80',
                '100:200',
            ],
            'volumes': [
                '/tmp',
                '/var/lib',
            ],
            'ip': '172.20.0.2',
        }
        expected = {
            container_name: ensemble.Container(
                name=container_name,
                host=server,
                image='foo',
                command='bar',
                environment=[
                    ['FOO', 123],
                    ['BAR', 'BAZ']
                ],
                ports=[
                    [80, 80],
                    [100, 200]
                ],
                volumes=[
                    '/tmp',
                    '/var/lib'
                ],
                ip='172.20.0.2'
            )
        }
        self.assertEquals(ensemble.parse_container(container_name, config, server, {}),
                          expected)

class TestMultiprocess(unittest.TestCase):

    def setUp(self):
        self.old_server_create_group_create = ensemble.ServerCreateGroup.create
        def new_server_create_group_create(self):
            time.sleep(0.01)
        ensemble.ServerCreateGroup.create = new_server_create_group_create

    def tearDown(self):
        ensemble.ServerCreateGroup.create = self.old_server_create_group_create

    def test_server_group(self):
        graph = ensemble.DependencyGraph()
        servers = {'s%d' % i: DummyServer('s%d' % i) for i in range(3)}
        ensemble.create_things(servers, graph)

    def test_child_container(self):
        s1 = DummyServer('s1')
        c1 = DummyContainer('c1', s1)
        s1.containers = {'c1': c1}
        servers = {'s1': s1}
        graph = ensemble.DependencyGraph()
        graph.add(('s1', 'c1'), None, ('s1', None))
        ensemble.create_things(servers, graph)

    def test_multiple_dependencies(self):
        pass

    def test_resolve(self):
        pass

class DummyServer(ensemble.Server):

    def create(self):
        time.sleep(0.01)

    def refresh(self):
        pass

class DummyContainer(ensemble.Container):

    def create(self):
        time.sleep(0.01)

    def refresh(self):
        pass
