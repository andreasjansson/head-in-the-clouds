import time
import unittest2 as unittest
import yaml
import simplejson as json
import mox

from headintheclouds import docker
from headintheclouds import ec2

from headintheclouds.ensemble import parse
from headintheclouds.ensemble import dependency
from headintheclouds.ensemble import thingindex
from headintheclouds.ensemble import create
from headintheclouds.ensemble.dependencygraph import DependencyGraph
from headintheclouds.ensemble.server import Server
from headintheclouds.ensemble.container import Container
from headintheclouds.ensemble.exceptions import ConfigException

def container_equals(self, other):
    self_dict = self.__dict__.copy()
    other_dict = other.__dict__.copy()
    if 'host' in self_dict:
        self_dict['host'] = str(self_dict['host'])
    if 'host' in other_dict:
        other_dict['host'] = str(other_dict['host'])
    return self_dict == other_dict

Server.__eq__ = lambda self, other: self.__dict__ == other.__dict__
Container.__eq__ = container_equals

class TestVariables(unittest.TestCase):

    def test_good_parse_variables(self):
        self.assertEquals(dependency.parse_variables('$a'), {'$a': 'a'})
        self.assertEquals(dependency.parse_variables('${a}'), {'${a}': 'a'})
        self.assertEquals(dependency.parse_variables('foo$bar.baz'), {'$bar': 'bar'})
        self.assertEquals(dependency.parse_variables('foo${bar.baz}'), {'${bar.baz}': 'bar.baz'})
        self.assertEquals(dependency.parse_variables('$foo${bar.baz}'), {'$foo': 'foo', '${bar.baz}': 'bar.baz'})
        self.assertEquals(dependency.parse_variables('a$foo-a${bar.baz}a'), {'$foo': 'foo', '${bar.baz}': 'bar.baz'})
        self.assertEquals(dependency.parse_variables(''), {})
        self.assertEquals(dependency.parse_variables('foo'), {})

    def test_parse_variables_bad(self):
        self.assertRaises(ConfigException, dependency.parse_variables, '$')
        self.assertRaises(ConfigException, dependency.parse_variables, '$$')
        self.assertRaises(ConfigException, dependency.parse_variables, '${}')
        self.assertRaises(ConfigException, dependency.parse_variables, '${')
        self.assertRaises(ConfigException, dependency.parse_variables, '${aaa')

    # def test_resolve_thing(self):
    #     thing = Server(name='foo', provider='ec2', size='m1.small',
    #                             bid=0.3, ip='123.123.123.123', internal_address=None)
    #     self.assertEquals(ensemble.resolve('${host.ip}', thing, '${host.ip}'), '123.123.123.123')
    #     self.assertEquals(ensemble.resolve('$foo${host.size}', thing, '${host.size}'), '$foom1.small')
    #     self.assertEquals(ensemble.resolve('${host.provider} $foo', thing, '${host.provider}'), 'ec2 $foo')
    #     self.assertEquals(ensemble.resolve('${host.bid}', thing, '${host.bid}'), '0.3')

    # def test_resolve_server(self):
    #     thing = Server(name='foo', security_group='asdf',
    #                             size='m1.small', bid=0.3, ip='123.123.123.123')
    #     server = Server(name='bar', provider='ec2', internal_address='${foo.security_group}',
    #                              size='${foo.ip} ${foo.bid} def', security_group='${foo.internal_address}')
    #     self.assertTrue(server.resolve(thing, 'internal_address', '${foo.security_group}'))
    #     self.assertEquals(server.internal_address, 'asdf')
    #     self.assertTrue(server.resolve(thing, 'size', '${foo.bid}'))
    #     self.assertEquals(server.size, '${foo.ip} 0.3 def')
    #     self.assertTrue(server.resolve(thing, 'size', '${foo.ip}'))
    #     self.assertEquals(server.size, '123.123.123.123 0.3 def')
    #     self.assertFalse(server.resolve(thing, 'security_group', '${foo.internal_address}'))
    #     self.assertEquals(server.security_group, '${foo.internal_address}')

    # def test_resolve_container(self):
    #     thing = Container(name='foo', host=None, image='image-foo', command='cmd')
    #     container = Container(name='bar', host=None,
    #                                    command='a ${foo.containers.c1.image} b',
    #                                    image='${foo.host} ${foo.image}',
    #                                    environment=[['${foo.containers.c1.image}', 'bar'],
    #                                                 ['foo', '${foo.containers.c1.command}']])
    #     self.assertTrue(container.resolve(thing, 'command', '${foo.containers.c1.image}'))
    #     self.assertEquals(container.command, 'a image-foo b')
    #     self.assertTrue(container.resolve(thing, 'environment:0:0', '${foo.containers.c1.image}'))
    #     self.assertEquals(container.environment, [
    #         ['image-foo', 'bar'], ['foo', '${foo.containers.c1.command}']])
    #     self.assertTrue(container.resolve(thing, 'environment:1:1', '${foo.containers.c1.command}'))
    #     self.assertEquals(container.environment, [['image-foo', 'bar'], ['foo', 'cmd']])
    #     self.assertFalse(container.resolve(thing, 'image', '${foo.host}'))
    #     self.assertEquals(container.image, '${foo.host} ${foo.image}')
    #     self.assertTrue(container.resolve(thing, 'image', '${foo.image}'))
    #     self.assertEquals(container.image, '${foo.host} image-foo')

    # def test_all_field_attrs(self):
    #     container = Container(name='c1',
    #                                    host=None,
    #                                    image='blah',
    #                                    ports=[[80, 80], [1000, 1001]],
    #                                    volumes=['vol1', 'vol2', 'vol3'],
    #                                    environment=[['foo', 'bar'], ['baz', 'qux']])
    #     expected = [
    #         ('foo', 'environment:0:0'),
    #         ('bar', 'environment:0:1'),
    #         ('baz', 'environment:1:0'),
    #         ('qux', 'environment:1:1'),
    #         ('blah', 'image'),
    #         ('vol1', 'volumes:0'),
    #         ('vol2', 'volumes:1'),
    #         ('vol3', 'volumes:2'),
    #         (80, 'ports:0:0'),
    #         (80, 'ports:0:1'),
    #         (1000, 'ports:1:0'),
    #         (1001, 'ports:1:1'),
    #     ]
    #     self.assertEquals(list(ensemble.all_field_attrs(container)), expected)

    # def test_get_servers_parameterised_json(self):
    #     foo = Server(name='foo-0', provider='ec2', size='m1.small')
    #     bar = Server(name='bar-0', provider='ec2', size='m1.small')

    #     foo.containers = {
    #         'baz-0': Container(
    #             name='baz-0',
    #             host=foo,
    #             environment=[['FOO', '${host.ip} ${bar.ip}']],
    #         )
    #     }

    #     servers = {'foo-0': foo, 'bar-0': bar}

    #     servers_json = ensemble.get_servers_parameterised_json(servers)
    #     server_dicts = json.loads(servers_json)

    #     expected_dicts = {
    #         'bar-0': {'active': '${bar-0.active}',
    #                   'bid': '${bar-0.bid}',
    #                   'image': '${bar-0.image}',
    #                   'internal_address': '${bar-0.internal_address}',
    #                   'ip': '${bar-0.ip}',
    #                   'name': '${bar-0.name}',
    #                   'placement': '${bar-0.placement}',
    #                   'provider': '${bar-0.provider}',
    #                   'security_group': '${bar-0.security_group}',
    #                   'size': '${bar-0.size}'},
    #         'foo-0': {'active': '${foo-0.active}',
    #                   'bid': '${foo-0.bid}',
    #                   'image': '${foo-0.image}',
    #                   'internal_address': '${foo-0.internal_address}',
    #                   'ip': '${foo-0.ip}',
    #                   'name': '${foo-0.name}',
    #                   'placement': '${foo-0.placement}',
    #                   'provider': '${foo-0.provider}',
    #                   'security_group': '${foo-0.security_group}',
    #                   'size': '${foo-0.size}'}
    #     }
    #     self.assertEquals(server_dicts, expected_dicts)

class TestFieldList(unittest.TestCase):

    def test_indexed_items(self):
        pass

class TestDependencyGraph(unittest.TestCase):

    def test_remove(self):
        graph = DependencyGraph()
        graph.add('b', (1, 2), 'a')
        graph.add('b', (1, 3), 'a')
        self.assertEquals(graph.graph, {'a': {'b'}})
        self.assertEquals(graph.inverse_graph, {'b': {'a'}})
        graph.remove('b', (1, 2), 'a')
        self.assertEquals(graph.graph, {'a': {'b'}})
        self.assertEquals(graph.inverse_graph, {'b': {'a'}})
        graph.remove('b', (1, 3), 'a')
        self.assertEquals(graph.graph, {})
        self.assertEquals(graph.inverse_graph, {})

    def test_find_cycle_positive(self):
        graph = DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('b', None, 'a')
        self.assertIsNotNone(graph.find_cycle())

        graph = DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('b', None, 'c')
        graph.add('b', None, 'd')
        graph.add('c', None, 'a')
        self.assertIsNotNone(graph.find_cycle())

        graph = DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('a', None, 'c')
        graph.add('b', None, 'd')
        graph.add('c', None, 'd')
        graph.add('d', None, 'c')
        self.assertIsNotNone(graph.find_cycle())

    def test_find_cycle_negative(self):
        graph = DependencyGraph()
        graph.add('a', None, 'b')
        graph.add('a', None, 'c')
        graph.add('b', None, 'd')
        graph.add('c', None, 'd')
        graph.add('b', None, 'c')
        self.assertIsNone(graph.find_cycle())

    def test_get_free_nodes(self):
        graph = DependencyGraph()
        graph.add('e', None, 'f')
        graph.add('e', None, 'd')
        graph.add('e', None, 'c')
        graph.add('d', None, 'c')

        all_nodes = set('abcdef')

        self.assertEquals(graph.get_free_nodes(all_nodes), set(('a', 'b', 'c', 'f')))
        graph.remove('d', None, 'c')
        self.assertEquals(graph.get_free_nodes(all_nodes), set(('a', 'b', 'c', 'd', 'f')))
        graph.remove('e', None, 'c')
        self.assertEquals(graph.get_free_nodes(all_nodes), set(('a', 'b', 'c', 'd', 'f')))
        graph.remove('e', None, 'd')
        self.assertEquals(graph.get_free_nodes(all_nodes), set(('a', 'b', 'c', 'd', 'f')))
        graph.remove('e', None, 'f')
        self.assertEquals(graph.get_free_nodes(all_nodes), set(('a', 'b', 'c', 'd', 'e', 'f')))

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
        parse.expand_template(config, templates)
        self.assertEquals(config, expected)

    def test_overwrite(self):
        config = {
            'foo': 'bar',
            'baz': 'qux',
            'template': 'a',
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
        parse.expand_template(config, templates)
        self.assertEquals(config, expected)

    def test_missing_template(self):
        config = {
            'foo': 'bar',
            'baz': 'qux',
            'template': 'a',
        }
        templates = {
            'b': {
                'foo': '789',
            },
        }
        self.assertRaises(ConfigException, parse.expand_template,
                          config, templates)

class TestProcessDependencies(unittest.TestCase):

    def setUp(self):
        self.mox = mox.Mox()

    def tearDown(self):
        self.mox.UnsetStubs()

    def test_new_servers(self):
        foo = Server(name='foo-0', provider='ec2', size='m1.small')
        servers = {'foo-0': foo}
        existing_servers = {}

        expected_graph = {}
        expected_changes = {'new_servers': {foo}}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_new_containers(self):
        foo = Server(name='foo-0', provider='ec2', size='m1.small')

        foo.containers = {
            'baz-0': Container(
                name='baz-0',
                host=foo,
            )
        }

        servers = {'foo-0': foo}

        existing_servers = {}

        expected_graph = {('SERVER', 'foo-0'): {('CONTAINER', 'foo-0', 'baz-0')}}
        expected_changes = {'new_servers': {foo}, 'new_containers': set(foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_changing_servers(self):
        foo = Server(name='foo-0', provider='ec2', size='m1.small')
        foo.containers = {
            'baz-0': Container(
                name='baz-0',
                host=foo,
            )
        }
        servers = {'foo-0': foo}

        e_foo = Server(name='foo-0', provider='ec2', size='m1.large', running=True)
        e_foo.containers = {
            'baz-0': Container(
                name='baz-0',
                host=e_foo,
            )
        }

        existing_servers = {'foo-0': e_foo}

        expected_graph = {
            ('SERVER', 'foo-0'): {('CONTAINER', 'foo-0', 'baz-0')}
        }
        expected_changes = {'changing_servers': {foo}, 'changing_containers': set(foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_changing_containers(self):
        foo = Server(name='foo-0', provider='ec2', size='m1.small')
        foo.containers = {
            'baz-0': Container(
                name='baz-0',
                host=foo,
                command='/bin/qux'
            )
        }
        servers = {'foo-0': foo}

        e_foo = Server(name='foo-0', provider='ec2', size='m1.small', running=True)
        e_foo.containers = {
            'baz-0': Container(
                name='baz-0',
                host=e_foo,
                command='/bin/bar',
                running=True,
            )
        }

        existing_servers = {'foo-0': e_foo}

        expected_graph = {}
        expected_changes = {'changing_containers': set(foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_absent_containers(self):
        foo = Server(name='foo-0', provider='ec2', size='m1.small')
        servers = {'foo-0': foo}

        e_foo = Server(name='foo-0', provider='ec2', size='m1.small', running=True)
        e_foo.containers = {
            'baz-0': Container(
                name='baz-0',
                host=e_foo,
                running=True,
            )
        }

        existing_servers = {'foo-0': e_foo}

        expected_graph = {}
        expected_changes = {'absent_containers': set(e_foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_existing_parameterised_container(self):
        foo = Server(name='foo-0', provider='ec2', size='m1.small')
        bar = Server(name='bar-0', provider='ec2', size='m1.small')

        foo.containers = {
            'baz-0': Container(
                name='baz-0',
                image='foo/bar',
                host=foo,
                environment={'FOO': '${host.ip} ${bar.ip}'},
            )
        }

        servers = {'foo-0': foo, 'bar-0': bar}

        e_foo = Server(name='foo-0', provider='ec2', size='m1.small', ip='1.2.3.4', running=True)
        e_bar = Server(name='bar-0', provider='ec2', size='m1.small', ip='5.4.3.2', running=True)

        e_foo.containers = {
            'baz-0': Container(
                name='baz-0',
                image='foo/bar',
                host=e_foo,
                environment={'FOO': '1.2.3.4 5.4.3.2'},
                running=True,
            )
        }

        existing_servers = {'foo-0': e_foo, 'bar-0': e_bar}

        expected_graph = {}
        expected_changes = {}

        self.mox.StubOutWithMock(docker, 'get_registry_image_id')
        self.mox.StubOutWithMock(docker, 'get_image_id')
        docker.get_registry_image_id('foo/bar').AndReturn('1234')
        docker.get_image_id('baz-0').AndReturn('1234')

        self.mox.ReplayAll()

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

        self.mox.VerifyAll()

class TestParseServer(unittest.TestCase):

    def test_fields(self):
        config = {
            'provider': 'ec2',
            'size': 'm1.large',
            'image': 'foobar',
            'placement': 'us-east-1b',
            'bid': 0.2,
            'security_group': 'foo'
        }
        expected = {
            'serv-0': Server(
                name='serv-0',
                provider='ec2',
                size='m1.large',
                image='foobar',
                placement='us-east-1b',
                bid=0.2,
                security_group='foo',
            )
        }
        self.assertEquals(parse.parse_server('serv', config, {}), expected)

class TestParseContainer(unittest.TestCase):

    def test_fields(self):
        server = Server('s1')
        config = {
            'image': 'foo',
            'command': 'bar',
            'environment': {
                'FOO': 123,
                'BAR': 'BAZ',
            },
            'ports': [
                80,
                '100:200',
            ],
            'volumes': {
                '/tmp': '/var/lib',
            },
            'ip': '172.20.0.2',
        }
        expected = {
            'cont-0': Container(
                name='cont-0',
                host=server,
                image='foo',
                command='bar',
                environment={
                    'FOO': 123,
                    'BAR': 'BAZ'
                },
                ports=[
                    [80, 80, 'tcp'],
                    [100, 200, 'tcp']
                ],
                volumes={
                    '/tmp': '/var/lib'
                },
                ip='172.20.0.2'
            )
        }

        actual = parse.parse_container('cont', config, server, {})

        self.assertEquals(actual, expected)

class TestMultiprocess(unittest.TestCase):

    def test_servers(self):
        graph = DependencyGraph()
        servers = {'s%d' % i: DummyServer('s%d' % i) for i in range(3)}
        create.create_things(servers, graph, set(), set(), set())

    def test_child_container(self):
        s1 = DummyServer('s1')
        c1 = DummyContainer('c1', s1)
        s1.containers = {'c1': c1}
        servers = {'s1': s1}
        graph = DependencyGraph()
        graph.add(('s1', 'c1'), None, ('s1', None))

        create.create_things(servers, graph, set(), set(), set())

    def test_multiple_dependencies(self):
        pass

    def test_resolve(self):
        pass

class DummyServer(Server):

    def create(self):
        time.sleep(0.01)
        return [self]

    def is_active(self):
        return True

class DummyContainer(Container):

    def create(self):
        time.sleep(0.01)
        return [self]

    def is_active(self):
        return True

class TestParseRealConfigs(unittest.TestCase):

    def test_case_1(self):
        config_yaml = '''
foo:
  provider: ec2
  size: m1.medium
  image: ubuntu 12.04
  containers:
    foo:
      image: quay.io/example/foo
      ports:
        - 8080
    baz:
      image: quay.io/example/baz
      environment:
        BAR_HOST: ${bar.ip}
        HOSTNAME: ${host.name}-${host.ip}
  count: 2

bar:
  provider: ec2
  size: m1.small
  image: ubuntu 12.04
  containers:
    qux:
      image: quay.io/example/qux
      ports:
        - 80
        - 1001
'''

        foo0 = Server(
                name='foo-0',
                provider='ec2',
                size='m1.medium',
                image='ami-ad3660c4')
        foo0.containers={
            'foo-0': Container(
                name='foo-0',
                host=foo0,
                image='quay.io/example/foo',
                ports=[[8080, 8080, 'tcp']],
            ),
            'baz-0': Container(
                name='baz-0',
                host=foo0,
                image='quay.io/example/baz',
                environment={
                    'BAR_HOST': '${bar.ip}',
                    'HOSTNAME': '${host.name}-${host.ip}',
                },
            )
        }

        foo1 = Server(
                name='foo-1',
                provider='ec2',
                size='m1.medium',
                image='ami-ad3660c4')
        foo1.containers={
            'foo-0': Container(
                name='foo-0',
                host=foo1,
                image='quay.io/example/foo',
                ports=[[8080, 8080, 'tcp']],
            ),
            'baz-0': Container(
                name='baz-0',
                host=foo1,
                image='quay.io/example/baz',
                environment={
                    'BAR_HOST': '${bar.ip}',
                    'HOSTNAME': '${host.name}-${host.ip}',
                },
            )
        }

        bar0 = Server(
            name='bar-0',
            provider='ec2',
            image='ami-ad3660c4',
            size='m1.small')
        bar0.containers={
            'qux-0': Container(
                name='qux-0',
                host=bar0,
                image='quay.io/example/qux',
                ports=[
                    [80, 80, 'tcp'],
                    [1001, 1001, 'tcp']
                ])
        }

        expected_servers = {
            'foo-0': foo0,
            'foo-1': foo1,
            'bar-0': bar0,
        }

        config = yaml.load(config_yaml)
        servers = parse.parse_config(config)

        self.assertEquals(servers, expected_servers)

        expected_graph = {
            ('SERVER', 'foo-0'): set([
                ('CONTAINER', 'foo-0', 'foo-0'),
                ('CONTAINER', 'foo-0', 'baz-0'),
            ]),
            ('SERVER', 'foo-1'): set([
                ('CONTAINER', 'foo-1', 'foo-0'),
                ('CONTAINER', 'foo-1', 'baz-0'),
            ]),
            ('SERVER', 'bar-0'): set([
                ('CONTAINER', 'foo-0', 'baz-0'), 
                ('CONTAINER', 'foo-1', 'baz-0'),
                ('CONTAINER', 'bar-0', 'qux-0')
            ]),
        }

        expected_changes = {
            'new_servers': {servers['foo-0'], servers['foo-1'], servers['bar-0']},
            'new_containers': (set(servers['foo-0'].containers.values()) |
                               set(servers['foo-1'].containers.values()) |
                               set(servers['bar-0'].containers.values())),
        }

        dependency_graph, changes = dependency.process_dependencies(servers, {})

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

        self.assertEquals(servers['foo-0'].containers['baz-0'].fields['environment']['BAR_HOST'], '${bar.ip}')
        self.assertEquals(servers['foo-0'].containers['baz-0'].fields['environment']['HOSTNAME'], 'foo-0-${host.ip}')
        self.assertEquals(servers['foo-1'].containers['baz-0'].fields['environment']['BAR_HOST'], '${bar.ip}')
        self.assertEquals(servers['foo-1'].containers['baz-0'].fields['environment']['HOSTNAME'], 'foo-1-${host.ip}')

        bar0 = servers['bar-0']
        bar0.fields['ip'] = '1.2.3.4'

        thing_index = thingindex.build_thing_index(servers)

        dependents = dependency_graph.get_dependents(bar0.thing_name())
        for thing_name, pointers in dependents.items():
            dependent = thing_index[thing_name]
            for pointer in pointers:
                pointer.resolve(dependent, bar0)

        self.assertEquals(servers['foo-0'].containers['baz-0'].fields['environment']['BAR_HOST'], '1.2.3.4')
        self.assertEquals(servers['foo-1'].containers['baz-0'].fields['environment']['BAR_HOST'], '1.2.3.4')
        self.assertEquals(servers['foo-0'].containers['baz-0'].fields['environment']['HOSTNAME'], 'foo-0-${host.ip}')


def changes_equals(changes, expected_changes):
    if sorted(changes.keys()) != sorted(expected_changes.keys()):
        return False

    for k, v in changes.items():
        key = lambda x: x.thing_name()
        if sorted(v, key=key) != sorted(expected_changes[k], key=key):
            return False

    return True
