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
        foo = Server(name='foo', provider='ec2', size='m1.small')
        servers = {'foo': foo}
        existing_servers = {}

        expected_graph = {}
        expected_changes = {'new_servers': {foo}}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_new_containers(self):
        foo = Server(name='foo', provider='ec2', size='m1.small')

        foo.containers = {
            'baz': Container(
                name='baz',
                host=foo,
            )
        }

        servers = {'foo': foo}

        existing_servers = {}

        expected_graph = {('SERVER', 'foo'): {('CONTAINER', 'foo', 'baz')}}
        expected_changes = {'new_servers': {foo}, 'new_containers': set(foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_changing_servers(self):
        foo = Server(name='foo', provider='ec2', size='m1.small')
        foo.containers = {
            'baz': Container(
                name='baz',
                host=foo,
            )
        }
        servers = {'foo': foo}

        e_foo = Server(name='foo', provider='ec2', size='m3.large', running=True)
        e_foo.containers = {
            'baz': Container(
                name='baz',
                host=e_foo,
            )
        }

        existing_servers = {'foo': e_foo}

        expected_graph = {
            ('SERVER', 'foo'): {('CONTAINER', 'foo', 'baz')}
        }
        expected_changes = {'changing_servers': {foo}, 'changing_containers': set(foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_changing_containers(self):
        foo = Server(name='foo', provider='ec2', size='m1.small')
        foo.containers = {
            'baz': Container(
                name='baz',
                host=foo,
                command='/bin/qux'
            )
        }
        servers = {'foo': foo}

        e_foo = Server(name='foo', provider='ec2', size='m1.small', running=True)
        e_foo.containers = {
            'baz': Container(
                name='baz',
                host=e_foo,
                command='/bin/bar',
                running=True,
            )
        }

        existing_servers = {'foo': e_foo}

        expected_graph = {}
        expected_changes = {'changing_containers': set(foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_absent_containers(self):
        foo = Server(name='foo', provider='ec2', size='m1.small')
        servers = {'foo': foo}

        e_foo = Server(name='foo', provider='ec2', size='m1.small', running=True)
        e_foo.containers = {
            'baz': Container(
                name='baz',
                host=e_foo,
                running=True,
            )
        }

        existing_servers = {'foo': e_foo}

        expected_graph = {}
        expected_changes = {'absent_containers': set(e_foo.containers.values())}

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

    def test_existing_parameterised_container(self):
        foo = Server(name='foo', provider='ec2', size='m1.small')
        bar = Server(name='bar', provider='ec2', size='m1.small')

        foo.containers = {
            'baz': Container(
                name='baz',
                image='foo/bar',
                host=foo,
                environment={'FOO': '${host.ip} ${bar.ip}'},
            )
        }

        servers = {'foo': foo, 'bar': bar}

        e_foo = Server(name='foo', provider='ec2', size='m1.small', ip='1.2.3.4', running=True)
        e_bar = Server(name='bar', provider='ec2', size='m1.small', ip='5.4.3.2', running=True)

        e_foo.containers = {
            'baz': Container(
                name='baz',
                image='foo/bar',
                host=e_foo,
                environment={'FOO': '1.2.3.4 5.4.3.2'},
                running=True,
            )
        }

        existing_servers = {'foo': e_foo, 'bar': e_bar}

        expected_graph = {}
        expected_changes = {}

        self.mox.StubOutWithMock(docker, 'get_registry_image_id')
        self.mox.StubOutWithMock(docker, 'get_image_id')
        docker.get_registry_image_id('foo/bar').AndReturn('1234')
        docker.get_image_id('baz').AndReturn('1234')

        self.mox.ReplayAll()

        dependency_graph, changes = dependency.process_dependencies(servers, existing_servers)

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

class TestParseServer(unittest.TestCase):

    def test_fields(self):
        config = {
            'provider': 'ec2',
            'size': 'm3.large',
            'image': 'foobar',
            'placement': 'us-east-1b',
            'bid': 0.2,
            'security_group': 'foo'
        }
        expected = {
            'serv': Server(
                name='serv',
                provider='ec2',
                size='m3.large',
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
            'cont': Container(
                name='cont',
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
  size: m3.medium
  image: ubuntu 14.04
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
  image: ubuntu 14.04
  containers:
    qux:
      image: quay.io/example/qux
      ports:
        - 80
        - 1001
'''

        foo0 = Server(
                name='foo',
                provider='ec2',
                size='m3.medium',
                image='ami-1e917676')
        foo0.containers={
            'foo': Container(
                name='foo',
                host=foo0,
                image='quay.io/example/foo',
                ports=[[8080, 8080, 'tcp']],
            ),
            'baz': Container(
                name='baz',
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
                size='m3.medium',
                image='ami-1e917676')
        foo1.containers={
            'foo': Container(
                name='foo',
                host=foo1,
                image='quay.io/example/foo',
                ports=[[8080, 8080, 'tcp']],
            ),
            'baz': Container(
                name='baz',
                host=foo1,
                image='quay.io/example/baz',
                environment={
                    'BAR_HOST': '${bar.ip}',
                    'HOSTNAME': '${host.name}-${host.ip}',
                },
            )
        }

        bar0 = Server(
            name='bar',
            provider='ec2',
            image='ami-1e917676',
            size='m1.small')
        bar0.containers={
            'qux': Container(
                name='qux',
                host=bar0,
                image='quay.io/example/qux',
                ports=[
                    [80, 80, 'tcp'],
                    [1001, 1001, 'tcp']
                ])
        }

        expected_servers = {
            'foo': foo0,
            'foo-1': foo1,
            'bar': bar0,
        }

        config = yaml.load(config_yaml)
        servers = parse.parse_config(config)

        self.assertEquals(servers, expected_servers)

        expected_graph = {
            ('SERVER', 'foo'): set([
                ('CONTAINER', 'foo', 'foo'),
                ('CONTAINER', 'foo', 'baz'),
            ]),
            ('SERVER', 'foo-1'): set([
                ('CONTAINER', 'foo-1', 'foo'),
                ('CONTAINER', 'foo-1', 'baz'),
            ]),
            ('SERVER', 'bar'): set([
                ('CONTAINER', 'foo', 'baz'), 
                ('CONTAINER', 'foo-1', 'baz'),
                ('CONTAINER', 'bar', 'qux')
            ]),
        }

        expected_changes = {
            'new_servers': {servers['foo'], servers['foo-1'], servers['bar']},
            'new_containers': (set(servers['foo'].containers.values()) |
                               set(servers['foo-1'].containers.values()) |
                               set(servers['bar'].containers.values())),
        }

        dependency_graph, changes = dependency.process_dependencies(servers, {})

        self.assertEquals(dependency_graph.graph, expected_graph)
        self.assertTrue(changes_equals(changes, expected_changes))

        self.assertEquals(servers['foo'].containers['baz'].fields['environment']['BAR_HOST'], '${bar.ip}')
        self.assertEquals(servers['foo'].containers['baz'].fields['environment']['HOSTNAME'], 'foo-${host.ip}')
        self.assertEquals(servers['foo-1'].containers['baz'].fields['environment']['BAR_HOST'], '${bar.ip}')
        self.assertEquals(servers['foo-1'].containers['baz'].fields['environment']['HOSTNAME'], 'foo-1-${host.ip}')

        bar0 = servers['bar']
        bar0.fields['ip'] = '1.2.3.4'

        thing_index = thingindex.build_thing_index(servers)

        dependents = dependency_graph.get_dependents(bar0.thing_name())
        for thing_name, pointers in dependents.items():
            dependent = thing_index[thing_name]
            for pointer in pointers:
                pointer.resolve(dependent, bar0)

        self.assertEquals(servers['foo'].containers['baz'].fields['environment']['BAR_HOST'], '1.2.3.4')
        self.assertEquals(servers['foo-1'].containers['baz'].fields['environment']['BAR_HOST'], '1.2.3.4')
        self.assertEquals(servers['foo'].containers['baz'].fields['environment']['HOSTNAME'], 'foo-${host.ip}')


def changes_equals(changes, expected_changes):
    if sorted(changes.keys()) != sorted(expected_changes.keys()):
        return False

    for k, v in changes.items():
        key = lambda x: x.thing_name()
        if sorted(v, key=key) != sorted(expected_changes[k], key=key):
            return False

    return True
