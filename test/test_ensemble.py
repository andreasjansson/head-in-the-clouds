import unittest2 as unittest

from headintheclouds import ensemble

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
        thing = ensemble.Server('foo', 'ec2', 'm1.small', 0.3, '123.123.123.123')
        self.assertEquals(ensemble.resolve('${host.ip}', thing, 0), '123.123.123.123')
        self.assertEquals(ensemble.resolve('$foo${host.type}', thing, 1), '$foom1.small')
        self.assertEquals(ensemble.resolve('${host.provider} $foo', thing, 0), 'ec2 $foo')
        self.assertEquals(ensemble.resolve('${host.bid}', thing, 0), '0.3')

    def test_resolve_server(self):
        thing = ensemble.Server('foo', 'ec2', 'm1.small', 0.3, '123.123.123.123')
        server = ensemble.Server('bar', '${foo.provider}', '${foo.ip} ${foo.bid} def')
        server.resolve(thing, 'provider', 0)
        self.assertEquals(server.provider, 'ec2')
        server.resolve(thing, 'type', 1)
        self.assertEquals(server.type, '${foo.ip} 0.3 def')
        server.resolve(thing, 'type', 0)
        self.assertEquals(server.type, '123.123.123.123 0.3 def')

    def test_resolve_container(self):
        thing = ensemble.Container('foo', None, 'image-foo', 'cmd')
        container = ensemble.Container('bar', None, command='a ${foo.host.containers.c1.image} b',
                                       environment=[('${foo.host.containers.c1.image}', 'bar'),
                                                    ('foo', '${foo.host.containers.c1.cmd}')])
        container.resolve(thing, 'command', 0)
        self.assertEquals(thing.command, 'a image-foo b')
        container.resolve(thing, 'env-key:0', 0)
        self.assertEquals(thing.environment, [
            ('image-foo', 'bar'), ('foo', '${foo.host.containers.c1.cmd}')])
        container.resolve(thing, 'env-value:0', 0)
        self.assertEquals(thing.environment, [('image-foo', 'bar'), ('foo', 'cmd')])

    def test_resolve_container(self):
        server = ensemble.Server('foo', 'ec2', 'm1.small', 0.3, '123.123.123.123')
        self.assertEquals(ensemble.resolve('${host.ip}', server, 0), '123.123.123.123')
        self.assertEquals(ensemble.resolve('$foo${host.type}', server, 1), '$foom1.small')
        self.assertEquals(ensemble.resolve('${host.provider} $foo', server, 0), 'ec2 $foo')
        self.assertEquals(ensemble.resolve('${host.bid}', server, 0), '0.3')

    def test_resolve_existing(self):
        existing_servers = {
            's1': ensemble.Server(name='s1', type='blah'),
            's2': ensemble.Server(name='s2', provider='foo'),
        }
        graph = ensemble.DependencyGraph()
#        graph.add('

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
