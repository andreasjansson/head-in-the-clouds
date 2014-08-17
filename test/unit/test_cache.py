import unittest2 as unittest
import uuid
import time
import collections

from headintheclouds import cache

class TestCache(unittest.TestCase):

    def setUp(self):
        self.original_filename = cache.FILENAME
        cache.FILENAME = 'tmp_test_cache.db'

    def tearDown(self):
        cache.flush()
        cache.FILENAME = self.original_filename

    def test_non_existent(self):
        self.assertIsNone(cache.get('foo'))

    def test_set_get_string(self):
        key = randstr()
        value = randstr()
        cache.set(key, value)
        self.assertEquals(cache.get(key), value)

    def test_set_get_object(self):
        key = randstr()
        value = {randstr(): [randstr()]}
        cache.set(key, value)
        self.assertEquals(cache.get(key), value)

    def test_expire(self):
        key = randstr()
        value = randstr()
        ttl = 1
        cache.set(key, value, ttl)
        self.assertEquals(cache.size(), 1)
        self.assertEquals(cache.get(key), value)
        time.sleep(1.1)
        self.assertIsNone(cache.get(key))
        self.assertEquals(cache.size(), 0)

    def test_flush(self):
        for _ in range(5):
            cache.set(randstr(), randstr())
        self.assertEquals(cache.size(), 5)
        cache.flush()
        self.assertEquals(cache.size(), 0)

    def test_delete(self):
        key = randstr()
        value = randstr()
        cache.set(key, value)
        cache.delete(key)
        self.assertEquals(cache.get(key), None)
        self.assertEquals(cache.size(), 0)

    def test_cached(self):

        ret = [randstr(), randstr()]

        times_called = {'n': 0}

        @cache.cached
        def foo():
            times_called['n'] += 1
            return ret

        self.assertEquals(foo(), ret)
        self.assertEquals(foo(), ret)
        self.assertEquals(foo(), ret)
        self.assertEquals(times_called['n'], 1)

    def test_cached_args(self):
        ret = randstr()

        times_called = collections.Counter()

        @cache.cached
        def foo(x):
            times_called[x] += 1
            return (ret, x)

        self.assertEquals(foo(1), (ret, 1))
        self.assertEquals(foo(1), (ret, 1))
        self.assertEquals(foo('a'), (ret, 'a'))
        self.assertEquals(foo('a'), (ret, 'a'))
        self.assertEquals(times_called[1], 1)
        self.assertEquals(times_called['a'], 1)

    def test_cached_uncache(self):
        ret = [randstr(), randstr()]

        times_called = collections.Counter()

        @cache.cached
        def foo():
            times_called['n'] += 1
            return ret

        self.assertEquals(foo(), ret)
        self.assertEquals(foo(), ret)
        cache.uncache(foo)
        self.assertEquals(foo(), ret)
        self.assertEquals(times_called['n'], 2)

    def test_cached_recache(self):
        ret = [randstr(), randstr()]

        times_called = collections.Counter()

        @cache.cached
        def foo():
            times_called['n'] += 1
            return ret

        self.assertEquals(foo(), ret)
        self.assertEquals(foo(), ret)
        self.assertEquals(cache.recache(foo), ret)
        self.assertEquals(times_called['n'], 2)
        self.assertEquals(foo(), ret)
        self.assertEquals(times_called['n'], 2)

    def test_open_file(self):
        cache.set('foo', 'bar')
        self.assertEquals(cache.get('foo'), 'bar')
        cache._cursor = None
        self.assertEquals(cache.get('foo'), 'bar')

    def test_upsert(self):
        cache.set('foo', 'bar')
        self.assertEquals(cache.get('foo'), 'bar')
        self.assertEquals(cache.size(), 1)
        cache.set('foo', 'baz')
        self.assertEquals(cache.get('foo'), 'baz')
        self.assertEquals(cache.size(), 1)

def randstr():
    return str(uuid.uuid4())
