import os
import re
import math
import shutil
import cPickle
import uuid
import contextlib
from functools import wraps
import inspect
from fabric.api import *

def print_table(table, columns=None):
    def is_number(x):
        try:
            float(re.sub('[kMGTP]B$', '', x))
            return True
        except ValueError:
            return False

    if columns is None:
        columns = table[0].keys()

    lengths = {k: len(k) for k in columns}
    aligns = {k: '' for k in columns}
    clean_table = []

    for row in table:
        clean_row = {}
        for column in columns:
            value = str(row[column])
            if len(value) > lengths[column]:
                lengths[column] = len(value)
            if aligns[column] == '' and not is_number(value):
                aligns[column] = '-'
            clean_row[column] = value
        clean_table.append(clean_row)

    header_format_parts = []
    for column in columns:
        header_format_parts.append('%%(%s)-%ds' % (column, lengths[column]))
    header_format_string = '  '.join(header_format_parts)

    format_parts = []
    for column in columns:
        format_parts.append('%%(%s)%s%ds' % (column, aligns[column], lengths[column]))
    format_string = '  '.join(format_parts)

    print(header_format_string % {k: k for k in columns})
    
    for row in clean_table:
        print format_string % row

def cached(func):

    @wraps(func)
    def wrapper(*args, **kwargs):
        recache = kwargs.get('_recache')
        if recache:
            del kwargs['_recache']
        uncache = kwargs.get('_uncache')
        if uncache:
            del kwargs['_uncache']

        cache_key = func.__module__ + '.' + func.__name__
        if args or kwargs:
            cache_key += cPickle.dumps((args, kwargs))

        if uncache:
            cache().delete(cache_key)

        if recache:
            ret = func(*args, **kwargs)
            cache().set(cache_key, ret)
        else:
            ret = cache().get(cache_key)
            if ret is None:
                ret = func(*args, **kwargs)
                cache().set(cache_key, ret)
        return ret
    wrapper._cached = True
    return wrapper

def recache(fn, *args, **kwargs):
    if not hasattr(fn, '__call__'):
        raise Exception('%s is not a function' % str(fn))
    if not hasattr(fn, '_cached'):
        raise Exception('Function is not decorated with @cached')
    kwargs['_recache'] = True
    return fn(*args, **kwargs)

def uncache(fn, *args, **kwargs):
    if not hasattr(fn, '__call__'):
        raise Exception('%s is not a function' % str(fn))
    if not hasattr(fn, '_cached'):
        raise Exception('Function is not decorated with @cached')
    kwargs['_uncache'] = True
    return fn(*args, **kwargs)

class NoneCache(object):
    def __init__(self):
        pass
    def get(self, key):
        return None
    def set(self, key, value):
        return None
    def delete(self, key):
        return None
    def flush(self):
        return None

class FSCache(object):
    def __init__(self):
        self.client = pyfscache.FSCache('%s/cache' % hitc_home(), days=7)
    def get(self, key):
        try:
            return self.client[key]
        except KeyError:
            return None
    def set(self, key, value):
        try:
            self.client.update_item(key, value)
        except pyfscache.fscache.CacheError:
            self.client[key] = value
    def delete(self, key):
        try:
            self.client.expire(key)
        except Exception:
            pass
    def flush(self):
        try:
            shutil.rmtree('%s/cache' % hitc_home())
        except OSError, e:
            if e.errno != 2:
                raise

try:
    import pyfscache
    Cache = FSCache
except ImportError:
    Cache = NoneCache

def cache():
    if not hasattr(cache, 'client'):
        cache.client = Cache()
    return cache.client

def hitc_home():
    cb_home = os.path.expanduser('~/.hitc')
    if not os.path.exists(cb_home):
        os.mkdir(cb_home)
    return cb_home

def _role_match(role, name):
    if role is None:
        return True

    regex = '^%s-[0-9]+$' % role
    return bool(re.match(regex, name))

def filter_role(role, nodes):
    return [x for x in nodes if _role_match(role, x['name'])]

def env_var(var):
    value = os.environ.get(var)
    if not value:
        raise Exception('Missing required environment variable: %s' % var)
    return value

def average(x):
    return sum(x) * 1.0 / len(x)

def variance(x):
    avg = average(x)
    return [(s - avg) ** 2 for s in x]

def stddev(x):
    return math.sqrt(average(variance(x)))

def median(x):
    return sorted(x)[len(x) // 2]

@contextlib.contextmanager
def temp_dir():
    tmp_dir = '/tmp/' + str(uuid.uuid4())
    run('mkdir %s || true' % tmp_dir)
    try:
        yield tmp_dir
    finally:
        run('rm -rf %s' % tmp_dir)

def autodoc(func):

    argspec = inspect.getargspec(func)
    args = argspec.args
    defaults = argspec.defaults
    if defaults is None:
        defaults = []

    without_defaults = args[:len(args) - len(defaults)]
    with_defaults = ['%s=%s' % (k, v) for k, v in zip(args[len(args) - len(defaults):], defaults)]
    arg_string = ','.join(without_defaults + with_defaults)
    if arg_string:
        arg_string = ':' + arg_string

    if not func.__doc__:
        func.__doc__ = arg_string
    else:
        func.__doc__ = arg_string + '\n' + func.__doc__

    @wraps(func)
    def wrapped(*args, **kwargs):
        func(*args, **kwargs)

    return wrapped
    
if not hasattr(env, 'name_prefix'):
    env.name_prefix = 'HITC-'
