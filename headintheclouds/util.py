import os
import re
import shutil
import cPickle
from fabric.api import *
import pyfscache

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

def cached(wrapped):
    
    def wrapper(*args, **kwargs):
        recache = kwargs.get('_recache')
        if recache:
            del kwargs['_recache']
        uncache = kwargs.get('_uncache')
        if uncache:
            del kwargs['_uncache']

        cache_key = wrapped.__module__ + '.' + wrapped.__name__
        if args or kwargs:
            cache_key += cPickle.dumps((args, kwargs))

        if uncache:
            cache().delete(cache_key)

        if recache:
            ret = wrapped(*args, **kwargs)
            cache().set(cache_key, ret)
        else:
            ret = cache().get(cache_key)
            if ret is None:
                ret = wrapped(*args, **kwargs)
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

class Cache():
    def __init__(self):
        self.client = pyfscache.FSCache('%s/cache' % cloudbuster_home(), days=7)
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
            shutil.rmtree('%s/cache' % cloudbuster_home())
        except OSError, e:
            if e.errno != 2:
                raise

def cache():
    if not hasattr(cache, 'client'):
        cache.client = Cache()
    return cache.client

def cloudbuster_home():
    cb_home = os.path.expanduser('~/.cloudbuster')
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

NAME_PREFIX = 'hitc-'
