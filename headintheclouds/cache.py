import os
import errno
from headintheclouds.dependencies import PyDbLite
import cPickle
import time
from functools import wraps

FILENAME = '~/.hitc/cache.pdl'

NO_CACHE = os.environ.get('HITC_NO_CACHE', None) == 'true'

_cursor = None

python_set = set

def get(key):
    if NO_CACHE:
        return None

    records = _db()._key[key]

    if records:
        record = records[0]
        value = record['value']
        expire = record['expire']
        if not expire or expire > time.time():
            return cPickle.loads(str(value))
        delete(key)
    return None

def set(key, value, ttl=None):
    if NO_CACHE:
        return None

    if ttl is None:
        expire = 0
    else:
        expire = time.time() + ttl

    records = _db()._key[key]
    _db().delete(records)
    _db().insert(key=key, value=cPickle.dumps(value), expire=expire)
    _db().commit()

def delete(key):
    if NO_CACHE:
        return None

    records = _db()._key[key]
    if not records:
        return None
    _db().delete(records)
    _db().commit()

def flush():
    global _cursor

    try:
        os.unlink(_filename())
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise
    _cursor = None

def size():
    if NO_CACHE:
        return None

    return len(_db().records)

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
            delete(cache_key)

        if recache:
            ret = func(*args, **kwargs)
            set(cache_key, ret)
        else:
            ret = get(cache_key)
            if ret is None:
                ret = func(*args, **kwargs)
                set(cache_key, ret)
        return ret
    wrapper._cached = True
    return wrapper

def recache(fn, *args, **kwargs):
    if NO_CACHE:
        return fn(*args, **kwargs)

    if not hasattr(fn, '__call__'):
        raise Exception('%s is not a function' % str(fn))
    if not hasattr(fn, '_cached'):
        raise Exception('Function is not decorated with @cached')
    kwargs['_recache'] = True
    return fn(*args, **kwargs)

def uncache(fn, *args, **kwargs):
    if NO_CACHE:
        return None

    if not hasattr(fn, '__call__'):
        raise Exception('%s is not a function' % str(fn))
    if not hasattr(fn, '_cached'):
        raise Exception('Function is not decorated with @cached')
    kwargs['_uncache'] = True
    fn(*args, **kwargs)

def _db():
    global _cursor
    if _cursor is None:
        try:
            os.makedirs(os.path.dirname(_filename()))
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
        _cursor = PyDbLite.Base(_filename())
        try:
            _cursor.open()
        except IOError, e:
            if e.errno != errno.ENOENT:
                raise
            _cursor.create('key', 'value', 'expire')
            _cursor.create_index('key')
            
    return _cursor

def _filename():
    return os.path.abspath(os.path.expanduser(FILENAME))
