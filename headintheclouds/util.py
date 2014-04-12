import os
import re
import math
import uuid
import contextlib
from functools import wraps
import inspect
from fabric.api import *

def print_table(table, columns=None, sort=None, default_sort=None):
    def is_number(x):
        try:
            float(re.sub('[kMGTP]B$', '', x))
            return True
        except ValueError:
            return False

    if columns is None:
        columns = table[0].keys()

    if sort:
        column_aliases = {}
        for column in columns:
            if isinstance(column, (tuple, list)):
                alias, column = column
            else:
                alias = column
            column_aliases[alias] = column
        def sort_key(x):
            if sort in column_aliases:
                column = column_aliases[sort]
            elif default_sort and default_sort in column_aliases:
                column = column_aliases[default_sort]
            else:
                column = columns[0]
            try:
                return x[column]
            except (KeyError, TypeError):
                return getattr(x, column, None)
        table.sort(key=sort_key)

    column_names = []
    for column in columns:
        if isinstance(column, (tuple, list)):
            column, _ = column
        column_names.append(column)

    lengths = {k: len(k) for k in column_names}
    aligns = {k: '' for k in column_names}

    clean_table = []
    for row in table:
        clean_row = {}
        for column in columns:
            if isinstance(column, (tuple, list)):
                column, prop = column
            else:
                prop = column

            try:
                value = row[prop]
            except (KeyError, TypeError):
                value = getattr(row, prop, '')

            value = str(value)
            if len(value) > lengths[column]:
                lengths[column] = len(value)
            if aligns[column] == '' and not is_number(value):
                aligns[column] = '-'
            clean_row[column] = value
        clean_table.append(clean_row)

    header_format_parts = []
    for column in column_names:
        header_format_parts.append('%%(%s)-%ds' % (column, lengths[column]))
    header_format_string = '  '.join(header_format_parts)

    format_parts = []
    for column in column_names:
        format_parts.append('%%(%s)%s%ds' % (column, aligns[column], lengths[column]))
    format_string = '  '.join(format_parts)

    print(header_format_string % {k: k for k in column_names})
    
    for row in clean_table:
        print format_string % row

def env_var(var, default=None):
    value = os.environ.get(var, default)
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

