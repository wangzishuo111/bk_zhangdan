#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kv_store import read
from kv_store import write

from nose.tools import assert_equal, assert_true, assert_is_none

def setUp():
    import hbase_util
    hbase_util.switch2local()
    import hbase_util_local
    hbase_util_local.clear()
    from base import opt
    opt.set_opt_local()

def test():
    assert_is_none(read('test_key1'))

    assert_true(write('test_key1', 'test hbase'))

    assert_equal('test hbase', read('test_key1'))

def main():
    setUp()
    print 'KV get: ', read('test_key1')
    print 'KV set: ', write('test_key1', 'test hbase')
    print 'KV get: ', read('test_key1')

if __name__ == '__main__':
    main()
