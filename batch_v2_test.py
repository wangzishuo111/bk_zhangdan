#!/usr/bin/python 
# -*- coding: utf-8 -*-

from batch_manager_v2 import clear_batch
from batch_manager_v2 import set_batch
from batch_manager_v2 import get_last_batch
from batch_manager_v2 import del_batch

from nose.tools import assert_equal, assert_true 

def setUp():
    import hbase_util
    hbase_util.switch2local()
    import hbase_util_local
    hbase_util_local.clear()
    from base import opt
    opt.set_opt_local()

def test():
    assert_true(clear_batch('test', '1', '2', '3', 'jpg'))

    assert_true(set_batch('test', '1', '2', '3', 'jpg', '!!!'))
    assert_equal('!!!', get_last_batch('test', '1', '2', '3', 'jpg'))

    assert_true(set_batch('test', '1', '2', '3', 'jpg', 'batch1'))
    assert_equal('batch1', get_last_batch('test', '1', '2', '3', 'jpg'))

    assert_true(set_batch('test', '1', '2', '3', 'jpg', 'batch3'))
    assert_equal('batch3', get_last_batch('test', '1', '2', '3', 'jpg'))

    assert_true(set_batch('test', '1', '2', '3', 'jpg', 'batch2'))
    assert_equal('batch3', get_last_batch('test', '1', '2', '3', 'jpg'))

    assert_true(del_batch('test', '1', '2', '3', 'jpg', 'batch3'))
    assert_equal('batch2', get_last_batch('test', '1', '2', '3', 'jpg'))

    assert_true(del_batch('test', '1', '2', '3', 'jpg', 'batch2'))
    assert_equal('batch1', get_last_batch('test', '1', '2', '3', 'jpg'))

    assert_true(del_batch('test', '1', '2', '3', 'jpg', 'batch1'))
    assert_equal('!!!', get_last_batch('test', '1', '2', '3', 'jpg'))

def main():
    setUp()


    print 'clear:', clear_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', '!!!')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set', set_batch('test', '1', '2', '3', 'jpg', 'batch1')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', 'batch3')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', 'batch2')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'del:', del_batch('test', '1', '2', '3', 'jpg', 'batch2')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'del:', del_batch('test', '1', '2', '3', 'jpg', 'batch3')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')



if __name__ == '__main__':
    main()

