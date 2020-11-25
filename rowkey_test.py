#!/usr/bin/python
# -*- coding: utf-8 -*-

from rowkey import gen, gen_v2
from nose.tools import assert_equal

def setUp():
    pass

def rowkey_test():
    assert_equal('0-test1-11_2_3_jpg', gen('test1', '11', '2', '3', 'jpg'))
    assert_equal('55-test2-11_2_3_jpg', gen_v2('test2', '11', '2', '3', 'jpg'))

def rowkey_main():
    print gen('test1', '11', '2', '3', 'jpg')
    print gen_v2('test2', '11', '2', '3', 'jpg')

if __name__ == '__main__':
    rowkey_main()

