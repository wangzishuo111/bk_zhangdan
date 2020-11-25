#!/usr/bin/python
# -*- coding: utf-8 -*-

from base import util

def gen(batch):
    md5 = util.get_md5(batch)
    md5_int = int(md5, 16)
    magic_num = md5_int % 10
    row_key = '%d-%s' % (magic_num, batch)
    return row_key

def test_():
    print gen('test1')
    print gen('test2')

def main():
    print gen('test1')
    print gen('test2')

if __name__ == '__main__':
    main()
