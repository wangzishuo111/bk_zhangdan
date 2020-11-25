#!/usr/bin/python
# -*- coding: utf-8 -*-
from base import util

def gen(track_id, tpid, tpe, seq, image_type):
    md5 = util.get_md5(track_id)
    md5 = int(md5, 16)
    magic_num = md5 % 10
    row_key = '%d-%s-%s_%s_%s_%s' % \
            (magic_num, track_id, tpid, tpe, seq, image_type)
    return row_key

def gen_v2(track_id, tpid, tpe, seq, image_type):
    md5 = util.get_md5(track_id + '-' + tpid)
    md5 = int(md5, 16)
    magic_num = md5 % 100
    row_key = '%02d-%s-%s_%s_%s_%s' % \
            (magic_num, track_id, tpid, tpe, seq, image_type)
    return row_key

def compute_magic(track_id):
    md5 = util.get_md5(track_id)
    md5 = int(md5, 16)
    return md5 % 10

def compute_magic_v2(track_id, tpid):
    md5 = util.get_md5(track_id + '-' + tpid)
    md5 = int(md5, 16)
    return md5 % 100

def main():
    print gen('test1', '11', '2', '3', 'jpg')
    print gen('test2', '11', '2', '3', 'jpg')
    print gen('test3', '11', '2', '3', 'jpg')
    print gen('test4', '11', '2', '3', 'jpg')
    print gen('1000229_20180829150215363', '1000229_20180829150215587462',
                '09', '000', 'png')
    print gen_v2('1000229_20180829150215363', '1000229_20180829150215587462',
                '09', '000', 'png')


if __name__ == '__main__':
    main()
