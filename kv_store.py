#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hbase_util
import base.opt as opt
from config import GET_CONF
from base.singleton import singleton
from base import util
import params


@singleton
class KVStore(object):
    def __init__(self):
        env = opt.option().env
        self.table = GET_CONF('kv_store', env)

    def gen_rowkey(self, key):
        md5 = util.get_md5(key)
        md5_to_int = int(md5, 16)
        magic_num = md5_to_int % 100
        row_key = '%02d-%s' % (magic_num, key)
        return row_key

    def set(self, key, val):
        rowkey = self.gen_rowkey(key)
        ret = hbase_util.put_col(self.table, rowkey, 'data', 'data', val)
        return ret

    def get(self, key):
        rowkey = self.gen_rowkey(key)
        val = hbase_util.get_col(self.table, rowkey, 'data', 'data')
        return val

def write(key, value):
    kv = KVStore()
    return kv.set(key, value)

def read(key):
    kv = KVStore()
    ret = kv.get(key)
    if not ret:
        return None
    return ret.get('data:data')

def kv_store_main():
    print write('shen', 'test hbase')
    print read('shen')

if __name__ == '__main__':
    kv_store_main()
