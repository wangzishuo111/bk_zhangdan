#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.log import logger
import base.opt as opt
from base.timer import Timer

import hbase_util_thrift as hbase_util
import kds_rowkey
import params

def get_table_name():
    env = opt.option().env
    return 'kds_data_' + env

def kds_save(batch, data):
    rowkey = kds_rowkey.gen(batch)
    table = get_table_name()
    cols = []
    cols.append(('kds', 'data', data))
    logger().info('save data for row[%s], table[%s]', rowkey, table)
    if not hbase_util.put_cols(table, rowkey, cols):
        return False
    return True

def kds_get(batch):
    rowkey = kds_rowkey.gen(batch)
    table = get_table_name()
    ret = hbase_util.get_col(table, rowkey, 'kds', 'data')
    logger().info('seek data for row[%s]', rowkey)
    if ret:
        return ret['kds:data']
    return None

def kds_del(batch):
    rowkey = kds_rowkey.gen(batch)
    logger().info('delete rowkey[%s]', rowkey)
    table = get_table_name()
    return hbase_util.delete(table, rowkey)

def kds_test():
    print 'save:', kds_save('test', '11')
    print 'get:', kds_get('test')
    print 'delete:', kds_del('test')
    print 'get:', kds_get('test')

def kds_main():
    kds_test()

if __name__ == '__main__':
    kds_main()
