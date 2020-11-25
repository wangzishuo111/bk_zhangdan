#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

from base.log import logger
from base.opt import FLAG
from config import GET_CONF

import hbase_util
import rowkey
import params

def get_table_name():
    env = FLAG().env
    return GET_CONF('meta_table', env)

def _batch_add(batch_list, new_batch):
    l = batch_list.strip().split(' ')
    if new_batch not in batch_list:
        l.append(new_batch)
        l.sort(reverse=True)
        new_batch_list = ' '.join(l)
    else:
        new_batch_list = batch_list
    return new_batch_list

def _get_last_batch(batch_list):
    batch_list = batch_list.strip()
    if not batch_list:
        return None
    return batch_list.split(' ')[0].strip()

def _batch_del(batch_list, batch):
    l = batch_list.strip().split(' ')
    try:
        l.remove(batch)
    except:
        pass
    new_batch_list = ' '.join(l)
    return new_batch_list

def get_last_batch(track_id, tpid, tpe, seq, image_type):
    batch_list = get_batch_list(track_id, tpid, tpe, seq, image_type)
    if batch_list:
        return _get_last_batch(batch_list)
    else:
        return None

def get_batch_list(track_id, tpid, tpe, seq, image_type):
    table = get_table_name()
    #row = '%s-%s_%s_%s_%s' % (track_id, tpid, tpe, seq, image_type)
    row = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    ret = hbase_util.get_col(table, row, 'meta', 'batch_list')
    if ret:
        assert ret.keys() == ['meta:batch_list']
        return ret['meta:batch_list']
    else:
        return None

def set_batch(track_id, tpid, tpe, seq, image_type, batch):
    batch_list = get_batch_list(track_id, tpid, tpe, seq, image_type)
    logger().debug('old batch_list:%s', batch_list)
    if batch_list:
        batch_list = _batch_add(batch_list, batch)
    else:
        batch_list = batch
    logger().debug('new batch_list:%s', batch_list)

    # write
    table = get_table_name()
    #row_key = '%s-%s_%s_%s_%s' % (track_id, tpid, tpe, seq, image_type)
    row = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    cols = []
    cols.append(('meta', 'batch_list', batch_list))
    return hbase_util.put_cols(table, row, cols)

def del_batch(track_id, tpid, tpe, seq, image_type, batch):
    batch_list = get_batch_list(track_id, tpid, tpe, seq, image_type)
    logger().debug('old batch_list:%s', batch_list)
    if batch_list:
        batch_list = _batch_del(batch_list, batch)
    else:
        batch_list = ''
    logger().debug('new batch_list:%s', batch_list)

    # write
    table = get_table_name()
    #row_key = '%s-%s_%s_%s_%s' % (track_id, tpid, tpe, seq, image_type)
    row = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    cols = []
    cols.append(('meta', 'batch_list', batch_list))
    return hbase_util.put_cols(table, row, cols)

def clear_batch(track_id, tpid, tpe, seq, image_type):
    table = get_table_name()
    #row_key = '%s-%s_%s_%s_%s' % (track_id, tpid, tpe, seq, image_type)
    row = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    cols = []
    cols.append(('meta', 'batch_list', ''))
    sys.stdout.flush()
    ret = hbase_util.put_cols(table, row, cols)

    sys.stdout.flush()
    return ret

def batch_manager_main():
    print 'clear:', clear_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', '!!!')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set', set_batch('test', '1', '2', '3', 'jpg', 'batch1')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', 'batch3')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', 'batch2')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'set:', set_batch('test', '1', '2', '3', 'jpg', 'batch2')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'del:', del_batch('test', '1', '2', '3', 'jpg', 'batch2')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

    print 'del:', del_batch('test', '1', '2', '3', 'jpg', 'batch3')
    print 'last:', get_last_batch('test', '1', '2', '3', 'jpg')

if __name__ == '__main__':
    batch_manager_main()
