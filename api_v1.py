#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.log import logger
from base.opt import FLAG
from config import GET_CONF
from base.timer import Timer

import hbase_util
import batch_manager
import rowkey

def get_table_name():
    env = FLAG().env
    return GET_CONF('content_table', env)

def image_write(track_id, tpid, tpe, seq,
                image_type, content, task_seq, batch):
    row_key = rowkey.gen(track_id, tpid, tpe, seq, image_type)

    cols = []
    table = batch_manager.get_table_name()
    cols.append(('meta', 'type', tpe))
    cols.append(('meta', 'seq', seq))
    cols.append(('meta', 'image_type', image_type))
    cols.append(('meta', 'task_seq_' + task_seq, batch))
    if not hbase_util.put_cols(table, row_key, cols):
        return False

    cols = []
    table = get_table_name()
    logger().info('write row_key[%s] to table[%s]', row_key, table)
    cols.append(('content', batch, content))
    if not hbase_util.put_cols(table, row_key, cols):
        return False
    return True

def image_upload(track_id, tpid, tpe, seq, image_type,
                content, task_seq='!!!', batch='!!!'):
    if not task_seq:
        task_seq = '!!!'
    if not batch:
        batch = '!!!'
    _timer = Timer()
    _timer.stage_begin('image write')
    if not image_write(track_id, tpid, tpe, seq,
                       image_type, content, task_seq, batch):
        return False
    _timer.stage_begin('set batch')
    if not batch_manager.set_batch(track_id, tpid, tpe,
                                   seq, image_type, batch):
        return False
    _timer.finish()
    logger().debug(_timer.dump())
    return True

def image_get(track_id, tpid, tpe, seq, image_type, batch=None):
    #row_key = '%s-%s_%s_%s_%s' % (track_id, tpid, tpe, seq, image_type)
    row_key = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    if not batch:
        batch = batch_manager.get_last_batch(
            track_id, tpid, tpe, seq, image_type)
        if not batch:
            logger().error('get empty batch for row_key[%s]', row_key)
            return None

    table = get_table_name()
    logger().debug('row [%s], batch[%s]', row_key, batch)
    ret = hbase_util.get_col(table, row_key, 'content', batch)
    if ret:
        return ret['content:' + batch]
    return None

def image_clear(track_id, tpid, tpe, task_seq):
    pass

def image_delete(track_id, tpid, tpe, seq, image_type, batch=''):
    row_key = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    if not batch:
        batch = batch_manager.get_last_batch(
            track_id, tpid, tpe, seq, image_type)
        if not batch:
            logger().error('get empty batch for row_key[%s]', row_key)
            return False

    table = get_table_name()
    columns = ['content:' + batch]
    logger().debug('delete row [%s], batch[%s]', row_key, batch)
    hbase_util.delete(table, row_key, columns=columns)
    batch_manager.del_batch(track_id, tpid, tpe, seq, image_type, batch)
    return True

def image_exist(track_id, tpid, tpe, seq, image_type, batch=''):
    row_key = rowkey.gen(track_id, tpid, tpe, seq, image_type)
    logger().info('row_key[%s], batch[%s]', row_key, batch)
    if not batch:
        last_batch = batch_manager.get_last_batch(track_id, tpid, tpe, seq, image_type)
        return last_batch != None
    else:
        if tpe == '00' and seq == '004':
            return False
        else:
            table = get_table_name()
            ret = hbase_util.get_col(table, row_key, 'content', batch)
            return not not ret

def image_stat(track_id, tpe, seq_list, image_type):
    conn = hbase_util.get_thrift_conn()
    table_name = batch_manager.get_table_name()
    logger().info('scan table:%s', table_name)
    table = conn.table(table_name)
    start_key = '%d-%s' % (rowkey.compute_magic(track_id), track_id)

    ret = {}
    for seq in seq_list:
        ret[seq] = []

    for row_data in table.scan(row_start=start_key,
                               row_stop=start_key + '~',
                               batch_size=300):
        row_key = row_data[0]
        data = row_data[1]
        if 'meta:type' not in data or tpe != data['meta:type']:
            logger().info('bad type[%s], rowkey[%s]',
                          data['meta:type'], row_key)
            continue
        if 'meta:seq' not in data or data['meta:seq'] not in seq_list:
            logger().info('bad seq [%s], rowkey[%s]',
                          data['meta:seq'], row_key)
            continue
        if 'meta:image_type' not in data or \
                image_type != data['meta:image_type']:
            logger().info('bad image_type[%s], rowkey[%s]',
                          data['meta:image_type'], row_key)
            continue
        if 'meta:batch_list' not in data or not data['meta:batch_list']:
            logger().info('empty batch_list for rowkey[%s]', row_key)
        tpid = '_'.join(row_key.split('-')[2].split('_')[0:2])
        ret[data['meta:seq']].append(tpid)

    for key, val in ret.items():
        logger().info('stat result %s:%d', key, len(val))

    return ret

def api_v1_main():
    print 'delete:', image_delete('test', '11', '2', '3', 'jpg')
    print 'delete:', image_delete('test', '11', '2', '3', 'jpg')
    print 'delete:', image_delete('test', '11', '2', '3', 'jpg')
    print 'get default:', image_get('test', '11', '2', '3', 'jpg')

    print 'upload:', image_upload('test', '11', '2', '3', 'jpg',
                                  'content_1', batch='batch_1')
    print 'get last:', image_get('test', '11', '2', '3', 'jpg')

    print 'upload:', image_upload('test', '11', '2', '3', 'jpg',
                                  'content_3', batch='batch_3')
    print 'get last:', image_get('test', '11', '2', '3', 'jpg')

    print 'upload:', image_upload('test', '11', '2', '3', 'jpg',
                                  'content_2', batch='batch_2')
    print 'get last:', image_get('test', '11', '2', '3', 'jpg')

    print 'get batch1:', image_get('test', '11', '2', '3', 'jpg',
                                    batch='batch_1')
    print 'get batch2:', image_get('test', '11', '2', '3', 'jpg',
                                    batch='batch_2')
    print 'get batch3:', image_get('test', '11', '2', '3', 'jpg',
                                    batch='batch_3')
    print 'exists batch1:', image_exist('test', '11', '2', '3', 'jpg',
                                        batch='batch_1')
    print 'exists batch2:', image_exist('test', '11', '2', '3', 'jpg',
                                        batch='batch_2')
    print 'exists batch3:', image_exist('test', '11', '2', '3', 'jpg',
                                        batch='batch_3')
    print 'exists default:', image_exist('test', '11', '2', '3', 'jpg')



if __name__ == '__main__':
    api_v1_main()
