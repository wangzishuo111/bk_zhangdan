#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.log import logger
import base.opt as opt
from config import GET_CONF

import api_v1
import api_v2
import hbase_util
import kv_store

def image_upload(track_id, tpid, tpe, seq,
                image_type, content, task_seq='!!!', batch='!!!'):
    return api_v2.image_upload(track_id, tpid, tpe, seq,
                               image_type, content, task_seq, batch)

def image_get(track_id, tpid, tpe, seq, image_type, batch=None):
    data = api_v2.image_get(track_id, tpid, tpe, seq, image_type, batch)
    if data:
        return data
    return api_v1.image_get(track_id, tpid, tpe, seq, image_type, batch)

def image_clear(track_id, tpid, tpe, task_seq):
    pass

def image_delete(track_id, tpid, tpe, seq, image_type, batch=''):
    api_v1.image_delete(track_id, tpid, tpe, seq, image_type, batch)
    api_v2.image_delete(track_id, tpid, tpe, seq, image_type, batch)
    return True

def image_exist(track_id, tpid, tpe, seq, image_type, batch=''):
    if api_v2.image_exist(track_id, tpid, tpe, seq, image_type, batch):
        return True
    return api_v1.image_exist(track_id, tpid, tpe, seq, image_type, batch)

def image_stat(track_id, tpe, seq_list, image_type):
    return []

def get_track_location_table():
    env = opt.option().env
    return GET_CONF('png_location', env)

def track_location_set(track_id, location):
    table_name = get_track_location_table()
    cols = []
    cols.append(('location', 'hdfs_path', location))
    ret = hbase_util.put_cols(table_name, track_id, cols)
    logger().info('write hbase rowkey[%s] -> column[%s] content[%s]',
                    track_id, 'location:hdfs_path', location)
    return ret

def track_location_get(track_id):
    table_name = get_track_location_table()
    ret = hbase_util.get(table_name, track_id,
                        column_family='location', column='hdfs_path')
    if not ret:
        logger().info('rowkey[%s] not found', track_id)
        return None
    ret = ret['location:hdfs_path']
    logger().info(
            'get hbase rowkey[%s] column[location:hdfs_path]-> content[%s]',
            track_id, ret)
    return ret

def get_track_batch_table():
    env = opt.option().env
    return GET_CONF('newest_batch', env)

def track_batch_set(track_id, batch):
    table_name = get_track_batch_table()
    cols = []
    cols.append(('batch', 'newest', batch))
    ret = hbase_util.put_cols(table_name, track_id, cols)
    logger().info('write hbase rowkey[%s] -> column[%s] content[%s]',
            track_id, 'batch:newest', batch)
    return ret

def track_batch_get(track_id):
    table_name = get_track_batch_table()
    ret = hbase_util.get(table_name, track_id,
                        column_family='batch', column='newest')
    if not ret:
        logger().info('rowkey[%s] not found', track_id)
        return None
    ret = ret['batch:newest']
    logger().info(
            'get hbase rowkey[%s] column[batch:newest] -> content[%s]',
            track_id, ret)
    return ret

def jaguar_idmap_set(kts_id, jaguar_id):
    prefix = 'idmap'
    key = '%s_%s' % (prefix, kts_id)
    return kv_store.write(key, jaguar_id)

def jaguar_idmap_get(kts_id):
    prefix = 'idmap'
    key = '%s_%s' % (prefix, kts_id)
    return kv_store.read(key)

def jaguar_track_extend_set(jaguar_id, track_extend_info):
    prefix = 'track_extend'
    key = '%s_%s' % (prefix, jaguar_id)
    return kv_store.write(key, track_extend_info)

def jaguar_track_extend_get(jaguar_id):
    prefix = 'track_extend'
    key = '%s_%s' % (prefix, jaguar_id)
    return kv_store.read(key)

def jaguar_osm_auto_set(jaguar_id, auto_osm_info):
    prefix = 'auto_osm'
    key = '%s_%s' % (prefix, jaguar_id)
    return kv_store.write(key, auto_osm_info)

def jaguar_osm_auto_get(jaguar_id):
    prefix = 'auto_osm'
    key = '%s_%s' % (prefix, jaguar_id)
    return kv_store.read(key)

def jaguar_osm_fusion_set(jaguar_id, fusion_osm_info):
    prefix = 'fusion_osm'
    key = '%s_%s' % (prefix, jaguar_id)
    return kv_store.write(key, fusion_osm_info)

def jaguar_osm_fusion_get(jaguar_id):
    prefix = 'fusion_osm'
    key = '%s_%s' % (prefix, jaguar_id)
    return kv_store.read(key)

def api_main():
    print 'set track_extend :', \
            jaguar_track_extend_set('11_123_123', '{"k1": "v1"}')
    print 'get track_extend :', \
            jaguar_track_extend_get('11_123_123')
    print 'set idmap :', \
            jaguar_idmap_set('1111', '1234')
    print 'get idmap :', \
            jaguar_idmap_get('1111')
    print 'set auto osm :', \
            jaguar_osm_auto_set('11_123_123', 'auto osm test info')
    print 'get auto osm :', \
            jaguar_osm_auto_get('11_123_123')
    print 'set fusion osm :', \
            jaguar_osm_fusion_set('11_123_123', 'fusion osm test info')
    print 'get fusion osm :', \
            jaguar_osm_fusion_get('11_123_123')
    return
    print track_batch_set('1222', 'test_1')
    print track_batch_get('1222')
    print track_location_set('1222', '/hdfs/haha')
    print track_location_get('1222')
    print 'delete :', image_delete('test', '11', '2', '3', 'jpg')
    print 'delete :', image_delete('test', '11', '2', '3', 'jpg')
    print 'delete :', image_delete('test', '11', '2', '3', 'jpg')
    print 'delete :', image_delete('test', '11', '2', '3', 'jpg')
    print 'delete :', image_delete('test', '11', '2', '3', 'jpg')
    print 'upload:', image_upload('test', '11', '2', '3', 'jpg',
                                  'content_0')
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
    print 'exists default:', image_exist('1_1', '11', '00', '004', 'jpg')

if __name__ == '__main__':
    api_main()
