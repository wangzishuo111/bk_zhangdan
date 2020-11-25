#!/usr/bin/env python
# -*- coding: utf-8 -*-


import redis_pool
from base.log import *
import base64
from timer import Timer
from redis import StrictRedis


class RedisUtil():

    def __init__(self, env):
        self.write_dic = redis_pool.get('write_dic_' + env)
        self.write_q = redis_pool.get('write_q_' + env)
        #self.write_dic = StrictRedis(host='192.168.5.46', port=6379, db=4, password='kdgogo')
        #self.write_q = StrictRedis(host='192.168.5.46', port=6379, db=7, password='kdgogo')

    def write_image_to_redis(self, row_key, mappings):
        _timer = Timer()
        _timer.stage_begin('base64')
        content = base64.b64encode(mappings['content'])
        _timer.stage_end()
        mappings.update({'content': content})
        _timer.stage_begin('write_redis')
        self.write_dic.hmset(row_key, mappings)
        self.write_q.rpush('write_q', row_key)
        _timer.finish()
        logger().info(_timer.dump())
        return True

    def get_img_from_redis(self, row_prefix, batch):
        _timer = Timer()
        _timer.stage_begin('get')
        if batch:
            key = row_prefix + batch
            data = self.write_dic.hgetall(key)
            if data:	
                data = base64.b64decode(data['content'])	
                _timer.finish()
                logger().info(_timer.dump())
                return data
        else:
            row_pattern = row_prefix + '*'
            #logger().info('row_pattern[%s]', row_pattern)
            _timer.stage_begin('scan')
            _scanner = self.write_dic.scan_iter(match=row_pattern)
            _timer.stage_begin('loop get')
            for key in _scanner:
                if key:
                    data = self.write_dic.hgetall(key)
                    return base64.b64decode(data['content'])
        return None

    def repair(self):
        key_list = self.write_q.lrange('write_q', 0, -1)


def main():
    manager = RedisUtil('dev')
    #mappings = {'type': '00', 'seq': '004', 'task_seq': '', 'batch': '1', 'content': 'abc test', 'image_type': 'txt'}
    #manager.write_image_to_redis('4503_20180516151148537-4503_20180516151215585697_00_004_jpeg-_1', mappings)
    data = manager.get_img_from_redis('4503_20180516151148537-4503_20180516151215585697_00_004_jpeg-', '')
    #print data
    if data:
        open('test.jpeg', 'wb').write(data)
    else:
        print(data)

if __name__ == '__main__':
    main()
