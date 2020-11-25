#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import base64
import sys
import os
import time

import redis_pool
from base.log import logger

class Api(object):
    def __init__(self):
        self.__raw_redis = redis_pool.get('raw_data')
        self.__upload_redis = redis_pool.get('upload')
        #self.__p = Popen('echo', stdin=PIPE, stdout=PIPE)

    def image_exist(self, track_id, tpid):
        segs = track_id.split('_')
        assert len(segs) == 2, 'bad track_id[%s]' % track_id
        task_id = segs[0]
        return self.__raw_redis.hexists('track_' + track_id, tpid)

    def get_task_info(self, task):
        data = self.__upload_redis.hget('task_info', task)
        try:
            jdata = json.loads(data)
        except: # pylint: disable=W0702
            return None, None

        num_reducer = int(jdata['num_reducer'])
        task_seq = jdata['task_seq']
        return task_seq, num_reducer

    def image_get(self, track_id, tpid):
        task_id = track_id.split('_')[0]
        task_seq, num_reducer = self.get_task_info(task_id)
        logger().info('task_seq:%s, num_reducer:%s', task_seq, str(num_reducer))
        if not task_seq or not num_reducer:
            return None
        cmd = '/opt/hadoop-3.1.0/bin/hadoop jar ' + \
              'jar/HDFS004Store.jar kd.mapreduce.Hdfs004Store2 '
        arg = '%s %s %d %s' % (tpid, task_seq, num_reducer, './' + tpid)
        cmd = "%s %s" % (cmd, arg)
        logger().info('cmd:%s', cmd)
        ret = 0 == os.system(cmd) # pylint: disable C0122
        if ret:
            data = open('./' + tpid).read()
            jpg = data.split(',')[-1]
            jpg = base64.b64decode(jpg)

            os.remove('./' + tpid)
            logger().info('jpg size:%d', len(jpg))
            return jpg
        else:
            return None

def image_exist(track_id, tpid):
    return Api().image_exist(track_id, tpid)

def image_get(track_id, tpid):
    return Api().image_get(track_id, tpid)

def main():
    data = Api().image_get(
            '1014140_20180804111056883', '1014140_20180804111248548156')
    print len(data)
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        track_id, tpid = line.split(',')
        beg = time.time()
        data = Api().image_get(track_id, tpid)
        end = time.time()
        print >> sys.stderr, '---cost: %.2f SEC' % (end - beg)
        print len(data)

if __name__ == '__main__':
    main()


