#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
from subprocess import Popen, PIPE
from base.log import *
import base64
import redis_pool
from base.singleton import *

def remove(path):
    try: os.remove(path)
    except: pass

@singleton
class Api():
    def __init__(self):
        self.__raw_redis = redis_pool.get('raw_data')
        self.__upload_redis = redis_pool.get('upload')
        self.__p = Popen('echo', stdin=PIPE, stdout=PIPE)
        self.__pipe_in, self.__pipe_out = os.popen2("hadoop jar jar/HDFS004Store3.jar kd.mapreduce.Hdfs004Store", "wr"); 

    def image_exist(self, track_id, tpid):
        segs = track_id.split('_') 
        assert len(segs) == 2, 'bad track_id[%s]' % track_id
        task_id = segs[0]
        return self.__raw_redis.hexists('track_' + track_id, tpid)

    def get_task_info(self, task):
        data = self.__upload_redis.hget('task_info', task)
        try:
            jdata = json.loads(data)
        except:
            return None, None

        num_reducer = int(jdata['num_reducer'])
        task_seq = jdata['task_seq']
        return task_seq, num_reducer

    def image_get(self, track_id, tpid):
        task_id = track_id.split('_')[0]
        task_seq, num_reducer = self.get_task_info(task_id)
        tp_file = './' + tpid
        remove(tp_file)
        #l = raw_input()
        logger().info('task_seq:%s, num_reducer:%d', task_seq, num_reducer)
        if not task_seq or not num_reducer: 
            return None
        arg = '%s,%s,%d,%s\n' % (tpid, task_seq, num_reducer, tp_file)
        logger().info('arg:%s', arg)
        self.__pipe_in.write(arg)
        self.__pipe_in.flush()
        for i in range(100):
            if not os.path.exists(tp_file):
                time.sleep(0.1)
                continue
            time.sleep(0.2)
            data = open(tp_file).read()
            logger().info('data size:%d', len(data))
            if not data:
                return None
            jpg = data.split(',')[-1]
            
            jpg = base64.b64decode(jpg)

            remove(tp_file)
            return jpg

        logger().error('image get timeout')
        return None

def image_exist(track_id, tpid):
    return Api().image_exist(track_id, tpid)

def image_get(track_id, tpid):
    return Api().image_get(track_id, tpid)

def main():
    log_init('INFO', './123.log', quiet = False)
    #print Api().image_exist('1014140_20180804111056883', '1014140_20180804111248548156')
    data = Api().image_get('1000042_20180829144927988', '1000042_20180829144928535337')
    print len(data)
    return
    for i in range(10):
        line = raw_input()
        if not line: break
        if i % 2 == 0:
            Api().image_get('1014140_20180804111056883', '1014140_20180804111248548156')
        else:
            Api().image_get('1018014_20180814111400782', '1018014_20180814111415902740')

if __name__ == '__main__':
    main()

