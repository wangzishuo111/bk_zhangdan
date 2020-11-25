# -*- coding:utf-8 -*-


import redis_pool
import sys
import random
from rawdata_api_v1 import Api

def check(task_id):
	r = redis_pool.get('raw_data')
	keys = r.keys('track_' + task_id + '*')
	for i in range(20):
		idx = random.randint(0, len(keys) -1)
		track_id = keys[idx]
	
		data = r.hgetall(track_id)
		track_id = track_id[6:]
		key_list = data.keys()
		random.shuffle(key_list)
		tpid = key_list[0]
	
		print >> sys.stderr, 'track_id:%s, tpid:%s' % (track_id, tpid)
		data = Api().image_get(track_id, tpid)
		if not data:
			return False
		if len(data) < 100:
			print >> sys.stderr, 'bad jpg size[%d] %s,%s' % (len(data), track_id, tpid)
			return False

	return True
	

def main():
	while True:
		line = sys.stdin.readline()
		if not line: break
		task_id = line.strip()
		ret = check(task_id)
		print task_id, ret
	




if __name__ == '__main__':
	main()
