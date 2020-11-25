
import redis_pool
import sys


r = redis_pool.get('raw_data')

keys = r.keys('track_*')
for key in keys:
	
	data = r.hgetall(key)
	for tp in data:
		print key, tp
