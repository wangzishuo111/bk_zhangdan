
import redis_pool
import sys


r = redis_pool.get('raw_data')

task_id = sys.argv[1]

keys = r.keys('track_%s*' % task_id)

print keys
for k in keys:
	r.delete(k)
