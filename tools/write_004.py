

import sys
import api
import time
cnt = 0
for line in sys.stdin:
	track_id, tpid = line.strip().split(' ')
	track_id = track_id[6:]
	beg = time.time()
	api.image_upload(track_id, tpid, '00', '004', 'jpg','')
	end = time.time()
	cnt += 1

	if cnt % 10000 == 0:
		print >> sys.stderr, '%d lines' % cnt
		print >> sys.stderr, 'track_id:', track_id
		print >> sys.stderr, 'tpid:', tpid
		print >> sys.stderr, 'cost:', (end - beg)
