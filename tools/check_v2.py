
import api
import sys


def main():
	with open('/opt/hbase-store.shen/1017987.txt', 'r') as f:
		data = f.readlines()
	for line in data:
		segs = line.strip().split('-')
		track_id = segs[1].strip('\'')
		tpid = segs[2].strip('\'')
		tpe = '00'
		seq = '004'
		image_type = 'jpg'
		print >> sys.stderr, 'verify track_id[%s] tpid[%s]' % (track_id, tpid)
		ret = api.image_get(track_id, tpid, tpe, seq, image_type)
		if not ret:
			print >> sys.stderr, 'track_id[%s] tpid[%s] not 004' % (track_id, tpid)
		

if __name__ == "__main__":
	main()
