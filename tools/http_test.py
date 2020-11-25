# -*- coding:utf-8 -*-
import httplib
import json
import urllib
import urllib2
import sys
import os
import time

from timer import Timer
import httplib

conn = httplib.HTTPConnection("localhost:9527")

'''
timer = Timer()

timer.stage_begin('create')
data = {}
data['table'] = 'abc'
data['column_family'] = ['info', 'home']
data = json.dumps(data)


url = 'http://localhost:9527/table/create'
req = urllib2.Request(url = url, data = data)

#res_data = urllib2.urlopen(req)
#res = res_data.read()
#print len(res)

timer.stage_begin('get')
url = 'http://localhost:9527/table/get?table=users&row=cui&column_family=cf&column=sex'
req = urllib2.Request(url = url)

res_data = urllib2.urlopen(req)
res = res_data.read()
print res

timer.stage_begin('create track')
'''
url = 'http://localhost:9527/track/create?track_id=test'
req = urllib2.Request(url = url)

res_data = urllib2.urlopen(req)
res = res_data.read()
print res


def write_image(track_id, track_point_id, data):
	url = '/image/write?track_id=%s&track_point_id=%s' % (track_id, track_point_id)
	#print 'write image len:', len(data)
	conn.request(method="POST", url=url, body=data)

	response = conn.getresponse()
	res_data = response.read()
	status = response.status
	#print status
	#print res_data

def read_image(track_id, track_point_id):
	url = '/image/read?track_id=%s&track_point_id=%s' % (track_id, track_point_id)
	conn.request(method="GET", url=url)

	response = conn.getresponse()
	status = response.status
	#print 'status:' ,status
	res_data = response.read()
	#print len(res_data)
	return res_data

def cut_name(name):
	return name[:-4]

def file2str(fpath):
	return open(fpath, 'rb').read()

def write_performance():
	flist = os.listdir('/tmp/data1')
	#print len(flist)
	begin = time.time()
	for fname in flist:
		#print fname
		#print cut_name(fname)
		fpath = '/tmp/data1/' + fname
		data = file2str(fpath)
		#print 'len:', len(data)
		write_image('test', cut_name(fname), data)
		#data = read_image('test', '1')
		#print type(data)
		#break
	end = time.time()
	print 'write test'
	print 'time cost:%.3f' % (end - begin)
	print 'avg cost:%.3f' % ((end - begin) / len(flist))
	print '-----------------'

def read_performance():
	flist = os.listdir('/tmp/data1')
	print len(flist)
	begin = time.time()
	for fname in flist:
		#print fname
		#print cut_name(fname)
		fpath = '/tmp/data1/' + fname
		data = read_image('test', cut_name(fname))
		#data = read_image('test', '1')
		#print len(data)
		#break
	end = time.time()
	print 'read test'
	print 'time cost:%.3f' % (end - begin)
	print 'avg cost:%.3f' % ((end - begin) / len(flist))
	print '-----------------'

def main():
	write_performance()
	read_performance()

if __name__ == '__main__' :
	main()
