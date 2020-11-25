# -*- coding:utf-8 -*-
import httplib
import json
import urllib
import urllib2

from timer import Timer

timer = Timer()
timer.stage_begin('1')
url = 'http://localhost:8000/data1/4163_20180511133421284265/pic:data'
req = urllib2.Request(url = url, data = None)

res_data = urllib2.urlopen(req)
res = res_data.read()
print len(res)

timer.stage_begin('2')
url = 'http://localhost:8000/data1/4163_20180511133439932445/pic:data'
req = urllib2.Request(url = url, data = None)

res_data = urllib2.urlopen(req)
res = res_data.read()
print len(res)

timer.stage_begin('3')
url = 'http://localhost:8000/data1/4163_20180511133421284265/pic:data'
req = urllib2.Request(url = url, data = None)

res_data = urllib2.urlopen(req)
res = res_data.read()
print len(res)

timer.stage_end()

timer.stage_begin('conn')
conn = httplib.HTTPConnection("localhost:8000")
timer.stage_begin('get1')
conn.request(method="GET", url=url) 
response = conn.getresponse()
res= response.read()
print len(res)


timer.stage_begin('get2')
url = 'http://localhost:8000/data1/4163_20180511133421284265/pic:data'
conn.request(method="GET", url=url) 
response = conn.getresponse()
res= response.read()
print len(res)

timer.stage_begin('get3')
url = 'http://localhost:8000/data1/152645313337724714319/pic:data'
conn.request(method="GET", url=url) 
response = conn.getresponse()
res= response.read()
print len(res)


timer.finish()
print timer.dump()
