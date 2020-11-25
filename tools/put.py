# -*- coding:utf-8 -*-
import httplib
import json
import urllib
import urllib2

import base64
from timer import Timer

timer = Timer()
timer.stage_begin('1')

data = {}
row = {}
row['Row'] = 'cui'

col = base64.b64encode('info:name')
val = base64.b64encode('xuting')
content = {"column": col, "$": val}

one_row = {}
one_row['key'] = base64.b64encode('wang')
one_row['Cell'] = [content]
row['Row'] = [one_row]
data = json.dumps(row)
print data


#{"Row":[{"key":"cm93NQo=", "Cell": [{"column":"Y2Y6ZQo=", "$":"dmFsdWU1Cg=="}]}]}

url = 'http://localhost:8000/data2/wang'
req = urllib2.Request(url = url, data = data)

headers = {}
headers['Content-Type'] = 'text/json'
headers['Accept'] = 'text/json'

conn = httplib.HTTPConnection("localhost:8000")
timer.stage_begin('get1')
conn.request(method="POST", url=url, body=data, headers = headers) 
response = conn.getresponse()
res= response.read()
print res

timer.finish()
print timer.dump()
