# -*- coding:utf-8 -*-
import httplib
import json
import urllib
import urllib2
import sys

import base64
from timer import Timer

timer = Timer()
timer.stage_begin('1')

key = base64.b64encode('cui')
col = base64.b64encode('cf:home')
value = base64.b64encode('nj')

data = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="cm93NQo="><Cell column="Y2Y6ZQo=">dmFsdWU1Cg==</Cell></Row></CellSet>'
data = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="%s">      <Cell column="%s">     %s           </Cell></Row></CellSet>' % (key, col, value)


#data = urllib.quote(data)


print data
print len(data)


#{"Row":[{"key":"cm93NQo=", "Cell": [{"column":"Y2Y6ZQo=", "$":"dmFsdWU1Cg=="}]}]}


headers = {}
headers['Content-Type'] = 'text/xml'
headers['Accept'] = 'text/xml'
#headers['Accept'] = 'text/xml'
print headers

timer.stage_begin('get1')

conn = httplib.HTTPConnection("127.0.0.1:8000")
conn.request(method="PUT", url='/users/wang', body = data, headers = headers) 

response = conn.getresponse()
print 'headers:' + str(response.getheaders())
res= response.read()

print res

timer.finish()
print timer.dump()
print data
sys.exit(0)

url = 'http://localhost:8000/users/wang'
print data
req = urllib2.Request(url, data = data, headers = headers)
#req.add_header('Content-Type', 'text/xml')
#req.add_header('Accept', 'text/xml')

res_data = urllib2.urlopen(req)
res = res_data.read()
print len(res)
