#!/usr/bin/python
# -*- coding: utf-8 -*-

import base64
import httplib
import json
from config import *
from base.log import *

__rest_host = GET_CONF('hbase_rest', 'host')
__rest_port = GET_CONF('hbase_rest', 'port')

def get_conn():
	return httplib.HTTPConnection("%s:%s" % (__rest_host, __rest_port))

def put_cols(table, row, cols): #(column_family, column, data)
	row = base64.b64encode(row)

	headers = {}
	headers['Content-Type'] = 'application/json'
	headers['Accept'] = 'application/json'

	data = {}
	rows = data["Row"] = []
	one_row = {}
	rows.append(one_row)

	one_row['key'] = row
	cells = one_row['Cell'] = []

	for (column_family, column, content) in cols:
		col = '%s:%s' % (column_family, column)
		logger().debug('%s: %s', col, content)
		col = base64.b64encode(col)
		content = base64.b64encode(content)
		one_cell = {}
		cells.append(one_cell)
		one_cell['column'] = col
		one_cell['$'] = content

	data = json.dumps(data)

	conn = get_conn()
	conn.request(method="PUT", url='/%s/fakerow' % table, body = data, headers = headers) 

	response = conn.getresponse()
	res = response.read()
	return 200 == response.status


def put_col(table, row, column_family, column, content):
	row = base64.b64encode(row)
	col = '%s:%s' % (column_family, column)
	col = base64.b64encode(col)
	content = base64.b64encode(content)

	headers = {}
	headers['Content-Type'] = 'application/json'
	headers['Accept'] = 'application/json'

	data = {}
	rows = data["Row"] = []
	one_row = {}
	rows.append(one_row)

	one_row['key'] = row
	cells = one_row['Cell'] = []
	one_cell = {}
	cells.append(one_cell)
	one_cell['column'] = col
	one_cell['$'] = content
	
	data = json.dumps(data)

	conn = get_conn()
	conn.request(method="PUT", url='/%s/fakerow' % table, body = data, headers = headers) 

	response = conn.getresponse()
	#res = response.read()
	return 200 == response.status
	
def put_col_old(table, row, column_family, column, data):
	row = base64.b64encode(row)
	col = '%s:%s' % (column_family, column)
	col = base64.b64encode(col)
	data = base64.b64encode(data)
	headers = {}
	headers['Content-Type'] = 'text/xml'
	headers['Accept'] = 'text/xml'
	
	data = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="%s"><Cell column="%s">%s</Cell></Row></CellSet>' % (row, col, data)
	conn = get_conn()
	conn.request(method="PUT", url='/%s/fakerow' % table, body = data, headers = headers) 

	response = conn.getresponse()
	res = response.read()
	return 200 == response.status

def get( table, row, column_family = None, column = None):
	headers = {'Accept': 'application/json'}

	if column_family and column:
		url = '/%s/%s/%s:%s' % (table, row, column_family, column)
	elif not column_family and not column:
		url = '/%s/%s' % (table, row)
	elif column_family:
		url = '/%s/%s/%s' % (table, row, column_family)
	
	conn = get_conn()
	conn.request(method="GET", url=url, headers = headers) 

	response = conn.getresponse()
	res = response.read()
	if 200 == response.status:
		return res
	else: 
		return None
		

def get_col(table, row, column_family, column):
	headers = {'Accept': 'application/json'}

	url = '/%s/%s/%s:%s' % (table, row, column_family, column)
	
	conn = get_conn()
	conn.request(method="GET", url=url) 

	response = conn.getresponse()
	
	if 200 == response.status:
		data = response.read()
		logger().info('data len:%d', len(data))
		return data
	else:
		logger().info('get col rest return[%d]', response.status)
		return None

def create_table(table, cf_list):
	headers = {}
	headers['Content-Type'] = 'text/xml'
	headers['Accept'] = 'text/xml'

	data = '<?xml version="1.0" encoding="UTF-8"?><TableSchema name="%s">' % table

	for cf in cf_list:
		if cf:
			data += '<ColumnSchema name="%s" />' % cf

	data += '</TableSchema>'

	conn = get_conn()
	conn.request(method="POST", url='/%s/schema' % table, body = data, headers = headers) 

	response = conn.getresponse()
	res = response.read()
	return response.status in [200, 201]


def build_one_filter(filter_info):
	column_family, column, content = filter_info
	column_family = base64.b64encode(column_family)
	column = base64.b64encode(column)
	content = base64.b64encode(content)
	
	data  = '''
		  {
        		"latestVersion":true, "ifMissing":true, 
		        "qualifier":"%s", "family":"%s", 
		        "op":"EQUAL", "type":"SingleColumnValueFilter", 
		        "comparator": {"value":"%s","type":"BinaryComparator"}
    	          }
		''' % (column, column_family, content)

	return data

def build_filters(filter_info_list):
	filters = ''
	for filter_info in filter_info_list:
		filter_cfg = build_one_filter(filter_info)
		if filters: filters += ',\n'
		filters += filter_cfg
	filters = '''
	  <filter> 
	  {
	"type":"FilterList",
        "op":"MUST_PASS_ALL",
        "filters":[
	''' + filters + '''
		 ]
       
	  }
	</filter> 
	'''
	return filters

def build_scan_cfg(batch = 100, start_row = '', end_row = '~~~', filters = None):
	start_row = base64.b64encode(start_row)
	end_row = base64.b64encode(end_row)

	part1 = '<Scanner startRow="%s" endRow="%s" batch="%s">' % (start_row, end_row, batch)
	if filters:
		part2 = build_filters(filters)
	else:
		part2 = ''
	part3 = '</Scanner>'
	return part1 + part2 + part3
	

def create_scanner(table, batch = 100, start_row = '', end_row = '~~~', filters = None):
	data = build_scan_cfg(batch = batch, start_row = start_row, end_row = end_row, filters = filters)
	
	headers = {}
	headers['Content-Type'] = 'text/xml'
	headers['Accept'] = 'application/json'

	conn = get_conn()
	conn.request(method="POST", url='/%s/scanner' % table, body = data, headers = headers) 

	response = conn.getresponse()
	headers = dict(response.getheaders())
	if 'location' not in headers:
		return None
	return headers['location'].split('/')[-1]


def scan_next(table, scan_id):
	headers = {}
	headers['Content-Type'] = 'text/xml'
	headers['Accept'] = 'application/json'

	conn = get_conn()
	conn.request(method="GET", url='/%s/scanner/%s' % (table, scan_id), headers = headers) 

	response = conn.getresponse()
	headers = dict(response.getheaders())
	res = response.read()
	return res


def destroy_scanner(table, scan_id):
	headers = {}
	headers['Content-Type'] = 'text/xml'
	headers['Accept'] = 'application/json'

	conn = get_conn()
	conn.request(method="DELETE", url='/%s/scanner/%s' % (table, scan_id), headers = headers) 

	response = conn.getresponse()
	return response.status == 200

class Scanner():
	def __init__(self, table, batch = 1, start_row = '', end_row = '~~~', filters = None):
		self.__scan_id = create_scanner(table, batch = batch, start_row = start_row, end_row = end_row, filters = filters)
		self.__table = table

	def __iter__(self):
		return self

	def next(self):
		 
		data = scan_next(self.__table, self.__scan_id)
		if not data: 
			self.destroy()
			raise StopIteration()
			
		return data

	def destroy(self):
		if self.__scan_id:
			destroy_scanner(self.__table, self.__scan_id)
			self.__scan_id = None

	def __del__(self):
		self.destroy()



def decode_json_data(data):
	data = json.loads(data)
	rows = data['Row']
	for row in rows:
		if 'key' in row:
			row['key'] = base64.b64decode(row['key'])
		if 'Cell' in row:
			cells = row['Cell']
			for cell in cells:
				cell['$'] = base64.b64decode(cell['$'])
				cell['column'] = base64.b64decode(cell['column'])
				del cell['timestamp']
	return data
	


def main():
	#print HBaseUtil().create_table('users', ['cf', 'ext'])
	#print build_one_filter(('cf', 'tag', '1'))
	#print build_filters([('cf', 'tag', '1'), ('cf', 'sex', 'male')])
	#print build_scan_cfg(start_row = '111', end_row = '222', batch = 999, filters = [('cf', 'tag', '1'), ('cf', 'sex', 'male')])

	print put_cols('users', 'cui', [('cf', 'sex', 'male'), ('cf', 'tag', '1')])
	print put_cols('users', 'wang', [('cf', 'sex', 'female'), ('cf', 'tag', '1')])
	print put_cols('users', 'tang', [('cf', 'sex', 'male'), ('cf', 'tag', '2')])
	print put_cols('users', 'zhang', [('cf', 'sex', 'female'), ('cf', 'tag', '2')])
	print get('users', 'cui')
	print get('users', 'cui')
	print get('users', 'cui', 'cf' )
	print get('users', 'cui', 'cf', 'name')
	print get_col('users', 'cui', 'cf', 'sex')

	print '------scan all --------'
	scan_id = create_scanner('users')

	print scan_next('users', scan_id)
	print destroy_scanner('users', scan_id)

	print '------scanner --------'
	for data in Scanner('users', batch=2, start_row = 'cui', end_row = 'tang~'):
		print decode_json_data(data)

	create_table('file_table_dev', ['data', 'info'])
	create_table('file_table_test', ['data', 'info'])
	create_table('file_table_prd', ['data', 'info'])
	



if __name__ == '__main__':
	main();
