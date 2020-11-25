#!/usr/bin/python
# -*- coding: utf-8 -*-

import base64
import httplib
import json
from config import *
from base.log import *
import happybase 
from timer import Timer
from base import util

__rest_host = GET_CONF('hbase_rest', 'host')
__rest_port = GET_CONF('hbase_rest', 'port')
__thrift_host = GET_CONF('hbase_thrift', 'host')
__thrift_port = int(GET_CONF('hbase_thrift', 'port'))

thrift_conn = None

def get_thrift_conn():
	global thrift_conn
	if not thrift_conn:
		thrift_conn = happybase.Connection(__thrift_host, __thrift_port) 
	return thrift_conn

def thrift_reconn():
	global thrift_conn
	thrift_conn.close()
	thrift_conn = happybase.Connection(__thrift_host, __thrift_port)

def _get(table, row, column_family, column):
	conn = get_thrift_conn()
	table = conn.table(table)
	if column_family and column:
		columns = []
		columns.append('%s:%s' % (column_family, column))
		return table.row(row, columns = columns)
	elif column_family:
		columns = []
		columns.append('%s' % (column_family))
		return table.row(row, columns = columns)
		
	else:
		return table.row(row)

def get(table, row, column_family = None, column = None):
	try:
		ret = _get(table, row, column_family, column)
	except:
		thrift_reconn()
		ret = _get(table, row, column_family, column)
	return ret

def get_col(table, row, column_family, column):
	return get(table, row, column_family, column)


class Writer():
	def __init__(self, data_dir):
		self.data_dir = data_dir
		self.task = None
		self.fd = None

	def write_line(self, task, line):
		if self.task == task:
			self.fd.write(line)
		else:
			if self.fd: self.fd.close()
			self.fd = open(os.path.join(self.data_dir, task), 'w')
			self.fd.write(line)
			self.task = task

	def finish(self):
		if self.fd: self.fd.close()

def main():
	data_dir = sys.argv[1]
	
	writer = Writer(data_dir)
	conn = get_thrift_conn()
	table = conn.table('file_table_prd_v2')
	count = 0
	for row_data in table.scan(row_start = '20-1228400_20180927161222480-1228400_20180927161410841747', row_stop = '20-1228400_20180927161222480-1228400_20180927161410841747~', batch_size = 1):
		rowkey = row_data[0]
		print rowkey
		continue
		for k, v in row_data[1].items():
			print k, len(v)
		count += 1
		if count % 1000 == 0: 
			logger().info('%d lines processed, current rowkey[%s]', count, rowkey)
			
			

	writer.finish()
	print 'total count:', count
		

	

 



if __name__ == '__main__':
	main();
