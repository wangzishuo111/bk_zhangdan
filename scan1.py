#!/usr/bin/python
# -*- coding: utf-8 -*-

import base64
import httplib
import json
from config import *
from base.log import *
import happybase 
from base.timer import Timer
from base import util

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


def main(task_id):
	conn = get_thrift_conn()
	table = conn.table('file_table_prd_v2')
	count = 0
	for i in range(100):
		if len(str(i)) != 2:
			i = '0' + str(i)
		else:
			i = str(i)
		start_row = i + '-' + task_id
		stop_row = i + '-' + task_id + '~'
		for row_data in table.scan(row_start = start_row, row_stop = stop_row, batch_size = 1):
			rowkey = row_data[0]
			print rowkey
			print row_data[1].keys()
	#		if count % 1000 == 0: 
	#			logger().info('%d lines processed, current rowkey[%s]', count, rowkey)
			
	print 'total count:', count
		
if __name__ == '__main__':
	task_id = sys.argv[1]
	main(task_id);
