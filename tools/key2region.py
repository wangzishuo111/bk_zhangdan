#!/usr/bin/python
# -*- coding: utf-8 -*-

import happybase 
import sys
import hbase_util_thrift
import json

def main():
	regions = hbase_util_thrift.get_regions('file_table_prd_v2')
	rowkey = sys.argv[1]
	for region in regions:
		start_key = region['start_key']
		end_key = region['end_key']
		if rowkey >= start_key and rowkey < end_key:
			print region['server_name']
			break
	
	

if __name__ == '__main__':
	main()

