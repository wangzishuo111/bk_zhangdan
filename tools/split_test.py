# -*- coding:utf-8 -*-

import happybase 
import hbase_util_thrift as hbase
 
connection = happybase.Connection('localhost', 6666) 
meta_table = connection.table('file_meta_prd') 
data_table = connection.table('file_table_prd') 
 
#$print type(meta_table)

content = ''
for i in range(10000):
	content += 'a'

 
for i in range(1000000):
	print i
	hbase.put_col('users', str(i), 'cf', 'content', content)

