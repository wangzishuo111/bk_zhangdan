# -*- coding:utf-8 -*-

import happybase 
import hbase_util_thrift as hbase
 
connection = happybase.Connection('localhost', 6666) 
meta_table = connection.table('file_meta_prd') 
data_table = connection.table('file_table_prd') 
 
#$print type(meta_table)

 
for key, data in meta_table.scan(row_prefix = '', limit=1000000, batch_size=10): 
	print key, data

	for k, v in data.items():
			 
		if k == 'meta:batch_list':
			if v == 'null':
				print 'fuck'
				d = data_table.row(key)
				image = d['content:null']
				hbase.put_col('file_table_prd', key, 'content', '!!!', image)
				data_table.delete(key, ['content:null'])
				hbase.put_col('file_meta_prd', key, 'meta', 'batch_list', '!!!')

		if k == 'meta:task_seq_null':
			print 'shit'
			hbase.put_col('file_meta_prd', key, 'meta', 'task_seq_!!!', '!!!')
			meta_table.delete(key, ['meta:task_seq_null'])
