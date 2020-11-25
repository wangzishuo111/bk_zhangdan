# encoding=utf-8  
from timer import Timer
import time
import random
from multiprocessing import Process
  
''''' 
该脚本用于尝试 使用 python 通过 Thrift 连接并操作 HBase 数据库 
prepare： 
    1. 启动 ThriftServer 于 HBASE 
    > hbase-deamn.sh start thrift/thrift2 
    > 在此，HBASE提供两种 thrift/thrift2 由于种种原因，语法并不兼容，其中 2 的语法封装更优雅，但部分 DDL 操作 
      不完善，而且 thrift API 资料相对多一些，所以先使用thrift 尝试 
    2. jps 应该有 ThriftServer 进程 
    3.Python 需要安装 thrift 和 hbase 模块，有网络的直接 pip，没有网络的把相同版本的模块代码下载下来用 sys.path.append('PATH') 引用,安装后的代码一般在 $PYTHON_HOME/Lib/site-packages 
    > pip install thrift 
      pip install hbase-thrift 
'''  
from thrift import Thrift  
from thrift.transport import TSocket, TTransport  
from thrift.protocol import TBinaryProtocol  
from hbase import Hbase  
import sys
  
'''
# server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口  
transport = TSocket.TSocket('192.168.5.48', 6666)  
  
# 可以设置超时  
transport.setTimeout(5000)  
# 设置传输方式（TFramedTransport或TBufferedTransport）  
trans = TTransport.TBufferedTransport(transport)  
# 设置传输协议  
protocol = TBinaryProtocol.TBinaryProtocol(trans)  
# 确定客户端  
client = Hbase.Client(protocol)  
# 打开连接  
transport.open()  
'''
  
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, TRegionInfo  
from hbase.ttypes import IOError, AlreadyExists  
  
tableName = "users"  
rowkey = "58339"  
  
# 获取所有表名  
  
'''
tableNames = client.getTableNames()  
print('tableNames:',tableNames)  
  
# 获取列族，返回map  
columnDescriptors = client.getColumnDescriptors(tableName)  
print("columnName",columnDescriptors)  
# 获取该表的所有Regions，包括起止key等信息，返回list  
tableRegions = client.getTableRegions(tableName)  
'''
  
# 获取行(tableName,rowKey) return List<TRowResult>  
  
'''
timer = Timer()
timer.stage_begin('get row')

row = client.getRow(tableName,rowkey)  

'''
#print("row:",row)  
  
# 获取 row 里的某一列  
#timer.stage_begin('get col')
#rowColumn = client.get(tableName,rowkey,"cf:content")  
#print("rowColumn",len(rowColumn)  )

# 获取 row 里的多列时间戳最新的，None 则为所有列  
#rowColumns = client.getRowWithColumns(tableName,rowkey,["bbdi:openId","bbdi:tempLogId"])  
#print("rowColumns",rowColumns)  
  
# client.mutateRow(tableName[1],"jason",)  
  
'''
# 创建表  
try:  
    # 创建列族，这里只传了第一个参数 name  
        struct ColumnDescriptor { 
          1:Text name, 
          2:i32 maxVersions = 3, 
          3:string compression = "NONE", 
          4:bool inMemory = 0, 
          5:string bloomFilterType = "NONE", 
          6:i32 bloomFilterVectorSize = 0, 
          7:i32 bloomFilterNbHashes = 0, 
          8:bool blockCacheEnabled = 0, 
          9:i32 timeToLive = -1 
        } 
    desc = ColumnDescriptor(name="colNameTest1")  
    # 创建表 (tableMame,[列族们])  
    client.createTable('our_table1', [desc])  
  
    print client.getTableNames()  
except AlreadyExists, tx:  
     print "Thrift exception"  
     print '%s' % (tx.message)  
'''
  
'''
# 插入行  
timer.stage_begin('put')
content = open('1_2_3.jpeg').read()
print len(content)
mutations = [Mutation(column="cf:content", value=content)]  
client.mutateRow("users","12345",mutations)  
timer.finish()
print timer.dump()
sys.exit(0)
  
#插入多行  
rowMutations = [BatchMutation("rowkey1",mutations),BatchMutation("rowkey2",mutations)]  
client.mutateRows("our_table1",rowMutations)  
  
# 删除一行  
client.deleteAllRow("our_table1","rowkey2")  
 
# scan  
# ScannerID scannerOpen(Text tableName, Text startRow, list<Text> columns)  
scanId = client.scannerOpen("our_table1","",["colNameTest1"])  
scanRescult =  client.scannerGet(scanId) #从scan中取一条  
scanRescult1 = client.scannerGetList(scanId,50) #从scan中取多条，同一个ScanID 上面去过一条，下面就取不到了  
print(scanRescult)  
print(scanRescult1)  
# 关闭该扫描  
client.scannerClose(scanId);  
'''


content = open('1_2_3.jpeg').read()
def put_one(key, client):
	timer = Timer()
	timer.stage_begin('put')
	#content = open('1_2_3.jpeg').read()
	print len(content)
	print "cf:content"
	mutations = [Mutation(column="cf:content", value=content)]  
	client.mutateRow("users", key, mutations)  
	timer.finish()
	print timer.dump()

def task(msg):
	for i in range(int(sys.argv[2])):
		key = str(random.randint(0, 100000))

		put_one(key)	

def main1():
	plist = []
	beg = time.time()
	num_task = int(sys.argv[1])
	num_row = int(sys.argv[2])
	for i in range(num_task):
		p = Process(target=task, args=('world',))
		plist.append(p)
		p.start()
	

	for p in plist:
		p.join()
	end = time.time()
	print end - beg
	print (end - beg) / num_task / num_row
	print 1.06 * num_task * num_row / (end - beg) * 8

class WriteProcess(Process):
	def run(self):
		# server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口  
		transport = TSocket.TSocket('localhost', 6666)  
		#transport = TSocket.TSocket('localhost', 6666)  
		  
		# 可以设置超时  
		#transport.setTimeout(8000)  
		# 设置传输方式（TFramedTransport或TBufferedTransport）  
		trans = TTransport.TBufferedTransport(transport)  
		# 设置传输协议  
		protocol = TBinaryProtocol.TBinaryProtocol(trans)  
		# 确定客户端  
		client = Hbase.Client(protocol)  
		# 打开连接  
		transport.open()  
		total = 0.
		for i in range(int(sys.argv[2])):
			key = str(random.randint(0, 10000))
			key = str(i)

			beg = time.time()
			put_one(key, client)	
			end = time.time()

			total += end - beg

		print 'total:', total
		print 'avg:', total / int(sys.argv[2])


def get_row(table, rowkey, client):
	timer = Timer()
	#timer.stage_begin('get row')
	#row = client.getRow(tableName,rowkey)  
	timer.stage_begin('get col')
	rowColumn = client.get(table, rowkey, "cf:content")  
	print len(rowColumn)
	if rowColumn:
		print 'content len', len(rowColumn[0].value)
	timer.finish()
	print timer.dump()

class ReadProcess(Process):
	def __init__(self):
		Process.__init__(self)
		#self.total = 0.0

	def run(self):
		# server端地址和端口,web是HMaster也就是thriftServer主机名,9090是thriftServer默认端口  
		transport = TSocket.TSocket('localhost', 6666)  
		  
		# 可以设置超时  
		transport.setTimeout(5000)  
		# 设置传输方式（TFramedTransport或TBufferedTransport）  
		trans = TTransport.TBufferedTransport(transport)  
		# 设置传输协议  
		protocol = TBinaryProtocol.TBinaryProtocol(trans)  
		# 确定客户端  
		client = Hbase.Client(protocol)  
		# 打开连接  
		transport.open()  
		total = 0.0
		for i in range(int(sys.argv[2])):
			key = str(i)
			key = str(random.randint(0, 1999))

			beg = time.time()
			get_row('users', key, client)	
			end = time.time()
			total += end - beg
		print 'total:', total
		print 'avg:', total / int(sys.argv[2])
	

def main():
	plist = []
	beg = time.time()
	num_task = int(sys.argv[1])
	num_row = int(sys.argv[2])
	for i in range(num_task):
		p = ReadProcess()
		p = WriteProcess()
		plist.append(p)
	
	for p in plist:
		p.start()

	for p in plist:
		p.join()
	total = 0.0
	end = time.time()

	print end - beg
	print (end - beg) / num_task / num_row
	print 'avg tps', num_task * num_row / (end - beg)
	print 1.06 * num_task * num_row / (end - beg) * 8
	
		


if __name__ == '__main__':
	main()
