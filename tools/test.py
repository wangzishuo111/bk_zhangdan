#/usr/bin/python                                                                                                                                                      
# -*- coding: utf-8 -*-

import sys
import os
from subprocess import *

pipe_in , pipe_out = os.popen2("hadoop jar  HDFS004Store3.jar kd.mapreduce.Hdfs004Store", "wr"); 
print type(pipe_in)
print type(pipe_out)

while True:
	s = raw_input()
	
	pipe_in.write(s + '\n')
	pipe_in.flush()
	print 'after flush'
	 
	#userid = pipe_out.readline() #读入结果
	print 'after readline'
	
	#print 'output:', userid, 
