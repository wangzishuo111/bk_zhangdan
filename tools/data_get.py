#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import base64

from base.log import *
import base.opt as opt
from config import *

import hbase_util
import index

import api


def main():
	'''
	print create_track('test')
	print write_image('test', '1', '1')
	print read_image('test', '1')

	print write_label('test', '1', '2')
	print read_label('test', '1')

	print read_image('123', '1')
	'''

	#hbase_util.create_table('file_big_table', ['info', 'data'])

	print 'upload:'
	print image_upload('test', '1', '00', '000', 'jpg', 'data1', task_seq = '1', batch = '1')
	print 'exist:'
	print image_exist('test', '1', '00', '000', 'jpg', batch = '1')
	print image_exist('test', '1', '00', '000', 'jpg', batch = '2')
	print 'get:'
	print image_get('test', '1', '00', '000', 'jpg', batch = '1')

	print 'get: batch 2'
	print image_get('test', '1', '00', '000', 'jpg', batch = '2')



if __name__ == '__main__':
	main()
