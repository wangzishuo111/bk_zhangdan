#!/usr/bin/python
# -*- coding: utf-8 -*-

import hbase_util_thrift
import hbase_util_local

put_cols = hbase_util_thrift.put_cols
put_col = hbase_util_thrift.put_col
get = hbase_util_thrift.get
get_col = hbase_util_thrift.get_col
delete = hbase_util_thrift.delete

def switch2local():
    global put_cols # pylint: disable=W0122
    global put_col # pylint: disable=W0122
    global get # pylint: disable=W0122
    global get_col # pylint: disable=W0122
    global delete # pylint: disable=W0122
    put_cols = hbase_util_local.put_cols
    put_col = hbase_util_local.put_col
    get = hbase_util_local.get
    get_col = hbase_util_local.get_col
    delete = hbase_util_local.delete

def main():
    switch2local()

if __name__ == '__main__':
    main()

