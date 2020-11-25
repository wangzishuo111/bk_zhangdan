#!/usr/bin/python
# -*- coding: utf-8 -*-

import happybase

from config import GET_CONF
from base.log import logger

__thrift_host = GET_CONF('hbase_thrift', 'host')
__thrift_port = int(GET_CONF('hbase_thrift', 'port'))

thrift_conn = None
retry_times = 3

def get_thrift_conn():
    global thrift_conn # pylint: disable=W0603
    if not thrift_conn:
        thrift_conn = happybase.Connection(
            __thrift_host, __thrift_port, timeout=10*1000)
    return thrift_conn

def thrift_reconn():
    global thrift_conn # pylint: disable=W0603
    if thrift_conn:
        thrift_conn.close()
    thrift_conn = happybase.Connection(
            __thrift_host, __thrift_port, timeout=10*1000)


def _put_cols(table, row, cols): #(column_family, column, data)
    col_dict = {}
    for (column_family, column, content) in cols:
        cf = '%s:%s' % (column_family, column)
        col_dict[cf] = content

    conn = get_thrift_conn()
    table = conn.table(table)

    table.put(row, col_dict, wal=True)
    return True

def put_cols(table, row, cols): #(column_family, column, data)
    succ = False
    for i in range(retry_times):
        try:
            ret = _put_cols(table, row, cols)
            succ = True
            break
        except Exception, e: # pylint: disable=W0703
            logger().error('exception:[%s]', str(e))
            thrift_reconn()
    if not succ:
        logger().info('put col failed %d times, table[%s], rowkey[%s]',
                      retry_times, table, row)
    return succ

def _put_col(table, row, column_family, column, content):
    cf = '%s:%s' % (column_family, column)
    conn = get_thrift_conn()
    table = conn.table(table)
    table.put(row, {cf : content}, wal=True)
    return True

def put_col(table, row, column_family, column, content):
    succ = False
    for i in range(retry_times):
        try:
            ret = _put_col(table, row, column_family, column, content)
            succ = True
            break
        except Exception, e:
            logger().error('exception:[%s]', str(e))
            thrift_reconn()
    if not succ:
        logger().info('put col failed %d times, rowkey[%s]', retry_times, row)
    return succ

def get_regions(table):
    conn = get_thrift_conn()
    table = conn.table(table)
    return table.regions()

def _get(table, row, column_family, column):
    conn = get_thrift_conn()
    table = conn.table(table)
    if column_family and column:
        columns = []
        columns.append('%s:%s' % (column_family, column))
        return table.row(row, columns=columns)
    elif column_family:
        columns = []
        columns.append('%s' % (column_family))
        return table.row(row, columns=columns)

    else:
        return table.row(row)

def get(table, row, column_family=None, column=None):
    succ = False
    for i in range(retry_times):
        try:
            ret = _get(table, row, column_family, column)
            succ = True
            break
        except Exception, e: # pylint: disable=W0703
            logger().error('exception:[%s]', str(e))
            thrift_reconn()
    if not succ:
        logger().info('hbase get failed %d times, rowkey[%s]',
                      retry_times, row)
        return None
    return ret

def get_col(table, row, column_family, column):
    return get(table, row, column_family, column)

def delete(table, row, columns = None):
    conn = get_thrift_conn()
    table = conn.table(table)
    if columns:
        table.delete(row, columns=columns)
    else:
        table.delete(row)
    return True

def test():
#    print get_col('file_table_prd_v2', '87-1234567890--11_22_70_seq_png', 'content', '!!!')
#    return
#    print put_cols(
#        'users', 'wang', [('cf', 'sex', 'female'), ('cf', 'tag', '1')])
#    print put_cols(
#        'users', 'tang', [('cf', 'sex', 'male'), ('cf', 'tag', '2')])
#    print put_cols(
#        'users', 'zhang', [('cf', 'sex', 'female'), ('cf', 'tag', '2')])
#    print get('users', 'cui')
#    print get('users', 'tang')
    d1 = get('file_table_prd_v2', '10-200754475_20190124115133738-200754475_20190124115310494140_09_000_png', 'content')
    for key in d1.keys():
        print key
#    print get('users', 'cui', 'cf', 'sex')
#    print get_col('users', 'cui', 'cf', 'sex')

#def main():
#    import rowkey
#    import sys
#    for line in sys.stdin:
#        track_id, tpid = line.strip().split(',')
#        file_path = 'wangjing/data/%s-%s.jpg' % track_id, tpid
#        data = open(file_path).read()
#        row = rowkey.gen(track_id, tpid, '00', '004', 'jpg')
#        #print row
#        data = put_col('file_table_prd_v2', row, 'content', '!!!', 'data')
#        print len(data)

if __name__ == '__main__':
    test()
