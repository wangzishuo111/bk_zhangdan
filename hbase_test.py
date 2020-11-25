#!/usr/bin/python
# -*- coding: utf-8 -*-

import hbase_util

from nose.tools import assert_equal, assert_true

def setUp():
    hbase_util.switch2local()
    import hbase_util_local
    hbase_util_local.clear()
    from base import opt
    opt.set_opt_local()

def test():
    assert_equal(None, hbase_util.get_col('test', 'row1', 'fam1', 'col1'))
    assert_true(hbase_util.put_col('test', 'row1', 'fam1', 'col1', 'content1'))
    assert_equal({'fam1:col1':'content1'},
                hbase_util.get_col('test', 'row1', 'fam1', 'col1'))
    assert_true(hbase_util.delete('test', 'row1'))
    assert_equal(None, hbase_util.get_col('test', 'row1', 'fam1', 'col1'))
    assert_true(hbase_util.delete('test', 'row1'))

    assert_true(hbase_util.put_cols('test', 'row1',  \
        [('fam1', 'col1', 'content1'), ('fam2', 'col2', 'content2')]))
    assert_equal({'fam1:col1': 'content1'},
                 hbase_util.get_col('test', 'row1', 'fam1', 'col1'))
    assert_equal({'fam2:col2': 'content2'},
                 hbase_util.get_col('test', 'row1', 'fam2', 'col2'))


