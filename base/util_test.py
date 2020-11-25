# -*- coding:utf-8 -*-

from util import ceil
from util import human_info
from util import get_val_ex
from util import json_read, json_write
from util import get_md5
from util import get_suffix
from util import transcode
from util import separate_file_name
from util import filetype_valid
from util import filetype2contenttype

from nose.tools import assert_equal, assert_true, assert_false

def test_ceil():
    assert_equal(ceil(2.5), 3)

def test_human_info():
    old_data = {'1': '2'}
    jdata = human_info(old_data)

def test_get_val_ex():
    data = {}
    data['key'] = 'value'
    assert_equal('dft_value', get_val_ex(data, 'bad_key', 'dft_value'))
    assert_equal('value', get_val_ex(data, 'key', 'dft_value'))

def test_json():
    old_data = {'key': [1, 2, 3]}
    jdata = json_write(old_data)
    new_data = json_read(jdata)
    assert_equal(old_data, new_data)

def test_get_md5():
    assert_equal('c4ca4238a0b923820dcc509a6f75849b', get_md5('1'))

def test_get_suffix():
    assert_equal('.jpg', get_suffix('/a/b/c.jpg'))
    assert_equal('.png', get_suffix('/home/hadoop/123.png'))

def test_transcode():
    ustr = u'我们'
    s = transcode(ustr)

def test_separate_file_name():
    correct_result = ('129_20170621132818068', '02', '000', 'yml.gz')
    result = separate_file_name('129_20170621132818068_02_000.yml.gz')
    assert_equal(correct_result, result)

def test_filetype_valid():
    assert_true(filetype_valid('png'))
    assert_false(filetype_valid('xml'))

def test_filetype2contenttype():
    assert_equal('image/png', filetype2contenttype('png'))
    assert_equal(None, filetype2contenttype('xml'))
