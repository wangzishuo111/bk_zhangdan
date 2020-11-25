#!/usr/bin/python
# -*- coding: utf-8 -*-

from base.opt import DEFINE_FLAG

DEFINE_FLAG("-e", "--env", dest="env", default='dev', help="hbase env")
DEFINE_FLAG("-t", "--test", dest="test", default=False, action='store_true')

