# -*- coding:utf-8 -*-

import sys
import json
import tornado.ioloop
import tornado.web

from base.log import logger
from base.opt import DEFINE_FLAG, FLAG
import cat_helper

DEFINE_FLAG("-g", "--get", action="store_true",
            dest="get", default=False, help="Whether to support get methord")

def make_error_result(msg):
    ret = {}
    ret['status'] = 'fail'
    ret['msg'] = msg
    return json.dumps(ret)

def make_success_result(k=None, v=None):
    ret = {}
    ret['status'] = 'success'
    if k:
        ret[k] = v
    return json.dumps(ret)

class BaseHandler(tornado.web.RequestHandler):
    def set_json_result(self, data):
        self.write(data)
        self.set_header('Content-type', 'application/json')

    def process(self):
        logger().error('not implement')
        sys.exit(-1)

    def get(self):
        cat_helper.agent_reset()
        try:
            if FLAG().get:
                return self.process()
            else:
                msg = 'get method not supported'
                logger().error(msg)
                self.write(msg)
                self.set_status(404)
        finally:
            cat_helper.agent_finish()

    def post(self):
        cat_helper.agent_reset()
        try:
            self.process()
        finally:
            cat_helper.agent_finish()

    def get_arg(self, key):
        return self.get_argument(key).encode('utf-8')

    def get_arg_ex(self, key, dft_val):
        try:
            val = self.get_argument(key)
            if val.strip() == 'null':
                val = None
            if not val:
                return dft_val
            if type(val) == unicode: # pylint: disable=W0123
                val = val.encode('utf-8')
            return val
        except: # pylint: disable=W0702
            return dft_val

if __name__ == '__main__':
    main()
