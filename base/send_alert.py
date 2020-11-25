#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

def msg(title, message):
    send_msg_url = 'http://op-01.gzproduction.com:9527/api/msg/send'
    payload = {'title': title, 'message': message, 'to_party': '20'}
    ret = requests.get(send_msg_url, params=payload)
    return ret.json()['code'] == '0'

if __name__ == '__main__':
    print msg('client requests', 'client requests')
