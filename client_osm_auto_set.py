# -*- coding: utf-8 -*-

import requests

def main():
    jaguar_osm_auto_set_url = 'http://hadoop-0015:8888/jaguar/osm/auto/set'
    payload = {'project_id': '11', 'jaguar_auto_id': '123_125',
               'auto_osm_info': 'shen debug 123'}
    res = requests.post(jaguar_osm_auto_set_url, data=payload)
    if res.status_code != 200:
        return False
    try:
        jdata = res.json()
    except Exception, e: # pylint: disable=W0703
        import traceback
        traceback.print_exc(e)
        return False
    return jdata['code'] == '0'

if __name__ == '__main__':
    print main()
