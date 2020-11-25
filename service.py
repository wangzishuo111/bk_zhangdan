# -*- coding:utf-8 -*-
# pylint: disable=W0703

import json
import sys

import tornado.ioloop
import tornado.web
import tornado.httputil

from base.log import logger, log_init
from base.opt import DEFINE_FLAG, FLAG
from base import util

import base_http
import api
import kds
import cat_helper

DEFINE_FLAG("-p", "--port", dest="port", default=9527, help="service port")

def make_error_result(error, msg):
    logger().error(msg)
    ret = {}
    ret['code'] = error
    ret['message'] = msg
    ret['error'] = None
    ret['result'] = None
    return json.dumps(ret)

def make_success_result(result=None):
    ret = {}
    ret['code'] = '0'
    ret['message'] = '成功'
    ret['error'] = None
    ret['result'] = result
    return json.dumps(ret)

class TestHandler(base_http.BaseHandler):
    def process(self):
        #logger().info("id:%s", id(self))
        data = self.request.body
        print data
        print '===='
        for f, c in self.request.files.items():
            print f, c[0]['filename'], c[0]['body']

        for f, c in self.request.files.items():
            print f, c


class TouchHandler(base_http.BaseHandler):
    def process(self):
        self.write('kick me baby')

class ImageUploadInternalHandler(base_http.BaseHandler):
    def process(self):

        task_seq = self.get_arg_ex('taskSeq', '!!!')
        batch = self.get_arg_ex('batch', '!!!')
        arg_override = self.get_arg_ex('override', 'false')
        if arg_override == 'false': # pylint: disable=W0122
            override = False
        elif arg_override == 'true': # pylint: disable=W0122
            override = True
        else:
            self.set_json_result(make_error_result(
                                2, 'invalid param override[%s]' % arg_override))
            return

        logger().info('override:%s', override)
        try:
            track_id = self.get_arg('trackId')
            tpid = self.get_arg('trackPointId')
            tpe = self.get_arg('type')
            seq = self.get_arg('seq')
            image_type = self.get_arg('imageType')
        except Exception, e: # pylint: disable=W0703
            #import traceback
            #print traceback.format_exc()
            logger().error('bad args, query[%s]', self.request.query)
            self.set_json_result(make_error_result(2, str(e)))
            return
        cat_helper.agent_stage_begin(
            'Api:ImageUploadInternal', '%s:%s' % (tpe, seq))

        content = self.request.body
        if tpe != '00' or seq != '004':
            if not content:
                logger().error('type[%s], seq[%s], post body empty', tpe, seq)
                self.set_json_result(make_error_result(2, 'post body empty'))
                return

        logger().info('upload %s_%s_%s.%s, task_seq[%s], batch[%s]', \
                tpid, tpe, seq, image_type, task_seq, batch)

        if not util.filetype_valid(image_type):
            logger().error('invalid image_type[%s]', image_type)
            self.set_json_result(make_error_result(
                                105, 'bad image_type[%s]' % image_type))
            return

        file_name = '%s_%s_%s.%s' % (tpid, tpe, seq, image_type)
        logger().info('override:%d', override)
        if not override:
            exist = api.image_exist(
                track_id, tpid, tpe, seq, image_type, batch)
            logger().info('override:%d, exist[%d]', override, exist)
            if exist:
                logger().error('文件已存在[%s]', file_name)
                self.set_json_result(make_error_result(107, '数据已存在'))
                return

        res_data = api.image_upload(track_id, tpid, tpe, seq, \
            image_type, content, task_seq, batch)
        if not res_data:
            self.set_json_result(make_error_result(3, '方法错误'))
            logger().error('failed to upload image[%s] to track[%s]',
                            file_name, track_id)
            return

        else:
            logger().info('success to upload image[%s] to track[%s]',
                            file_name, track_id)

        self.set_json_result(make_success_result())

class ImageUploadHandler(base_http.BaseHandler):
    def get_file_list(self):
        ret = []
        for f, c in self.request.files.items():
            ret.append([c[0]['filename'], c[0]['body']])
        return ret

    def process(self):
        stage = self.get_arg_ex('stageId', 'Other')
        try:
            track_id = self.get_arg('trackId')
        except Exception, e: # pylint: disable=W0703
            logger().error('bag args, query[%s]', self.request.query)
            self.set_json_result(make_error_result(2, str(e)))
            return
        batch = self.get_arg_ex('batch', '!!!')
        task_seq = self.get_arg_ex('taskSeq', '!!!')
        arg_override = self.get_arg_ex('override', 'false')
        if arg_override == 'false':
            override = False
        elif arg_override == 'true':
            override = True
        else:
            self.set_json_result(
                make_error_result(2, 'invalid param override[%s]' % override))
        logger().info('override:%d', override)

        file_list = self.get_file_list()
        if not file_list:
            self.set_json_result(
                make_error_result(2, 'invalid param file[empty]'))
            return

        for file_name, file_content in file_list:
            ret = util.separate_file_name(file_name)
            if not ret:
                continue

            tpid, tpe, seq, image_type = ret
            logger().info('upload %s_%s_%s.%s, task_seq[%s], batch[%s]',
                          tpid, tpe, seq, image_type, task_seq, batch)
            cat_helper.agent_stage_begin(
                    'Api:ImageUpload', '%s:%s' % (tpe, seq))
            if not util.filetype_valid(image_type):
                logger().error('invalid image_type[%s]', image_type)
                self.set_json_result(
                    make_error_result(105, 'bad image type[%s]' % image_type))
                return

            if not override:
                exist = api.image_exist(
                        track_id, tpid, tpe, seq, image_type, batch)
                logger().info('exist:%d', exist)
                if exist:
                    logger().error('文件已存在')
                    self.set_json_result(make_error_result(107, '数据已存在'))
                    return

            res_data = api.image_upload(track_id, tpid, tpe, seq, \
                image_type, file_content, task_seq, batch)
            if not res_data:
                self.set_json_result(make_error_result(3, '方法错误'))
                logger().error('failed to upload image[%s] to track[%s]',
                               file_name, track_id)
                return
            else:
                logger().info('success to upload image[%s] to track[%s]',
                              file_name, track_id)

        self.set_json_result(make_success_result())

class ImageGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
            tpid = self.get_arg('trackPointId')
            tpe = self.get_arg('type')
            seq = self.get_arg('seq')
            image_type = self.get_arg('imageType')
        except Exception, e: # pylint: disable=W0703
            logger().error('bad args, query[%s]', self.request.query)
            self.set_json_result(make_error_result(2, str(e)))
            return

        cat_helper.agent_stage_begin('Api:ImageGet', '%s:%s' % (tpe, seq))

        if not util.filetype_valid(image_type):
            logger().error('invalid image_type[%s]', image_type)
            self.set_json_result(
                make_error_result(105, 'bad image type[%s]' % image_type))
            return

        batch = self.get_arg_ex('batch', None)

        res_data = api.image_get(track_id, tpid, tpe, seq, image_type, batch)
        if not res_data:
            self.set_json_result(make_error_result(105, '数据不存在'))
            logger().error('failed to get image[%s] to track[%s]',
                           tpid, track_id)
            return

        self.write(res_data)
        logger().info('res_data len:%d', len(res_data))
        self.set_header('Content-type', util.filetype2contenttype(image_type))
        self.set_header('Content-length', len(res_data))
        self.flush()
        logger().info('success to get image[%s] of track[%s]', tpid, track_id)

class ImageExistHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
            tpid = self.get_arg('trackPointId')
            tpe = self.get_arg('type')
            seq = self.get_arg('seq')
            image_type = self.get_arg('imageType')
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(2, str(e)))
            return
        cat_helper.agent_stage_begin('Api:ImageExist', '%s:%s' % (tpe, seq))
        batch = self.get_arg_ex('batch', '')
        ret = api.image_exist(track_id, tpid, tpe, seq, image_type, batch)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class ImageStatHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
            tpe = self.get_arg('type')
            seq = self.get_arg('seq')

            image_type = self.get_arg('imageType')
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(2, str(e)))
            return
        seq = seq.split(',')
        ret = api.image_stat(track_id, tpe, seq, image_type)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class KdsBatchSaveHandler(base_http.BaseHandler):
    def process(self):
        logger().info('path:%s', self.request.path)
        try:
            batch = self.get_arg('batch')
            task_seq = self.get_arg('seq')
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(2, str(e)))
            return

        if not batch:
            self.set_json_result(make_error_result(2, 'batch empty'))
            return

        data = self.request.body
        if not data:
            self.set_json_result(make_error_result(2, 'post body empty'))
            return

        try:
            json.loads(data)
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(2, 'post body isnot json'))
            return

        if not kds.kds_save(batch, data):
            logger().info('kds save failed for batch[%s], datalen[%d]',
                          batch, len(data))
            self.set_json_result(make_error_result(3, 'kds save failed'))
            return
        self.set_json_result(make_success_result())

class KdsBatchGetHandler(base_http.BaseHandler):
    def process(self):
        path = self.request.path
        batch = path.split('/')[-3]
        if not batch:
            self.set_json_result(make_error_result(2, 'batch empty'))
            return
        data = kds.kds_get(batch)
        try:
            jdata = json.loads(data)
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(3, 'invalid json data'))
            return

        if not data:
            self.set_json_result(
                make_error_result(3, 'no data for batch[%s] found' % batch))

        self.set_json_result(make_success_result(jdata))

class KdsSeqDelHandler(base_http.BaseHandler):
    def process(self):
        path = self.request.path
        seq = path.split('/')[-3]
        if not seq:
            self.set_json_result(make_error_result(2, 'batch seq'))
            return

        seq_segs = seq.split('-')
        if len(seq_segs) < 2:
            self.set_json_result(make_error_result(2, 'bad seq[%s]' % seq))
            return
        batch = seq_segs[0]
        logger().info('del seq[%s], batch[%s]', seq, batch)

        if not kds.kds_del(batch):
            self.set_json_result(
                make_error_result(3, 'kds deleta failed for seq[%s]' % seq))

        self.set_json_result(make_success_result())

class TrackLocationGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(2, str(e)))
            return
        ret = api.track_location_get(track_id)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class TrackLocationSetHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
            location = self.get_arg('location')
        except Exception, e: # pylint: disable=W0703
            self.set_json_result(make_error_result(2, str(e)))
            return
        ret = api.track_location_set(track_id, location)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class TrackBatchSetHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
            batch = self.get_arg('batch')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        ret = api.track_batch_set(track_id, batch)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class TrackBatchGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            track_id = self.get_arg('trackId')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        ret = api.track_batch_get(track_id)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class JaguarTrackExtendSetHandler(base_http.BaseHandler):
    def process(self):
        try:
            jaguar_id = self.get_arg('jaguar_id')
            track_extend = self.get_arg('track_extend')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('jaguar_id[%s] track_extend[%s]',
                      jaguar_id, track_extend[:10])
        ret = api.jaguar_track_extend_set(jaguar_id, track_extend)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class JaguarTrackExtendGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            jaguar_id = self.get_arg('jaguar_id')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('jaguar_id[%s]', jaguar_id)
        ret = api.jaguar_track_extend_get(jaguar_id)
        if ret:
            j_ret = json.loads(ret)
        else:
            j_ret = ret
        res = make_success_result(j_ret)
        self.set_json_result(res)
        self.flush()

class JaguarIdMapSetHandler(base_http.BaseHandler):
    def process(self):
        try:
            kts_id = self.get_arg('kts_id')
            jaguar_id = self.get_arg('jaguar_id')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('set kts_id[%s] -> jaguar_id[%s]', kts_id, jaguar_id)
        ret = api.jaguar_idmap_set(kts_id, jaguar_id)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class JaguarIdMapGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            kts_id = self.get_arg('kts_id')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('kts_id[%s]', kts_id)
        ret = api.jaguar_idmap_get(kts_id)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class JaguarOsmAutoSetHandler(base_http.BaseHandler):
    def process(self):
        try:
            jaguar_id = self.get_arg('jaguar_id')
            auto_osm_info = self.get_arg('auto_osm_info')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('jaguar_id[%s], auto_osm_info[%s]',
                      jaguar_id, auto_osm_info[:10])
        ret = api.jaguar_osm_auto_set(jaguar_id,
                                      auto_osm_info)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class JaguarOsmAutoGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            jaguar_id = self.get_arg('jaguar_id')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('jaguar_id[%s]', jaguar_id)
        ret = api.jaguar_osm_auto_get(jaguar_id)
        if ret:
            j_ret = json.loads(ret)
        else:
            j_ret = ret
        res = make_success_result(j_ret)
        self.set_json_result(res)
        self.flush()

class JaguarOsmFusionSetHandler(base_http.BaseHandler):
    def process(self):
        try:
            jaguar_id = self.get_arg('jaguar_id')
            fusion_osm_info = self.get_arg('fusion_osm_info')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('jaguar_id[%s], fusion_osm_info[%s]',
                      jaguar_id, fusion_osm_info[:10])
        ret = api.jaguar_osm_fusion_set(jaguar_id,
                                        fusion_osm_info)
        res = make_success_result(ret)
        self.set_json_result(res)
        self.flush()

class JaguarOsmFusionGetHandler(base_http.BaseHandler):
    def process(self):
        try:
            jaguar_id = self.get_arg('jaguar_id')
        except Exception, e:
            self.set_json_result(make_error_result(2, str(e)))
            return
        logger().info('jaguar_id[%s]', jaguar_id)
        ret = api.jaguar_osm_fusion_get(jaguar_id)
        if ret:
            j_ret = json.loads(ret)
        else:
            j_ret = ret
        res = make_success_result(j_ret)
        self.set_json_result(res)
        self.flush()

app = tornado.web.Application(
    [
        (r"/test", TestHandler),
        (r"/touch", TouchHandler),

        (r"/image/upload/internal", ImageUploadInternalHandler),
        (r"/image/upload", ImageUploadHandler),
        (r"/image/get", ImageGetHandler),
        (r"/image/exist", ImageExistHandler),
        (r"/image/clear", TestHandler),

        (r"/image/stat", ImageStatHandler),
        (r"/track/location/get", TrackLocationGetHandler),
        (r"/track/location/set", TrackLocationSetHandler),

        (r"/track/batch/get", TrackBatchGetHandler),
        (r"/track/batch/set", TrackBatchSetHandler),

        (r"/kds/data/task/.+/auto/edit", KdsBatchSaveHandler),
        (r"/kds/data/batch/.+/auto/query", KdsBatchGetHandler),
        (r"/kds/data/seq/.+/auto/delete", KdsSeqDelHandler),

        (r"/jaguar/idmap/get", JaguarIdMapGetHandler),
        (r"/jaguar/idmap/set", JaguarIdMapSetHandler),

        (r"/jaguar/track_extend/get", JaguarTrackExtendGetHandler),
        (r"/jaguar/track_extend/set", JaguarTrackExtendSetHandler),
        (r"/jaguar/osm/auto/get", JaguarOsmAutoGetHandler),
        (r"/jaguar/osm/auto/set", JaguarOsmAutoSetHandler),
        (r"/jaguar/osm/fusion/get", JaguarOsmFusionGetHandler),
        (r"/jaguar/osm/fusion/set", JaguarOsmFusionSetHandler),
    ]
)

if __name__ == "__main__":
    if not FLAG().env:
        logger().error('--env must be set')
        sys.exit(1)

    log_init('info', 'logs/service_%s.txt' % FLAG().env, quiet=False)
    sockets = tornado.netutil.bind_sockets(int(FLAG().port))
    tornado.process.fork_processes(100)
    server = tornado.httpserver.HTTPServer(app)
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.instance().start()

