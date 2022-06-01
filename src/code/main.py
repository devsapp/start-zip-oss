# -*- coding: utf-8 -*-

import json
import time
import base64
import logging
import zipfile
import oss2
from oss2.models import PartInfo
from task_queue import TaskQueue
from helper import MemBuffer
from helper import StreamZipFile

LOG = logging.getLogger()

# FC handler
# event: the config, see event.json for example
# return: the signed url of the zip file


def main_handler(environ, start_response):
    LOG.info('event: %s', environ)
    try:
        request_body_size = int(environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        request_body_size = 0
    request_body = environ['wsgi.input'].read(request_body_size)

    evt = json.loads(request_body)
    context = environ['fc.context']

    oss_client = get_oss_client(evt, context)
    ret = _main_handler(oss_client, evt, context)

    status = '302 FOUND'
    response_headers = [('Location', sign_url(oss_client, ret))]
    start_response(status, response_headers)

    return "ok"


def _main_handler(oss_client, evt, context):
    source_dir = evt.has_key('source-dir') and evt['source-dir']
    source_files = evt.has_key('source-files') and evt['source-files']
    dest_file = evt.has_key('dest-file') and evt['dest-file']
    if not dest_file:
        dest_file = 'output/' + context.requestId + '.zip'

    return zip_files(oss_client, source_dir, source_files, dest_file)


def get_oss_client(evt, context):
    bucket = evt['bucket']
    creds = context.credentials
    auth = oss2.StsAuth(creds.accessKeyId,
                        creds.accessKeySecret, creds.securityToken)
    oss_client = oss2.Bucket(auth, 'oss-' + context.region +
                             '-internal.aliyuncs.com', bucket)
    return oss_client


def sign_url(oss_client, key, content_type=''):
    LOG.info('sign url, key: %s, content type: %s', key, content_type)

    url = oss_client.sign_url('GET', key, 300, headers={
        'Content-Type': content_type
    })
    return url.replace('-internal', '')

# add the source files into the zip file
# streaming: the source files are added in stream, no local files are needed
# concurrency: the output zip file is buffer in memory, it buffers at
# most 8MB data and uploads to OSS concurrently by 8 threads


def zip_files(oss_client, source_dir, source_files, dest_file):
    LOG.info('create zip, source_dir: %s, source_files: %s, dest_file: %s',
             source_dir, source_files, dest_file)

    start_time = time.time()
    upload_id = oss_client.init_multipart_upload(dest_file).upload_id

    def zip_add_file(zip_file, key):
        LOG.info('add zip file: %s', key)
        if key[-1] == '/':
            return
        obj = oss_client.get_object(key)
        zip_file.write_file(key[len(source_dir):], obj,
                            compress_type=zipfile.ZIP_STORED)

    def producer(queue):
        mem_buf = MemBuffer(queue)
        zip_file = StreamZipFile(mem_buf, 'w')

        if isinstance(source_files, list):
            for obj in source_files:
                zip_add_file(zip_file, obj)
        elif isinstance(source_dir, basestring):
            for obj in oss2.ObjectIterator(oss_client, prefix=source_dir):
                zip_add_file(zip_file, obj.key)
        else:
            raise Exception(
                'either `source_files` or `source_dir` must be speicified')

        zip_file.close()
        mem_buf.flush_buffer()

    parts = []

    def consumer(queue):
        while queue.ok():
            item = queue.get()
            if item is None:
                break

            part_no, part_data = item
            res = oss_client.upload_part(
                dest_file, upload_id, part_no, part_data)
            parts.append(PartInfo(part_no, res.etag))

    task_q = TaskQueue(producer, [consumer] * 16)
    task_q.run()

    oss_client.complete_multipart_upload(dest_file, upload_id, parts)
    end_time = time.time()
    LOG.info('create zip, cost: %s secs', end_time-start_time)

    return dest_file


if __name__ == '__main__':
    pass
