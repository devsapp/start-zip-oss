# -*- coding: utf-8 -*-

import json
import time
import logging
import zipfile
import oss2
from oss2.models import PartInfo
from task_queue import TaskQueue
from helper import MemBuffer
from helper import StreamZipFile
import base64

LOG = logging.getLogger()

# FC handler
# event: the config, see event.json for example
# return: the signed url of the zip file


def main_handler(event, context):
    event = json.loads(event)
    LOG.info("receive event: {}".format(event))

    body = ""
    # get http request body
    if "body" in event:
        body = event["body"]
        if event["isBase64Encoded"]:
            body = base64.b64decode(body).decode("utf-8")
    LOG.info("receive http body: {}".format(body))

    evt = json.loads(body)

    oss_client = get_oss_client(evt, context)
    ret = _main_handler(oss_client, evt, context)

    return {
        "statusCode": 302,
        "headers": {"Location": sign_url(oss_client, ret)},
        "body": body,
    }


def _main_handler(oss_client, evt, context):
    source_dir = evt.get("source-dir")
    source_files = evt.get("source-files")
    dest_file = evt.get("dest-file")
    if not dest_file:
        dest_file = "output/" + context.requestId + ".zip"

    return zip_files(oss_client, source_dir, source_files, dest_file)


def get_oss_client(evt, context):
    bucket = evt["bucket"]
    creds = context.credentials
    auth = oss2.StsAuth(creds.accessKeyId, creds.accessKeySecret, creds.securityToken)
    oss_client = oss2.Bucket(
        auth, "oss-" + context.region + "-internal.aliyuncs.com", bucket
    )
    return oss_client


def sign_url(oss_client, key, content_type=""):
    LOG.info("sign url, key: %s, content type: %s", key, content_type)

    url = oss_client.sign_url("GET", key, 300, headers={"Content-Type": content_type})
    return url.replace("-internal", "")


# add the source files into the zip file
# streaming: the source files are added in stream, no local files are needed
# concurrency: the output zip file is buffer in memory, it buffers at
# most 8MB data and uploads to OSS concurrently by 8 threads


def zip_files(oss_client, source_dir, source_files, dest_file):
    LOG.info(
        "create zip, source_dir: %s, source_files: %s, dest_file: %s",
        source_dir,
        source_files,
        dest_file,
    )

    start_time = time.time()
    upload_id = oss_client.init_multipart_upload(dest_file).upload_id

    def zip_add_file(zip_file, key, dir):
        if dir is None:
            dir = ""
        new_key = key.replace(dir, "", 1)
        LOG.info("add zip file key: %s, zip_key: %s", key, new_key)
        if key[-1] == "/":  # filter dir
            return
        obj = oss_client.get_object(key)
        zip_file.write_file(new_key, obj, compress_type=zipfile.ZIP_STORED)

    def producer(queue):
        mem_buf = MemBuffer(queue)
        with StreamZipFile(mem_buf, "w") as zip_file:
            if isinstance(source_files, list):
                for file in source_files:
                    zip_add_file(zip_file, file, None)
            elif isinstance(source_dir, str):
                for obj in oss2.ObjectIterator(oss_client, prefix=source_dir):
                    zip_add_file(zip_file, obj.key, source_dir)
            else:
                raise Exception(
                    "either `source_files` or `source_dir` must be speicified"
                )

    parts = []

    def consumer(queue):
        while queue.ok():
            item = queue.get()
            if item is None:
                break

            part_no, part_data = item
            res = oss_client.upload_part(dest_file, upload_id, part_no, part_data)
            parts.append(PartInfo(part_no, res.etag))

    task_q = TaskQueue(producer, [consumer] * 16)
    task_q.run()

    oss_client.complete_multipart_upload(dest_file, upload_id, parts)
    end_time = time.time()
    LOG.info("create zip, cost: %s secs", end_time - start_time)

    return dest_file


if __name__ == "__main__":
    pass
