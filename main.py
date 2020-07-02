#!/usr/bin/env python3.6
# encoding: utf-8

import math
import io
import os
import sys
import threading
import Queue
import time
import boto3
from io import BytesIO
from os import path
from filechunkio import FileChunkIO

class Chunk:
    num = 0
    offset = 0
    len = 0
    def __init__(self, n, o, l):
        self.num = n
        self.offset = o
        self.len = l

class S3(object):
    def __init__(self, bucket):
        self.session = boto3.Session(
            aws_access_key_id=COS_SERVER_PUBLIC_KEY,
            aws_secret_access_key=COS_SERVER_SECRET_KEY,
            #region_name=COS_ENDPOINT,
        )
        #self.client = boto3.client('s3', endpoint_url='https://cos.ap-guangzhou.myqcloud.com')
        self.s3_source = self.session.resource('s3')
        self.s3_client = self.session.client('s3', endpoint_url='https://cos.ap-guangzhou.myqcloud.com')
        self.bucket_name = bucket
        self.bucket = self.s3_source.Bucket(bucket)


    def upload(self, filename):
        data = open(filename, 'rb')
        file_name = filename.split('/')[-1]
        self.bucket.put_object(Key=file_name, Body=data)


    def down_data(self, object_name, filename):
        with open(filename, 'wb') as f:
            self.s3_client.download_fileobj(self.bucket_name, object_name, f)


def init_queue(filesize):
    chunkcnt = int(math.ceil(filesize*1.0/chunksize))
    q = Queue.Queue(maxsize = chunkcnt)
    for i in range(0,chunkcnt):
        offset = chunksize*i
        len = min(chunksize, filesize-offset)
        c = Chunk(i+1, offset, len)
        q.put(c)
    return q

def upload_chunk(filepath, mp, q, id):
    while (not q.empty()):
        chunk = q.get()
        fp = FileChunkIO(filepath, 'r', offset=chunk.offset, bytes=chunk.len)
        mp.upload_part_from_file(fp, part_num=chunk.num)
        fp.close()
        q.task_done()

def upload_file_multipart(s3, filepath, keyname, threadcnt=8):
    filesize = os.stat(filepath).st_size
    mp = s3.bucket.initiate_multipart_upload(keyname)
    q = init_queue(filesize)
    for i in range(0, threadcnt):
        t = threading.Thread(target=upload_chunk, args=(filepath, mp, q, i))
        t.setDaemon(True)
        t.start()
    q.join()
    mp.complete_upload()

def download_chunk(filepath, bucket, key, q, id):
    while (not q.empty()):
        chunk = q.get()
        offset = chunk.offset
        len = chunk.len
        resp = s3.bucket.connection.make_request("GET", s3.bucket.name, key.name, headers={"Range":"bytes=%d-%d" % (offset, offset+len)})
        data = resp.read(len)
        fp = FileChunkIO(filepath, 'r+', offset=offset, bytes=len)
        fp.write(data)
        fp.close()
        q.task_done()

def download_file_multipart(s3, key, filepath, threadcnt=8):
    if type(key) == str:
        key=s3.bucket.get_key(key)
    filesize=key.size
    if os.path.exists(filepath):
        os.remove(filepath)
    os.mknod(filepath)
    q = init_queue(filesize)
    for i in range(0, threadcnt):
        t = threading.Thread(target=download_chunk, args=(filepath, s3.bucket, key, q, i))
        t.setDaemon(True)
        t.start()
    q.join()

def move_big_file(s3, old_file_path, new_file_path, keyname, threadcnt):
    time1= time.time()
    upload_file_multipart(s3, old_file_path, keyname, threadcnt)
    time2= time.time()
    print("upload {keyname} with {tcnt} threads use {time} seconds".format(keyname=keyname, tcnt=threadcnt, time=time2-time1))
    key = s3.bucket.get_key(keyname)

    down_time1= time.time()
    download_file_multipart(s3, key, new_file_path, threadcnt)
    down_time2= time.time()
    print("download {keyname} with {tcnt} threads use {time} seconds".format(keyname=keyname, tcnt=threadcnt, time=down_time2-down_time1))

if __name__ == '__main__':

    chunksize = 8 << 20

    cos_access_key = "test"
    cos_secret_key = "123456"
    bucket = "test"
    host = "*****"

    old_filepath = "/old_path/big.file"
    new_filepath = "/new_path/big.file"
    keyname = "big.file"

    threadcnt = 8
    s3 = S3(bucket)

    move_big_file(s3, old_filepath, new_filepath, keyname, threadcnt)




