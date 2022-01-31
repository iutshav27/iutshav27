import os
import boto3
from boto3.s3.transfer import TransferConfig
import sys
import threading

os.environ.setdefault('AWS_PROFILE', 'aws_profile')

s3_client = boto3.client('s3')

S3_BUCKET = 'original-buck'
FILE_PATH = '/home/futurense/Downloads/'
KEY_PATH = 'raw_data_proj/'  # its the name of the folder creating inside the aws bucket

class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

def uploadFiles(filename):
    config = TransferConfig(multipart_threshold=1024*50, max_concurrency=10,
                            multipart_chunksize=1024*100, use_threads = True)
    file = FILE_PATH + filename
    key = KEY_PATH + filename
    s3_client.upload_file(file, S3_BUCKET, key,
    Config = config,
    Callback=ProgressPercentage(file)
    )

uploadFiles('vehicles.csv')


