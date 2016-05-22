import sys
import os
import threading

import boto3
import botocore


class BucketDoesNotExist(Exception):
    pass


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far,
                                             self._size, percentage))
            sys.stdout.flush()


class S3(object):
    def __init__(self, resource=None, should_raise=False, verbose=False):
        if resource is None:
            resource = boto3.resource('s3')  # using default configuration

        self.resource = resource
        self.should_raise = should_raise
        self.verbose = verbose

    def upload_object(self, bucket, body, s3_key):
        if not s3_key:
            raise ValueError('s3_key')

        bucket, exists = self._bucket(bucket)

        if not exists:
            return None

        return bucket.put_object(Body=body, Key=s3_key)

    def upload_file(self, bucket, filename, s3_key=None):
        if not filename:
            raise ValueError('filename')

        if s3_key is None:
            s3_key = os.path.basename(filename)

        bucket, exists = self._bucket(bucket)

        if not exists:
            return None

        callback = ProgressPercentage(filename) if self.verbose else None
        return bucket.upload_file(filename, s3_key, Callback=callback)

    def upload_directory(self, bucket, path, s3_key=None, keep_structure=True):
        if not path:
            raise ValueError('path')

        if s3_key is None:
            s3_key = ''  # bucket root

        bucket, exists = self._bucket(bucket)

        if not exists:
            return None

        for root, dirs, files in os.walk(path):
            for file in files:
                if file.startswith('.'):  # ignore hidden files
                    continue

                filename = os.path.join(root, file)

                if keep_structure:
                    # relative to root directory
                    edir = root.replace(path, '')
                    edir = edir[1:] if edir.startswith(os.path.sep) else edir
                    key = os.path.join(s3_key, edir, file)
                else:
                    key = os.path.join(s3_key, file)

                callback = ProgressPercentage(filename) if self.verbose else None
                bucket.upload_file(filename, key, Callback=callback)

    def _bucket(self, bucketname):
        if not bucketname:
            raise ValueError('bucketname')

        bucket = self.resource.Bucket(bucketname)
        exists = True
        try:
            self.resource.meta.client.head_bucket(Bucket=bucketname)
        except botocore.exceptions.ClientError as e:
            if self.should_raise:
                raise BucketDoesNotExist

            # If a client error is thrown, then check that it was a 404 error.
            error_code = int(e.response['Error']['Code'])
            # If it was a 404 error, then the bucket does not exist.
            if error_code == 404:
                exists = False

        return bucket, exists
