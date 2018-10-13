#! /usr/bin/env python3
# encoding: utf-8

#
#   Read a list of buckets from a file.
#   Compress the individual files from a bucket and add them to a tar archive.
#   Write the tar archive to a new, archive, bucket.
#

import argparse
import botocore.exceptions
import boto3
import smart_open
import sys
import tarfile
import traceback
import io
import os
import threading
import collections
import urllib3
import time
import zlib
 
# How much data to put in an archive file. Default is 1TB
#ARCHIVE_SIZE = 1000000000000
ARCHIVE_SIZE = 100000000000

PARSER = argparse.ArgumentParser('Tar up buckets')
PARSER.add_argument('--bucket_name', '-b', required=True, help='The name of the bucket to archive')
PARSER.add_argument('--new_bucket_name', '-n', default=None, help='The name of the bucket to extract files to.')
PARSER.add_argument('--profile', '-p', default=None, help='The AWS profile to use.')
PARSER.add_argument('--archive_name', '-a', default='dai-archive', help='The name of the bucket to store the tarfiles in.')
PARSER.add_argument('--region', '-r', default='us-west-2')
PARSER.add_argument('--compression', '-z', action='store_true', help='Turn on gzip compression.')
PARSER.add_argument('--create', '-c', action='store_true', help='Create a tar archive')
PARSER.add_argument('--extract', '-x', action='store_true', help='Extract a tar archive')
PARSER.add_argument('--verify', '-v', action='store_true', help = 'Verify that two buckets have the same contents.')
ARGS = PARSER.parse_args()


# The mode to use when writing the tar file. 
# Default is set to streaming write with gzip compression.
COMPRESS_MODE = 'w|gz'
COMPRESS_TAIL = '.tar.gz'
# Uncompressed tar file
UNCOMPRESS_MODE = 'w|'
UNCOMPRESS_TAIL = '.tar'


def main():
  ''' Get the archive bucket.
      Open the list of buckets to archive.
      Read through the list, working on one bucket at a time.
      Limit the size of tar files to the archive_size.
      Name the tar files with the bucket name and aaa-zzz appended.
  '''
  try:
    check_args()
    # If the script is running on an EC2 instance with a role providing S3 access, no need
    # for keys.
    if ARGS.profile == None:
      session = boto3.session.Session(region_name=ARGS.region)
    else:
      session = boto3.session.Session(profile_name=ARGS.profile, region_name=ARGS.region)
    s3 = session.resource('s3')
    s3_client = session.client('s3')
    if ARGS.create:
      archive = create_bucket(s3, s3_client, ARGS.region, ARGS.bucket_name)
      archive_bucket(ARGS.bucket_name, ARGS.archive_name, s3, ARCHIVE_SIZE, ARGS.profile, ARGS.compression)
    elif ARGS.extract:
      extract_bucket(ARGS.bucket_name, ARGS.new_bucket_name, ARGS.archive_name, s3, s3_client, ARGS.profile)
    elif ARGS.verify:
      verify_bucket(ARGS.bucket_name, ARGS.new_bucket_name, s3, s3_client)
    else:
      print('Nothing to do, quitting')
  except SystemExit as e:
    if e.code == 0:
      os._exit(0)
    else:
      (err_type, value, tb) = (sys.exc_info())
      print (f'Unexpected error in main, type {err_type}, value {value}')
      traceback.print_tb(tb, limit=10)
  except:
    (err_type, value, tb) = (sys.exc_info())
    print (f'Unexpected error in main, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=10)


def check_args():
  ''' Verify that either the create or extract flag is specified, but not both of at the same time. '''
  if ARGS.create and not (ARGS.extract or ARGS.verify):
    return
  elif ARGS.extract and not (ARGS.create or ARGS.verify):
    if ARGS.new_bucket_name == None:
      print ('A new bucket name must be specified for an extract operation.')
      sys.exit(-1)
    return
  elif ARGS.verify and not (ARGS.create or ARGS.extract):
    if ARGS.new_bucket_name == None:
      print ('A new bucket name must be specified the verify operation')
      sys.exit(-1) 
    return


META = 'ResponseMetadata'
STATUS = 'HTTPStatusCode'
def create_bucket(s3, s3_client, region, bucket_name):
  ''' See if the archive bucket exists, if not, create it.  
  '''
  try:  
    if not bucket_exists(bucket_name, s3):
      response = s3_client.create_bucket(
          ACL='private', 
          Bucket=bucket_name, 
          CreateBucketConfiguration={'LocationConstraint': region}
          )
      if response[META][STATUS] != 200:
        print(f'Bucket not created, bailing. HTTP Status is: {response[META][STATUS]}')
        sys.exit(0)
    return s3.Bucket(bucket_name)
  except:
    (err_type, value) = (sys.exc_info()[:2])
    print (f'Unexpected error in get_archive, type {err_type}, value {value}')
    sys.exit(-1)


THREAD_LIMIT = 60
FIFO_LIMIT = 200
LOCK = threading.Lock()
FIFO = collections.deque()


def archive_bucket(bucket_name, archive_name, s3, size, profile, compress):
  ''' Given the name of a bucket, an S3 bucket object pointing to an archive and
      an S3 session, open a bucket object for the bucket,
      then read the files one at a time, compress them and
      add them to a tar archive that is written to the archive bucket.

      The tar file size is constrained by S3 limits to 5TB. The actual size is
      set to be around ARCHIVE_SIZE. If the bucket being archived contains more than ARCHIVE_SIZE
      of compressed data, then multiple tar files are created. The tar files have a count added to their
      name to differentiate them. The compressed files are NOT split across the tar file. That
      is why there is an inexact size to the tar file.
  '''
  try: 
    # Make sure the bucket to be archived exists, before trying to archive it.
    if not bucket_exists(bucket_name, s3):
      print(f'The bucket {bucket_name} does not exist, skipping.')
      return

    # Fire up the writer  !!!!!!
    writer = threading.Thread(target= write_tars_to_s3, 
                args=(bucket_name, archive_name, size, profile, compress))
    writer.start()
    bucket = s3.Bucket(bucket_name)
    for object in bucket.objects.all():

      while threading.active_count() > THREAD_LIMIT or len(FIFO) >= FIFO_LIMIT: 
        print(f'Slept on limit. Thread count is {threading.active_count()}. FIFO has {len(FIFO)} objects.')
        time.sleep(2)   
        
      reader = threading.Thread(target=copy_s3_object,
                        args=(bucket, object))
      reader.start()
    # All of the objects hae been passed to a reader thread at this point. We need to wait for the
    # reader threads to finish up and then pass the end mark to the writer thread.
    while threading.active_count > 2:
      time.sleep(5)
    if lock.acquire():
      FIFO.append(None, None)
      lock.release()
    else:
      printf(f'Failed to lock fifo for end mark. Bailing')
      return
    # Give the writer 10 seconds for each item remaining on the queue. 
    # Plus an extra 10 seconds, because I don't really know what I'm doing
    # here.
    wait_time = 10.0 + (10.0 * len(FIFO))
    writer.join(timeout=wait_time)
    if writer.isAlive():
      print(f'Writer thread has not finished. Waited for {wait_time} seconds. Quiting in disgust.')
  except tarfile.HeaderError as e:
    print (f'Tarfile got an invalid buffer during write. Currenty writing {tar_name}. Punting on the whole operation.')
    return
  except urllib3.exceptions.ProtocolError as e:
    print (f'Archive function failure on key {object.key}. Error is {e.err_type}, Value is {e.value}')
    return
  except:
    (err_type, value, tb) = sys.exc_info()
    print (f'Unexpected error in archive_bucket, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=20)
    return


def copy_s3_object(bucket, object):
  ''' Given bucket, an S3 member object from that bucket,
      a fifo queue and a thread lock, grab the contents of the object
      and store it into a BytesIO array. Create a tarinfo object from
      the S3 member object. Then store them as a tuple in the fifo, making
      sure to use the lock to prevent race conditions from overwriting data.
  '''
  global FIFO, LOCK
  content = io.BytesIO()
  # Catch ProtocolError exceptions and retry five times
  retry = 0  # Keep track of retries to avoid the dreaded infinite loop.
  while True:
    try:
      bucket.download_fileobj(object.key, content)
      break
    except urllib3.exceptions.ProtocolError as e:
      if retry >= 5:
        return False
      else:
        retry = retry + 1
        print(f'Caught ProtocolError exception downloading S3 file {object.key}, retry #{retry}.')
        time.sleep(5*retry)
  try:
    content.seek(0)
    tarinfo = create_tarinfo(object)
    if LOCK.acquire():
      FIFO.append( (tarinfo, content) )
      LOCK.release()
      return True
    else:
      printf(f'Failed to lock fifo for {object.key}')
      return False
  except:
    (err_type, value, tb) = sys.exc_info()
    print (f'Unexpected error in copy_s3_object, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=20)
    return False


def write_tars_to_s3(bucket_name, archive_name, size, profile, compress):
  ''' Open a smart_open file in an S3 bucket, pop a tuple off of
      the fifo and use the tarinfo and contents of the tuple to
      write the data in tar format to the bucket.
  '''
  global FIFO, LOCK
  if compress:
    mode = COMPRESS_MODE
    tail = COMPRESS_TAIL
  else:
    mode = UNCOMPRESS_MODE
    tail = UNCOMPRESS_TAIL
  tarfile_count = 1   # A counter to use for naming multiple tar files on the same bucket.
  tar_name = 's3://%s/%s_file%d%s' % (archive_name, bucket_name, tarfile_count, tail)
  highwater = 0       # Watch the size of the queue to tune the number of reader threads.
  bytes_written = 0
  time.sleep(5) # Let the readers get started.
  try: 
    # Get a file handle for the tar file in the archive bucket.
    archive_out = smart_open.smart_open(tar_name, 'wb', profile_name=profile)
    tf = tarfile.open(mode=mode, fileobj=archive_out)
    while True:

      # Look for something on the queue. If nothing is there, sleep for a couple seconds and try again.
      try:
        LOCK.acquire()
        (tarinfo, content) = FIFO.popleft()
        LOCK.release()
        # The controlling function will put a tuple on the fifo that is (None, None)
        # when all of the objects have been processed.
        # If so return
        if tarinfo == None:
          if len(FIFO) > 0:
            print(f'write_tars_to_s3 got an end mark, but there are {len(FIFO)} files left to be processed.')
          return
      except IndexError:
        print('Hit IndexError')
        LOCK.release()
        time.sleep(2)
        continue  # Skip the rest of the while loop as no tuple was found
      queuelength = len(FIFO)
      if queuelength > highwater:  # Keep track of the high water mark.
        highwater = queuelength
        print(f'Highwater mark for queue is now {highwater}')
      if bytes_written > size:
        bytes_written = 0 
        tf.close()
        archive_out.close()
        tarfile_count = tarfile_count + 1
        tar_name = 's3://%s/%s_file%d%s' % (archive_name, bucket_name, tarfile_count, tail)
        print(f'Creating new tar file {tar_name}')
        archive_out = smart_open.smart_open(tar_name, 'wb', profile_name=profile)
        tf = tarfile.open(mode=mode, fileobj=archive_out)
      tf.addfile(tarinfo, content)
      bytes_written = bytes_written + content.getbuffer().nbytes
      #print(f'Key is: {tarinfo.name}, bytes_left are {ARCHIVE_SIZE - bytes_written}') 
      content.close()
  except zlib.error as e:
    print (f"Compression failed on {key}. I don't have a clue. Punting.")
    sys.exit(-1)
  except tarfile.HeaderError as e:
    print (f'Tarfile got an invalid buffer during write. Currenty writing {tar_name}. Punting on the whole operation.')
    sys.exit(-1)
  except urllib3.exceptions.ProtocolError as e:
    print (f'Archive function failure on key {object.key}. Error is {e.err_type}, Value is {e.value}')
    sys.exit(-1)
  except:
    (err_type, value, tb) = sys.exc_info()
    print (f'Unexpected error in archive_bucket, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=20)


def extract_bucket(bucket_name, new_bucket_name, archive_name, s3, s3_client, profile):
  ''' Given the name of bucket to create and an archive bucket,
      read all of the tar files in the archive bucket and extract
      them to the bucket. 
  '''
  try:
    # The archive bucket must exist, the new bucket must not exist.
    # Whew! Existential issues.
    if not bucket_exists(archive_name, s3):
      print(f'The archive bucket, {archive_bucket} does not exist, bailing out.')
      sys.exit(0)
    if bucket_exists(new_bucket_name, s3):
      print(f'The new bucket, {new_bucket_name}, already exists. Please provide name of non-existant bucket.')
      sys.exit(0)
    else:
      new_bucket = create_bucket(s3, s3_client, ARGS.region, new_bucket_name)
    archive_bucket = s3.Bucket(archive_name)
    not_found = True  # Need to make sure we find something
    for object in archive_bucket.objects.filter(Prefix=bucket_name):
      not_found = False
      tar_name = 's3://%s/%s' % (archive_name, object.key)
      archive_in = smart_open.smart_open(tar_name, 'rb', profile_name=profile)
      # Need to test for zip file and set mode.
      mode = get_compressed_mode(object.key)
      tf = tarfile.open(mode=mode, fileobj=archive_in)
      member = tf.next()
      while member != None:
        with tf.extractfile(member) as data:
          bdata = io.BytesIO()
          for line in data:
            bdata.write(line)
          bdata.seek(0)
          new_bucket.upload_fileobj(bdata, member.name)
          bdata.close
        member = tf.next()
      tf.close()
      archive_in.close()
    if not_found:
      print(f'Did not find any tar files with the name {bucket_name} in archive {archive_name}')
  except SystemExit as e:
    if e.code == 0:
      os._exit(0)
    else:
      (err_type, value, tb) = (sys.exc_info())
      print (f'Unexpected error in main, type {err_type}, value {value}')
      traceback.print_tb(tb, limit=10)
  except:
    (err_type, value, tb) = sys.exc_info()
    print (f'Unexpected error in archive_bucket, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=20)


def verify_bucket(bucket_name, new_bucket_name, s3, s3_client):
  ''' Given the names of two buckets, step through the files in the first bucket
      and verify that a file with the same key exists in the second bucket and has
      the same checksum.
  '''
  try:
    bucket = s3.Bucket(bucket_name)
    new_bucket = s3.Bucket(new_bucket_name)
    for object in bucket.objects.all():
      md5sum = s3_client.head_object(
          Bucket=bucket_name,
          Key=object.key
        )['ETag'][1:-1]

      new_md5sum = s3_client.head_object(
          Bucket=new_bucket_name,
          Key=object.key
        )['ETag'][1:-1]
      if md5sum != new_md5sum:
        print(f'Checksums differ for {object.key}.')
        print(f'Old is: {md5sum}')
        print(f'New is: {new_md5sum}')
        sys.exit(-1)
  except botocore.exceptions.ClientError as e:
    print(f'Exception during verify. Error is {e.err_type}, value is {e.value}')
    sys.exit(-1)
  except:
    (err_type, value, tb) = sys.exc_info()
    print (f'Unexpected error in archive_bucket, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=20)   
  return

def bucket_exists(bucket_name, s3):
  ''' Verify that the bucket exists. 
      Code stolen from,
      https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html
  '''
  exists = True
  try:
      s3.meta.client.head_bucket(Bucket=bucket_name)
  except botocore.exceptions.ClientError as e:
      # If a client error is thrown, then check that it was a 404 error.
      # If it was a 404 error, then the bucket does not exist.
      error_code = e.response['Error']['Code']
      if error_code == '404':
          exists = False
  return exists


def create_tarinfo(object):
  ''' Given a file name (s3 object) and the buffer holding
      the content, create a TarInfo object.
  '''
  try:
    tarinfo = tarfile.TarInfo(object.key)
    tarinfo.size = object.size
    '''The mtime field represents the data modification time of the file at the time it 
       was archived. It represents the integer number of seconds since January 1, 1970, 
       00:00 Coordinated Universal Time.
    '''
    tarinfo.mtime = object.last_modified.timestamp()
    tarinfo.uname = object.owner['DisplayName']
    return tarinfo
  except ValueError as e:
    (err_type, value, tb) = sys.exc_info()
    print (f'Problem in get_tarinfo, type {err_type}, value {value}')
    traceback.print_tb(tb, limit=10)
    sys.exit(-1)
  except:
    (err_type, value) = (sys.exc_info()[:2])
    print (f'Unexpected error in get_tarinfo, type {err_type}, value {value}')
    sys.exit(-1)

def get_compressed_mode(name):
  ''' Given a filename, see if it has a .gz on the end.
      If it does, return the tar mode for reading compressed tar files.
      If it does not, return the tar mode for reading regular tar files.
  '''
  filename, file_extension = os.path.splitext(name)
  if file_extension == '.gz':
    mode = 'r|gz'
  else:
    mode = 'r|'
  return mode

if __name__ == "__main__":
  main()