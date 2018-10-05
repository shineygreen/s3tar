#! /usr/bin/env python3
# encoding: utf-8

#
#		Read a list of buckets from a file.
#		Compress the individual files from a bucket and add them to a tar archive.
#		Write the tar archive to a new, archive, bucket.
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
 
# How much data to put in an archive file. Default is 1TB
ARCHIVE_SIZE = 1000000000000

PARSER = argparse.ArgumentParser('Tar up buckets')
PARSER.add_argument('--bucket_name', '-b', required=True, help='The name of the bucket to archive')
PARSER.add_argument('--new_bucket_name', '-n', help='The name of the bucket to extract files to.')
PARSER.add_argument('--profile', '-p', default=None, help='The AWS profile to use.')
PARSER.add_argument('--archive_name', '-a', default='dai-archive', help='The name of the bucket to store the tarfiles in.')
PARSER.add_argument('--region', '-r', default='us-west-2')
PARSER.add_argument('--compression', '-z', action='store_true', help='Turn on gzip compression.')
PARSER.add_argument('--create', '-c', action='store_true', help='Create a tar archive')
PARSER.add_argument('--extract', '-x', action='store_true', help='Extract a tar archive')
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
	if ARGS.create and ARGS.extract:
		print('The create and extract arguments are incompatible, use either one or the other.')
		sys.exit(0)
	elif not ARGS.create and not ARGS.extract:
		print('Either the -c or the -x flag must be specified')
		sys.exit(0)
	return

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
		return s3.Bucket(bucket_name)
	except:
		(err_type, value) = (sys.exc_info()[:2])
		print (f'Unexpected error in get_archive, type {err_type}, value {value}')
		sys.exit(-1)


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
	if compress:
		mode = COMPRESS_MODE
		tail = COMPRESS_TAIL
	else:
		mode = UNCOMPRESS_MODE
		tail = UNCOMPRESS_TAIL

	tarfile_count = 1   # A counter to use for naming multiple tar files on the same bucket.
	bytes_writen = 0    # Keep track of the archived tar file size so it can be limited to ARCHIVE_SIZE.
	# Get a file handle for the tar file in the archive bucket.
	tar_name = 's3://%s/%s_file%d%s' % (archive_name, bucket_name, tarfile_count, tail)
	# Make sure the bucket to be archived exists, before trying to archive it.
	if not bucket_exists(bucket_name, s3):
		print (f'The bucket {bucket_name} does not exist, skipping.')
		return

	try: 
		archive_out = smart_open.smart_open(tar_name, 'wb', profile_name=profile)
		tf = tarfile.open(mode=mode, fileobj=archive_out)
		bytes_written = 0
		bucket = s3.Bucket(bucket_name)
		for object in bucket.objects.all():
			content = io.BytesIO()
			bucket.download_fileobj(object.key, content)
			# Check to see if the tar file size is over the limit.
			# If so, close the current file and open a new one. 
			if bytes_written > size:
				bytes_written = 0
				tf.close()
				archive_out.close()
				tarfile_count = tarfile_count + 1
				tar_name = 's3://%s/%s_file%d%s' % (archive_name, bucket_name, tarfile_count, tail)
				archive_out = smart_open.smart_open(tar_name, 'wb', profile_name=profile)
				tf = tarfile.open(mode=mode, fileobj=archive_out)
			# Compress the content and then write it to the tar file.
			#compressed_content = zlib.compress(content, 9)
			tarinfo = create_tarinfo(object)
			content.seek(0)   # Need to set the stream to the beginning.
			tf.addfile(tarinfo, content)
	except zlib.error as e:
		print (f"Compression failed on {key}. I don't have a clue. Punting.")
		sys.exit(-1)
	except tarfile.HeaderError as e:
		print (f'Tarfile got an invalid buffer turing write. Currenty writing {tar_name}. Punting on the whole operation.')
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
		if not bucket_exists(archive_name, s3):
			print(f'The archive bucket, {archive_bucket} does not exist, bailing out.')
			sys.exit(0)
		archive = s3.Bucket(bucket_name)
		if new_bucket_name != None:
			dest_bucket = create_bucket(s3, s3_client, ARGS.region, new_bucket_name)
		else:
			dest_bucket = s3.Bucket(bucket_name)
		archive_bucket = s3.Bucket(archive_name)
		not_found = True  # Need to make sure we find something
		for object in archive_bucket.objects.filter(Prefix=bucket_name):
			not_found = False
			print(object.key)
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
					dest_bucket.upload_fileobj(bdata, member.name)
					bdata.close
				member = tf.next()
			tf.close()
			archive_in.close()
		if not_found:
			print(f'Did not find any tar files with the name {bucket_name} in archive {archive_name}')
	except:
		(err_type, value, tb) = sys.exc_info()
		print (f'Unexpected error in archive_bucket, type {err_type}, value {value}')
		traceback.print_tb(tb, limit=20)


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
	print(f'Name is {filename}, extension is {file_extension}')
	if file_extension == '.gz':
		mode = 'r|gz'
	else:
		mode = 'r|'
	print(f'Mode: {mode}')
	return mode

if __name__ == "__main__":
	main()