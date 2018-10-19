# s3tar
A python program to tar up S3 buckets and store the resulting file in a different S3 bucket.

## Problem
We have about 50TB of data stored as uncompressed files about 1MB in size. We don't currently have a need for the data, but it's good stuff, so we want to keep it around. We would like to reduce the S3 storage costs as well as make it easier to move the data in and out of S3.

## Proposed Solution
Archive the data files in a single bucket to compressed tar files in S3. There will be multiple tar files so we don't run into the 5TB S3 storage limit. We will use Python/Boto3 to implement the process.


## Evolution
This code has developed stepwise. The initial implementation was a simple loop that read an object from S3 into a BytesIO object. The contents of that object was then written to an object in an archive bucket in tar format. That worked, but the conversion was not fast enough. The program was getting about 209 kbit/sec for writes to the tar file. Using a measured compression rate of 10 to 1, the process would have taken about six years to complete. 

The code was updated to use threading for the read operations. This resulted in an order of magnitude improvement in performance, but was bottlenecked on compression for the single-threaded write operation. The result was it would take about half a year to complete. Since the transfer rate on writes was only 2.3 mbits/second and the EC2 instance network interface was rated at 5gbit/second, there was still some room for optimization. 

The next step is to parallelize the compression. There are two approaches to this: refactor the tar operation to create multiple tar files in paralell in multiple threads; or make the process a two-pass operation where the objects are compressed and then the tar file is created from the compressed objects. We chose the second options as it looks like a simpler approach.
