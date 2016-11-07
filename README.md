# Bobos3
Simple library for accessing and manipulating objects via S3 API.
Tested on Ceph and IBM Cloud Object Storage (Cleversafe).
# Usage
```
create_bucket(host, bucketName, accessKey, secretAccessKey)
s3bucket = S3Bucket(host, bucketName, accessKey, secretAccessKey)

uploadId = s3bucket.init_multipart_upload(fileName)
etag = s3bucket.write(fileName, partNo=1, uploadId=uploadId, content=content)
s3bucket.complete_multipart_upload(fileName, uploadId, parts=[(1, etag)])

s3bucket.exists(fileName)
s3bucket.remove(fileName)
```
# Tests
To run tests set environment varialbes `S3HOST, S3BUCKET, S3ACCESSKEY, S3SECRETKEY` and run `python test_bobos3.py`.