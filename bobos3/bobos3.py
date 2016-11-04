# -*- coding: utf-8 -*-

import hashlib
import requests
from xml.etree import ElementTree
from aws_requests_auth.aws_auth import AWSRequestsAuth


def create_bucket(host, bucketName, accessKey, secretKey):
    """
    Create a bucket.
    @param host: S3/Cleversafe host
    @param bucketName: Bucket name
    @param accessKey: Access Key ID
    @param secretKey: Secret Access Key
    """
    auth = AWSRequestsAuth(aws_access_key=accessKey,
                           aws_secret_access_key=secretKey,
                           aws_host=host,
                           aws_region='',
                           aws_service='s3'
                           )
    url = 'http://{host}/{bucket}'.format(host=host, bucket=bucketName)
    response = requests.request('PUT', url, auth=auth)
    return response.status_code == 200


class S3Bucket(object):
    """
    Simple S3/Cleversafe library. It supports: multipart read/write, getting size and removing.
    Uses path-style access to buckets (example.com/bucketName/key), not the virtual hosted-style (bucketName.example.com/key)
    """

    def __init__(self, host, bucketName, accessKey, secretKey):
        """
        Init the AWS Authenticating module.
        @param host: S3/Cleversafe host
        @param bucketName: Bucket name
        @param accessKey: Access Key ID
        @param secretKey: Secret Access Key
        """
        self.host = host
        self.bucketName = bucketName
        self.auth = AWSRequestsAuth(aws_access_key=accessKey,
                                    aws_secret_access_key=secretKey,
                                    aws_host=self.host,
                                    aws_region='',
                                    aws_service='s3'
                                    )
        self.s3NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'

    @property
    def _baseUrl(self):
        return 'http://{host}/{bucket}'.format(host=self.host, bucket=self.bucketName)

    def _request(self, method, key=None, headers=None, data=None):
        """Helper method. Sets url and headers"""
        url = self._baseUrl
        if key:
            url += '/' + key
        if not headers:
            headers = dict()
        if data:
            headers.update(self._get_content_hash_header(data))

        response = requests.request(method, url, auth=self.auth, headers=headers, data=data)
        response.raise_for_status()
        return response

    def _create_multipart_upload_body(self, parts):
        """Helper method that creates xml content containing part numbers and it's etags"""
        rootElement = ElementTree.Element('CompleteMultipartUpload')
        for part in parts:
            number, etag = part
            partElement = ElementTree.SubElement(rootElement, 'Part')
            numberElement = ElementTree.SubElement(partElement, 'PartNumber')
            numberElement.text = str(number)
            etagElement = ElementTree.SubElement(partElement, 'ETag')
            etagElement.text = etag

        return ElementTree.tostring(rootElement)

    def _get_upload_id(self, response):
        """Helper method that parses xml respons for uploadId value"""
        xmlRoot = ElementTree.fromstringlist(response.content)
        elementName = '{ns}UploadId'.format(ns=self.s3NS)
        return xmlRoot.find(elementName).text

    def _get_content_hash_header(self, content):
        """Helper method, calculates hash of request body and returns its in header dict"""
        header = {
            'x-amz-content-sha256': hashlib.sha256(content).hexdigest()
        }
        return header

    def exists(self, key):
        """
        Check if file exists.
        @param key: file name
        @return: Boolean
        """
        response = self._request('HEAD', key)
        return response.status_code == 200

    def get_size(self, key):
        """
        Get file size.
        @param key: file name
        @return: file size as int
        """
        response = self._request('HEAD', key)
        return int(response.headers.get('content-length'))

    def get_etag(self, key):
        """
        Get ETag of file.
        @param key: file name
        @return: ETag
        """
        response = self._request('HEAD', key)
        return response.headers.get('etag')

    def remove(self, key):
        """
        Delete file.
        @param key: file name
        """
        response = self._request('DELETE', key)
        return response.status_code == 204

    def read(self, key, offset, size):
        """
        Read a part of a file.
        @param key: file name
        @param offset: the byte to start read
        @param size: how many bytes will be read
        @return: read content
        """
        end = offset + size - 1
        headers = {
            'Range': 'bytes={begin!s}-{end!s}'.format(begin=offset, end=end)
        }
        response = self._request('GET', key, headers)
        return response.content

    def init_multipart_upload(self, key):
        """
        Initialize multipart upload. Returns upload Id, an identifier of current upload session.
        You have to call completeMultipartUpload after writing all file parts to complete the upload or
        abortMultipartUpload to abort this multipart upload session.
        @param key: file name
        @return: upload Id
        """
        url = '{key}?uploads'.format(key=key)
        response = self._request('POST', url)
        return self._get_upload_id(response)

    def write(self, key, partNo, uploadId, content):
        """
        Upload a part of file, using a multipart upload. It has to be opened using initMultipartUpload method.
        You have to call completeMultipartUpload after writing all file parts to complete the upload or
        abortMultipartUpload to abort this multipart upload session.
        @param key: fileName
        @param partNo: number of part, can be an int <1,10000>
        @param uploadId: upload ID
        @param content: content to upload
        @return: ETag of uploaded content
        """
        url = '{key!s}?uploadId={uploadId!s}&partNumber={partNo!s}'.format(key=key, uploadId=uploadId, partNo=partNo)
        response = self._request('PUT', url, data=content)
        return response.headers.get('etag')

    def complete_multipart_upload(self, key, uploadId, parts):
        """
        Close upload session. It need a list of all parts numbers and its ETags to complete file upload.
        @param key: file name
        @param uploadId: upload ID
        @param parts: list of tuples containing part number and ETag of this part
        """
        body = self._create_multipart_upload_body(parts)
        url = '{key}?uploadId={uploadId}'.format(key=key, uploadId=uploadId)
        headers = {
            'Content-Type': 'text/xml'
        }
        response = self._request('POST', url, data=body, headers=headers)
        return response.status_code == 200

    def abort_multipart_upload(self, key, uploadId):
        """
        Abort multipart upload session.
        @param key: file name
        @param uploadId: upload Id
        """
        url = '{key}?uploadId={uploadId}'.format(key=key, uploadId=uploadId)
        response = self._request('DELETE', url)
        return response.status_code == 204

    def delete_bucket(self):
        """
        Delete bucket.
        """
        response = self._request('DELETE')
        return response.status_code == 204
