# -*- coding: utf-8 -*-
import os
import unittest
from math import ceil

from requests import HTTPError

from bobos3.bobos3 import S3Bucket


class S3LibraryTests(unittest.TestCase):
    host = os.environ.get('S3HOST')
    accessKey = os.environ.get('S3ACCESSKEY')
    secretAccessKey = os.environ.get('S3SECRETKEY')
    bucketName = os.environ.get('S3BUCKET')

    testFileName = u"TestFile"
    # small text
    testFileContent = u"Test file content " * 5
    # 20 mb text
    testLargeFileContent = 20 * "a" * 1024 * 1024
    # 6mb chunk
    chunkSize = 6 * 1024 * 1024

    def uploadMultipartTestFile(self, fileName, fileContent):
        """Splits the file into chunks of size equal or lower than chunk size and performs a multipart upload"""
        fileContentLength = len(fileContent)
        parts = []
        totalParts = int(ceil(float(fileContentLength) / self.chunkSize))
        partNo = 1

        uploadId = self.connection.init_multipart_upload(fileName)
        while partNo <= totalParts:
            content = fileContent[(partNo - 1) * self.chunkSize:min((partNo) * self.chunkSize, fileContentLength)]
            etag = self.connection.write(fileName, partNo, uploadId, content)
            parts.append((partNo, etag))
            partNo += 1
        self.connection.complete_multipart_upload(fileName, uploadId, parts)
        self.removeOnTearDown.append(fileName)

    def setUp(self):
        super(S3LibraryTests, self).setUp()
        self.connection = S3Bucket(self.host, self.bucketName, self.accessKey, self.secretAccessKey)
        self.removeOnTearDown = []

    def tearDown(self):
        for file in self.removeOnTearDown:
            self.connection.remove(file)
        self.connection = None
        super(S3LibraryTests, self).tearDown()

    def test_uploadFile(self):
        """
        Upload a test file and check its size.
        """
        self.assertRaises(HTTPError, lambda _: self.connection.exists(self.testFileName), u'File should not exist.')
        self.uploadMultipartTestFile(self.testFileName, self.testFileContent)
        self.assertTrue(self.connection.exists(self.testFileName), u'File should exist.')

        expectedSize = len(self.testFileContent)
        retrievedSize = self.connection.get_size(self.testFileName)
        self.assertEqual(retrievedSize, expectedSize,
                         u"Size retrieved from S3: {0} Expected size: {1}".format(retrievedSize, expectedSize))

    def test_readFile(self):
        """
        Read from a file in S3
        """
        self.uploadMultipartTestFile(self.testFileName, self.testFileContent)

        # Read first 4 bytes
        self.assertEqual(self.connection.read(self.testFileName, 0, 4), self.testFileContent[0:4],
                         u"Retrieved content is not equal to the expected value.")
        # Read 4 bytes offset by 5
        self.assertEqual(self.connection.read(self.testFileName, 5, 4), self.testFileContent[5:9],
                         u"Retrieved content is not equal to the expected value.")
        # Read 4 bytes offset by file length - 2
        self.assertRaises(HTTPError, lambda _: self.connection.read(self.testFileName, len(self.testFileContent), 4),
                          u"Provided range should cause an exception.")
        # Read 4 bytes offset by file length
        self.assertRaises(HTTPError, lambda _: self.connection.read(self.testFileName, len(self.testFileContent), 4),
                          u"Provided range should cause an exception.")

        # finally read the whole file
        fileSize = self.connection.get_size(self.testFileName)
        self.assertEqual(self.connection.read(self.testFileName, 0, fileSize), self.testFileContent,
                         u"Retrieved content is not equal to the expected value.")

    def test_removeFile(self):
        """
        Remove a file
        """
        self.uploadMultipartTestFile(self.testFileName, self.testFileContent)
        self.assertTrue(self.connection.exists(self.testFileName), u'File should exist.')
        self.assertTrue(self.connection.remove(self.testFileName), u'File was not removed successfully.')
        self.assertRaises(HTTPError, lambda _: self.connection.exists(self.testFileName), u'File should be removed.')

    def test_getETag(self):
        """
        Get a ETag of a file
        """
        self.uploadMultipartTestFile(self.testFileName, self.testLargeFileContent)
        self.assertEqual(self.connection.get_etag(self.testFileName), '"152d89daa625b7d0efd72818d33e2894-4"')

    def test_abortMultipartUpload(self):
        """
        Abort an multipart upload session.
        """
        uploadId = self.connection.init_multipart_upload(self.testFileName)
        content = self.testLargeFileContent[0:self.chunkSize]
        etag = self.connection.write(self.testFileName, 1, uploadId, content)

        self.connection.abort_multipart_upload(self.testFileName, uploadId)

        content = self.testLargeFileContent[self.chunkSize:self.chunkSize*2]

        self.assertRaises(HTTPError, lambda _: self.connection.write(self.testFileName, 2, uploadId, content),
                          u'Further upload should not be possible.')

        parts = [(1, etag)]
        self.assertRaises(HTTPError, lambda _: self.connection.complete_multipart_upload(self.testFileName, uploadId, parts),
                          u'Complete after aborting multipart upload is not possible.')

if __name__ == "__main__":

    unittest.main()
