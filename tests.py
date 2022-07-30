import hashlib
import sys
import unittest
import os
from .src.SwiftClient import SwiftClient


class TestCases(unittest.TestCase):
    container_name = None
    storage_url = None
    file_name = 'dummy'

    def test_all(self):
        self.authentication()
        self.upload_file()
        self.read_metadata()
        self.add_metadata()
        self.delete_object()

    def authentication(self):
        self.swift = SwiftClient(self.storage_url, self.container_name)
        self.assertTrue(self.swift.authenticate(), 'Authentication failed')

    def upload_file(self):
        # Create a dummy file
        size_bytes = 100
        md5 = hashlib.md5()
        with open(self.file_name, 'wb') as f:
            data = os.urandom(size_bytes)
            f.write(data)
            md5.update(data)

        md5 = md5.digest().hex()

        self.assertTrue(self.swift.upload_file(self.file_name, self.file_name, meta={'myKey1':'myValue1'}), 'Uploading file failed')

        # Remove the dummy file
        os.remove(self.file_name)
    

    def read_metadata(self):
        meta = self.swift.object_get_metadata(self.file_name)
        self.assertEqual({'mykey1':'myValue1'}, meta)

    def delete_object(self):
        self.assertTrue(self.swift.object_delete(self.file_name))

    def add_metadata(self):
        self.assertTrue(self.swift.object_set_metadata(self.file_name, 'key2', 'value2'))
        self.assertEqual(self.swift.object_get_metadata(self.file_name),
        {
            'key2':'value2',
            'mykey1': 'myValue1'
        })

    def remove_metadata(self):
        self.assertTrue(self.swift.remove_object_metadata(self.file_name, 'mykey1'))
        self.assertEqual(self.swift.object_get_metadata(self.file_name), { 'key2':'value2' })

    def download(self):
        pass


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Usage: python -m tests <storage-url> <container-name>')
        exit()
    else:
        TestCases.container_name = sys.argv.pop()
        TestCases.storage_url = sys.argv.pop()

    unittest.main()