import hashlib, sys, unittest, os, time
import io

from src.ObjectStorageClient import ContainerInfo, ObjectInfo, ObjectStorageClient
sys.path.append('src')
import SwiftClient
class TestCases(unittest.TestCase):
    container_name = None
    storage_url = None
    object_name = 'test-object-123456789'

    def test_suite(self):
        self.container_name = f'universal-ocs-test-container-{round(time.time())}' # Test container name

        # Create the client
        self.client : ObjectStorageClient = SwiftClient.SwiftClient(self.storage_url)
        
        # Run tests
        self.container_create()
        self.container_list()

        self.client.use_container(self.container_name)

        self.object_upload()
        self.object_info()
        self.object_list()

        self.object_set_metadata()
        self.object_delete_metadata()
        self.object_download()
        
        self.object_delete()
        self.container_delete()


    def container_create(self):
        print(f'Creating container {self.container_name}')
        ok = self.client.container_create(self.container_name)
        self.assertTrue(ok)

    def container_list(self):
        print(f'Listing containers')
        containers = self.client.container_list()
        self.assertGreater(len(containers), 0, 'container_list() should not return an empty list')
        self.assertTrue(isinstance(containers[0], ContainerInfo), 'container_list() should return a list of ContainerInfo')
        self.assertIn(self.container_name, [c.name for c in containers], 'created container not in the containter list')
        containers = self.client.container_list(self.container_name)
        self.assertEqual(len(containers), 1, 'container_list(<prefix>) should return only the containers that start with <prefix>')

    def container_delete(self):
        print(f'Deleting container {self.container_name}')
        ok = self.client.container_delete(self.container_name)
        self.assertTrue(ok, 'container_delete() should return True on success')
        ok = self.client.container_delete('universal-osc-unexisting-container-12345677898765453265265264325456524524625')
        self.assertTrue(ok, 'container_delete() should return True if container does not exist')
        # TODO: Should not delete if container is not empty

    def object_upload(self):
        print(f'Uploading object')
        size_bytes = 100
        data = io.BytesIO(os.urandom(size_bytes))
        md5 = hashlib.md5()
        md5.update(data.getvalue())
        self.md5 = md5.digest().hex()
        ok = self.client.object_upload(data, self.object_name, {'myKey1':'myValue1'})
        self.assertTrue(ok, 'object_upload() should return true on success')

    def object_list(self):
        print('Listing objects')
        objects = self.client.object_list(True)
        self.assertTrue(isinstance(objects, list), 'object_list() should return a list')
        self.assertTrue(isinstance(objects[0], ObjectInfo), 'object_list() should return a list of ObjectInfo')
        self.assertIn(self.object_name, [c.name for c in objects], 'created container not in the containter list')
        objects = self.client.object_list(self.object_name)
        self.assertEqual(len(objects), 1, 'object_list(<prefix>) should return only the objects that start with <prefix>')
        # Note that the key is case insensitive and should be lower case when returned by the api

    def object_info(self):
        print('Checking object info')
        info = self.client.object_info(self.object_name)
        self.assertTrue(isinstance(info, ObjectInfo), 'object_info() should return an instance of ObjectInfo')
        self.assertDictEqual(info.metadata, { 'mykey1': 'myValue1'}, 'object_upload() should correctly set the object metadata')
        info = self.client.object_info('unexisting-object')
        self.assertIsNone(info, 'object_info() should return None if the object does not exist')


    def object_delete(self):
        print('Deleting object')
        ok = self.client.object_delete(self.object_name)
        self.assertTrue(ok, 'object_delete() should return true on success')
        
        objects = self.client.object_list(self.object_name)
        self.assertEqual(len(objects), 0, 'object_delete() should delete the given object properly')

    def object_set_metadata(self):
        print('Adding metadata to object')
        self.assertTrue(self.client.object_set_metadata(self.object_name, 'key2', 'value2'))
        self.assertEqual(self.client.object_info(self.object_name).metadata,
        {
            'key2':'value2',
            'mykey1': 'myValue1'
        })

    def object_delete_metadata(self):
        print('Deleting object metadata')
        self.assertTrue(self.client.object_delete_metadata(self.object_name, 'mykey1'))
        self.assertEqual(self.client.object_info(self.object_name).metadata, { 'key2':'value2' })

    def object_download(self):
        data = io.BytesIO()
        ok = self.client.object_download(self.object_name, data)
        self.assertTrue(ok, 'object_download() should return true on success')

        md5 = hashlib.md5()
        md5.update(data.getvalue())
        self.assertEqual(md5.digest().hex(), self.md5, 'object_download() and object_upload() should keep file integrity intact')


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage: python -m tests <storage-url>')
        exit()
    else:
        TestCases.storage_url = sys.argv.pop()

    unittest.main()