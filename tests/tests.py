import sys, unittest, os, io, random, string, warnings, time

from src.ObjectStorageClient import ContainerInfo, ContainerNotSpecified, ObjectInfo, ObjectStorageClient, SubdirInfo
from src.S3Client import S3Client
from src.SwiftClient import SwiftClient

def random_string(size: int = 10):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(size))

class TestCases(unittest.TestCase):
    container_name = None
    object_name = 'test-object-123456789'

    storage = {
        "type": None
    }

    def test_suite_v2(self):
        warnings.simplefilter("ignore", ResourceWarning)
        container_prefix = "obs-client-test-container-"
        container_name = f"{container_prefix}{random_string()}"

        if(self.storage['backend'] == "s3"):
            print(f'Creating S3 client')
            client : ObjectStorageClient = S3Client(self.storage['location'])
        else:
            print(f'Creating Openstack Swift client')
            client : ObjectStorageClient = SwiftClient(self.storage['region'])

        # container_create()
        print(f'Creating container')
        self.assertTrue(client.container_create(container_name), "container_create() should return True when a container is created")
        self.assertFalse(client.container_create(container_name), "container_create() should return False when a container already exists")

        # container_info()
        print(f'Getting container info')
        info = client.container_info(container_name)
        self.assertIsInstance(info, ContainerInfo, 'container_info() should return a ContainerInfo instance')
        self.assertEqual(info.name, container_name, 'container_info() should always return the right container name')
        self.assertIsNone(client.container_info(container_name + '123'), "container_info() should return None if the container does not exist")

        # container_list()
        print(f'Listing containers')
        containers = client.container_list()
        self.assertGreater(len(containers), 0, 'container_list() should not return an empty list when at least one container exists')
        self.assertIsInstance(containers[0], ContainerInfo, 'container_list() should return a list of ContainerInfo')
        self.assertIn(container_name, [c.name for c in containers], 'a created container should appear in the listed containers')
        containers = client.container_list(container_name)
        self.assertEqual(len(containers), 1, 'container_list(<prefix>) should return only the containers that start with <prefix>')

        # TODO: object_list(fetch_metadata=True)

        # Select active container
        self.assertFalse(client.use_container(container_prefix + random_string(12)), 'use_container() should return false if container does not exist')
        self.assertTrue(client.use_container(container_name), 'use_container() should return true on success')

        # object_upload()
        print(f'Uploading object')
        object_name = 'dir1' + '/' + random_string()
        size_bytes = 100
        data = io.BytesIO(os.urandom(size_bytes))
        self.assertTrue(client.object_upload(data, object_name, metadata={'Key1': 'Value1'}), 'object_upload() should return true on success')

        # object_info()
        print(f'Getting object info')
        self.assertIsNone(client.object_info(random_string(20)), 'object_info() should return None if the object does not exist in the container')
        
        info = client.object_info(object_name)
        self.assertIsInstance(info, ObjectInfo, 'object_info() should return an instance of ObjectInfo when the object exists')
        self.assertEqual(info.name, object_name, 'object_info() should return the right object name')
        self.assertEqual(info.bytes, size_bytes, 'object_info() should return the right object size')
        
        # md5 = hashlib.md5()
        # md5.update(data.getvalue())
        # md5 = md5.digest().hex()
        # self.assertEqual(info.hash, md5, 'object_upload() should upload the file without tempering the data')

        self.assertIsNotNone(info.hash, 'object_info() should return an ObjectInfo with "hash" value that is not None')
        self.assertIsInstance(info.hash, str, 'object_info() should return an ObjectInfo with "hash" value that is a string')
        self.assertTrue(info.hash, 'object_info() should return an ObjectInfo with "hash" value that is not empty')

        self.assertIsNotNone(info.content_type, 'object_info() should return an ObjectInfo with "content_type" value that is not None')
        self.assertIsInstance(info.content_type, str, 'object_info() should return an ObjectInfo with "content_type" value that is a string')
        self.assertTrue(info.content_type, 'object_info() should return an ObjectInfo with "content_type" value that is not empty')

        self.assertIsNotNone(info.metadata, 'object_info() should return an ObjectInfo with "metadata" value that is not None')
        self.assertIsInstance(info.metadata, dict, 'object_info() should return an ObjectInfo with "metadata" value that is a dict')
        self.assertFalse('Key1' in info.metadata, 'object_upload() should make sure metadata keys are lowercase before uploading and/or object_list() should return lowercase metadata keys')
        self.assertDictEqual(info.metadata, { 'key1': 'Value1' } , 'object_upload() should set the specified metadata')
        
        self.assertIsNotNone(info.last_modified, 'object_info() should return an ObjectInfo with "last_modified" value that is not None')
        self.assertIsInstance(info.last_modified, float, 'object_info() should return an ObjectInfo with "last_modified" value that is an float')
        self.assertTrue(info.last_modified, 'object_info() should return an ObjectInfo with "last_modified" value that is not empty')
        self.assertTrue((time.time() - info.last_modified) < 30, 'the object last_modified date should be coherent')

        res = client.object_list(fetch_metadata=True, prefix=object_name)

        self.assertEqual(info, res[0])

        # play with metadata
        print(f'Updating metadata')
        self.assertFalse(client.object_replace_metadata(random_string(20), { 'Key2': 'Value2' }), 'object_replace_metadata() should return false if the object does not exist')
        self.assertTrue(client.object_replace_metadata(object_name, { 'Key2': 'Value2' }), 'object_replace_metadata() should return true on success')
        self.assertDictEqual(client.object_info(object_name).metadata, { 'key2': 'Value2' }, 'object_replace_metadata() should properly replace the objects metadata')

        self.assertFalse(client.object_set_metadata(random_string(20), 'Key3', 'Value3'), 'object_set_metadata() should return false if the object does not exist')
        self.assertTrue(client.object_set_metadata(object_name, 'Key3', 'Value3'), 'object_set_metadata() should return true on success')
        self.assertDictEqual(client.object_info(object_name).metadata, { 'key2': 'Value2', 'key3': 'Value3' }, 'object_set_metadata() should properly set the objects metadata without deleting the existing metatada')

        self.assertFalse(client.object_delete_metadata(random_string(20), 'key2'), 'object_delete_metadata() should return false if the object does not exist')
        self.assertTrue(client.object_delete_metadata(object_name, 'uknownn-key'), 'object_delete_metadata() should return true if the key does not exist in the metadata')
        self.assertTrue(client.object_delete_metadata(object_name, 'key2'), 'object_delete_metadata() should return true on success')
        self.assertDictEqual(client.object_info(object_name).metadata, { 'key3': 'Value3' }, 'object_delete_metadata() should properly delete the specified key without changing the other metadata values')

        # Upload more objects
        print(f'Uploading more objects')
        client.object_upload(stream=io.BytesIO(os.urandom(100)), object_name='dir1/' + random_string())
        client.object_upload(stream=io.BytesIO(os.urandom(100)), object_name='dir1/subdir1/' + random_string())
        client.object_upload(stream=io.BytesIO(os.urandom(100)), object_name='dir1/subdir2/' + random_string())

        self.assertEqual(client.object_list(delimiter='/'), [SubdirInfo('dir1/')], 'object_list() with delimiter="/" should return a subdir')
        objects = client.object_list(prefix='dir1/', delimiter='/')
        self.assertIn(SubdirInfo(subdir='dir1/subdir2/'), objects, 'object_list() with delimiter and prefix should return the subdirs')

        # Download an object
        print(f'Downloading objects')
        downloaded_data = io.BytesIO()
        self.assertFalse(client.object_download(random_string(20), downloaded_data), 'object_download() should return false if object does not exist')
        self.assertTrue(client.object_download(object_name, downloaded_data), 'object_download() should return true on success')
        self.assertEqual(downloaded_data.getvalue(), data.getvalue(), 'object_download() should download the same data that was uploaded with object_upload()')

        # Upload a file
        print(f'Uploading file')
        filename = random_string() + '.txt'
        with open(filename, 'w') as f:
            f.write(random_string(100))
        
        self.assertTrue(client.upload_file(localFilePath=filename, object_name=filename))
        self.assertIsNotNone(client.object_info(filename))
        os.remove(filename)

        # Download a file
        print(f'Downloading file')
        self.assertTrue(client.download_file(outputFilePath=filename, object_name=filename))
        os.remove(filename)

        # Delete container
        self.assertFalse(client.container_delete(container_name), 'container_delete() should not delete a container that is not empty')

        # Delete objects
        print('Deleting objects')
        objects = client.object_list()

        self.assertTrue(client.object_delete(objects[0].name), 'object_delete() should return true on success')
        self.assertTrue(client.object_delete(objects[0].name + '123'), 'object_delete() should return true if the file does not exist')

        for o in objects:
            client.object_delete(o.name)

        self.assertTrue(len(client.object_list(container_name=container_name)) == 0, 'object_delete() should properly remove objects')

        self.assertTrue(client.container_delete(container_name), 'container_delete() should return true on success')
        self.assertIsNone(client.container_name, 'container_delete() should set the active container name to None upon successful delete')

        print('Force delete a container')
        # Create a non-empty container
        container_name = container_prefix + random_string()
        self.assertTrue(client.use_container(container_name, create=True), 'use_container(create=true) should return true on success')
        self.assertIsNotNone(client.container_info(container_name), 'use_container(create=true) should create the container if it does not exist')
        client.object_upload(stream=io.BytesIO(os.urandom(100)), object_name=random_string())

        # container_delete(force=True)
        self.assertTrue(client.container_delete(container_name, force=True), 'container_delete(force=True) should return true on success')
        self.assertIsNone(client.container_info(container_name), 'container_delete(force=True) should be able to delete a non-empty container')

        # object_*() should raise exception if container is not set
        print('Rasing exceptions')
        self.assertFalse(client.use_container(container_prefix + random_string(20)), 'use_container() should return false on failure')
        self.assertRaises(ContainerNotSpecified, client.object_delete, 'object_name')
        self.assertRaises(ContainerNotSpecified, client.object_info, 'object_name')
        self.assertRaises(ContainerNotSpecified, client.object_list)
        self.assertRaises(ContainerNotSpecified, client.object_upload, object_name='obj', stream=None)
        self.assertRaises(ContainerNotSpecified, client.object_download, object_name='obj', stream=None)
        self.assertRaises(ContainerNotSpecified, client.object_replace_metadata, object_name='obj', metadata= {})
        self.assertRaises(ContainerNotSpecified, client.object_set_metadata, object_name='obj', key='key', value='value')
        self.assertRaises(ContainerNotSpecified, client.object_delete_metadata, object_name='obj', key='key')

        # List created containers
        print('Removing all test containers that were created')
        containers = client.container_list(prefix=container_prefix)
        for c in containers:
            print(f' - Deleting {c.name}')
            client.container_delete(c.name, force=True)
        


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] not in ['s3','swift']:
        print('Usage:')
        print('   python -m tests.tests s3 <aws-location>')
        print('   python -m tests.tests swift <swift-storage-url>')
        exit()
    else:
        backend = sys.argv[1]

        if backend == 's3':
            TestCases.storage['location'] = sys.argv.pop()
        else:
            TestCases.storage['region'] = sys.argv.pop()

        TestCases.storage['backend'] = sys.argv.pop()

    unittest.main()