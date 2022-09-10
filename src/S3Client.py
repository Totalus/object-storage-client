#!/bin/python
#
#   AWS S3 Client
#   API Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
#   (error handling) https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html#parsing-error-responses-and-catching-exceptions-from-aws-services
#
#
#   - For metadata, we use tags as they are more versatile and can be changed without re-uploading the entire object.

import boto3, botocore

from .ObjectStorageClient import *

class S3Client(ObjectStorageClient):
    
    def __init__(self, location):
        self.client = boto3.client('s3')
        self.location = location
    
    # Container related actions
    
    def container_create(self, container_name: str) -> bool:
        """
        Create a new container. This request might take few seconds to complete.

        @param `container_name` The new container name
        @return True on success, False on failure (ex: already exists)
        """
        try:
            res = self.client.create_bucket(Bucket=container_name, CreateBucketConfiguration={ "LocationConstraint": self.location })
            return res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200
        except:
            return False

    def container_list(self, prefix: str = None) -> list[ContainerInfo]:
        buckets = self.client.list_buckets().get('Buckets', [])
        return [ ContainerInfo(b['Name'], None, None) for b in buckets if prefix is None or b['Name'].startswith(prefix) ]

    def container_delete(self, container_name: str, force: bool = False) -> bool:
        """
        Delete a container. It will not delete a container that contain objects unless `force`
        is set to True.

        @param `force` Set to True to delete a container even if it is not empty.
        @return True if the container was deleted or does not exist
        """
        try:
            res = self.client.delete_bucket(Bucket=container_name)
            print("container_deletes() res=", res)
            return True
        except:
            return False

    def container_info(self, container_name: str) -> ContainerInfo:
        """
        Fetch container information

        @return ContainerInfo or None if the container does not exist
        """
        try:
            result = self.client.head_bucket(Bucket=container_name)
        except botocore.exceptions.ClientError as e:
            result = e.response

        if result.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
            return ContainerInfo(container_name, None, None)
        elif result.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
            return None
        else:
            print('S3Client: unknown error code:', result.get('ResponseMetadata', {}).get('HTTPStatusCode'))
            return None

    # Object related actions

    def object_info(self, object_name: str, container_name: str = None) -> ObjectInfo|None:
        try:
            res = self.client.head_object(Bucket=self.get_container(container_name), Key=object_name)
        except botocore.exceptions.ClientError as e:
            res = e.response
        
        if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
            return ObjectInfo(
                name=object_name,
                bytes=res.get('ContentLength'),
                content_type=res.get('ContentType'),
                hash=res.get('ETag').replace('"',''),
                metadata=res.get('Metadata')
            )
        elif res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
            return None
        else:
            print(f"S3Client: object_info() status code: {res.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
            return None

    def object_replace_metadata(self, object_name: str, metadata: dict = {}, container_name: str = None) -> bool:
        try:
            res = self.client.copy_object(
                Bucket=self.get_container(container_name),
                Key=object_name,
                CopySource={'Bucket':self.container_name, 'Key': object_name},
                Metadata=metadata,
                MetadataDirective='REPLACE'
            )
        except botocore.exceptions.ClientError as e:
            res = e.response

        return res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200


    def object_delete_metadata(self, object_name: str, key: str) -> dict:
        """Delete a single metadata key-value for the specified object"""
        raise NotImplementedError

    def object_upload(self, stream, object_name: str, metadata: dict={}, container_name: str = None) -> bool:
        """Upload a stream, optionally specifying some metadata to apply to the object"""

        res = self.client.put_object(
            Body=stream,
            Bucket=self.get_container(container_name),
            Key=object_name,
            Metadata=metadata
        )

        if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
            return True
        else:
            print(f"S3Client: object_upload() status code: {res.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
            return False


    def object_download(self, object_name: str, stream) -> bool:
        """ 
        Download an object and write to the output stream
        """
        raise NotImplementedError

    def object_list(self,
        fetch_metadata: bool = False,
        prefix: str = None,
        delimiter: str = None,
        container_name: str = None,
    ) -> list[ObjectInfo|SubdirInfo]:
        
        if container_name is None:
            container_name = self.container_name

        args = {"Bucket": container_name}
        if prefix: args['Prefix'] = prefix
        if delimiter: args['Delimiter'] = delimiter

        res = self.client.list_objects_v2(**args)

        if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
            objects =  [ObjectInfo(o['Key'], o['Size'], o['ETag'], None, None) for o in res.get('Contents', [])]
            subdirs =  [SubdirInfo(o['Prefix']) for o in res.get('CommonPrefixes', [])]
            if fetch_metadata:
                for i in range(0, len(objects)):
                    objects[i] = self.object_info(objects[i].name)
            objects.extend(subdirs)
            return objects
        else:
            return []

    def object_delete(self, object_name: str, container_name: str = None) -> bool:
        """Delete the specified object"""
        raise NotImplementedError


    

if __name__ == "__main__":

    client = S3Client('us-west-2')

    client.use_container("firmware-autonom")
    print(client.object_list(delimiter='6', fetch_metadata=True))
    # client.object_info("test.txt")

    # client.container_create('universal-object-storage-client-test-container-1231321321312')
    # client.container_delete('universal-object-storage-client-test-container-1231321321312')
