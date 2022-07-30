
from abc import abstractclassmethod, abstractmethod


class ObjectStorageClient:
    """Abstract class that defines a generic object storage API. Subclass this class to support a new object storage backend."""

    container_name = None

    #
    #   Common implementation
    #

    def use_container(self, container_name: str):
        """Set the target container name"""
        self.container_name = container_name

    def object_path(self, object_name: str, container_name = None) -> str:
        """Build the object path"""
        path = object_name if object_name.startswith('/') else '/' + object_name
        if not container_name: container_name = self.container_name;
        if container_name != None:
            path = '/' + container_name + path
            path = path.replace('//', '/')
        return path

    def upload_file(self, localFilePath: str, object_name: str, meta: dict={}) -> bool:
        """Upload a file with, optionally specifying some metadata to apply to the object"""
        with open(localFilePath, 'rb') as file:
            ok = self.object_upload(file, object_name, meta)    
        return ok

    #
    #   Abstract functions to implement when subclassing
    #

    # Container related actions
    
    def container_create(self, container_name: str):
        """Create a new container"""
        raise NotImplementedError

    def container_list(self):
        """List available containers"""
        raise NotImplementedError


    # Object related actions

    def object_get_metadata(self, object_name: str) -> dict:
        """Return an objet's metadata (key-value string pairs where the key are lowercased)"""
        raise NotImplementedError

    def object_get_info(self, object_name: str) -> dict:
        """Return an objet's info (including metadata)"""
        raise NotImplementedError

    def object_set_metadata(self, object_name: str, key: str, value: str) -> bool:
        """Sets a single metadata key-value pair on the specified object"""
        raise NotImplementedError

    def object_delete_metadata(self, object_name: str, key: str) -> dict:
        """Delete a single metadata key-value for the specified object"""
        raise NotImplementedError

    def object_upload(self, stream, object_name: str, meta: dict={}) -> bool:
        """Upload a stream, optionally specifying some metadata to apply to the object"""
        raise NotImplementedError

    def object_download(self, object_name: str, outputFilePath: str = None) -> bool:
        """ 
        Download an object. Saves it to the specified outputFilePath if specified, otherwise print the content to stdout.
        """
        raise NotImplementedError

    def object_list(self,
        fetch_metadata = False,
        prefix: str = None,
        delimiter: str = None,
    ) -> dict:
        """List available objects in the selected container"""
        raise NotImplementedError

    def object_delete(self, object_name):
        """Delete the specified object"""
        raise NotImplementedError
