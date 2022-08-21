
from abc import abstractclassmethod, abstractmethod
from dataclasses import dataclass

class ObjectStorageClientError(Exception):
    """Custom exceptions"""
    pass

class AuthorizationError(ObjectStorageClientError):
    """User not allowed to perform the action"""
    pass

class ContainerNotSpecified(ObjectStorageClientError):
    pass


@dataclass
class ContainerInfo:
    name: str
    bytes: int|None          # Total  number of bytes in the container
    count: int|None          # Number of objects in the container

@dataclass
class ObjectInfo:
    name: str           # Name of the object
    bytes: int|None     # Size
    hash: str|None      # Hash (usualy md5)
    content_type: str|None
    metadata: dict[str, str]|None

@dataclass
class SubdirInfo:
    subdir: str         # Directory subpath

class ObjectStorageClient:
    """Abstract class that defines a generic object storage API. Subclass this class to support a new object storage backend."""

    container_name = None

    #
    #   Common implementation
    #

    def use_container(self, container_name: str | None, create=False) -> bool:
        """
        Set the target container name

        @param `container_name` name of the container to set (or None to clear the active container)
        @param `create` create the container if it does not already exist
        @return True on success, False if the container does not exist and cannot be created
        """
        if container_name is None:
            self.container_name = container_name
            return True

        # Does the container exist ?
        info = self.container_info(container_name)
        if info is not None:
            # Container already exists
            self.container_name = container_name
            return True
        elif create and self.container_create(container_name):
            self.container_name = container_name
            return True

        return False

    def object_path(self, object_name: str, container_name: str|None = None) -> str:
        """
        Build the object path that can be appened to the object storage url

        @raise NoActiveContainer if the active container is not specified
        """
        if container_name is None:
            container_name = self.container_name

        if container_name is None:
            raise ContainerNotSpecified

        path = object_name if object_name.startswith('/') else '/' + object_name
        path = '/' + container_name + path
        path = path.replace('//', '/')
        return path

    def upload_file(self, localFilePath: str, object_name: str, metadata: dict={}, container_name: str = None) -> bool:
        """Upload a file with, optionally specifying some metadata to apply to the object"""
        with open(localFilePath, 'rb') as file:
            ok = self.object_upload(
                file=file,
                object_name=object_name,
                metadata=metadata,
                container_name=container_name
                )
        return ok

    def download_file(self, object_name: str, outputFilePath: str, container_name: str = None) -> bool:
        """Download a file"""
        with open(outputFilePath, 'wb') as file:
            ok = self.object_download(
                object_name=object_name, 
                stream=file,
                container_name=container_name
                )
        return ok

    def object_set_metadata(self, object_name: str, key: str, value: str, container_name: str = None) -> bool:
        """Sets a single metadata key-value pair on the specified object"""
        info = self.object_info(
            object_name=object_name,
            container_name=container_name,
            )
        if info is None:
            return False
        info.metadata[key] = value
        return self.object_replace_metadata(
            object_name=object_name,
            metadata=info.metadata,
            container_name=container_name,
            )

    def object_delete_metadata(self, object_name: str, key: str, container_name: str = None) -> dict:
        """Delete a single metadata key-value for the specified object"""
        info = self.object_info(object_name)
        if info is None:
            return False
        if key in info.metadata:
            del info.metadata[key]
            return self.object_replace_metadata(
                object_name=object_name,
                metadata=info.metadata,
                container_name=container_name,
                )
        else:
            return True # Key not in the metadata

    #
    #   Abstract functions to implement when subclassing
    #

    # Container related actions
    
    def container_create(self, container_name: str) -> bool:
        """
        Create a new container

        @param `container_name` The name of the new container
        @return frue on success, false on failure (ex: already exists)
        """
        raise NotImplementedError

    def container_list(self, prefix: str = None) -> list[ContainerInfo]:
        """
        List available containers

        @param prefix Set to return only containers with that prefix
        @return A list of dictionaries with info on each the containers (name, size, object count, ...)
        """
        raise NotImplementedError

    def container_delete(self, container_name: str, force: bool = False) -> bool:
        """
        Delete a container. It will not delete a container that contain objects unless `force`
        is set to True.

        @param `force` Set to True to delete a container even if it is not empty.
        @return True if the container was deleted or does not exist
        """
        raise NotImplementedError

    def container_info(self, container_name: str) -> ContainerInfo|None:
        """
        Fetch container information

        @return ContainerInfo or None if the container does not exist
        """
        raise NotImplementedError

    # Object related actions

    def object_replace_metadata(self, object_name: str, metadata: dict = {}, container_name: str = None) -> bool:
        """
        Replace an object's metadata

        @returns true on success, false on failure
        """
        raise NotImplementedError

    def object_info(self, object_name: str, container_name: str = None) -> ObjectInfo|None:
        """
        Return an objet's info (including metadata)

        @return ObjectInfo or None if the object does not exist
        """
        raise NotImplementedError

    def object_upload(self, stream, object_name: str, metadata: dict={}, container_name: str = None) -> bool:
        """
        Upload a stream, optionally specifying some metadata to apply to the object

        @return true on success, false on failure
        """
        raise NotImplementedError

    def object_download(self, object_name: str, stream, container_name: str = None) -> bool:
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
        """
        List available objects in the specified container. If `container_name` is not specified,
        lists objects in the active container (see `use_container()`).

        If `delimiter` is set, it may return a mix of ObjectInfo and SubdirInfo that represent the objects
        and subdir found following the prefix. The delimiter allows to browse as if it was a file system.

        @param `prefix` : if set, filter the results that start with the given prefix
        @param `fetch_metadata` : if `True`, also fetch the objects metadata
        """
        raise NotImplementedError

    def object_delete(self, object_name: str, container_name: str = None) -> bool:
        """Delete the specified object"""
        raise NotImplementedError
