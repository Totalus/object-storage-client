#!/bin/python
#
#   OpenStack Swift Client
#   API Reference: https://docs.openstack.org/api-ref/object-store/
#

import os, requests

from .ObjectStorageClient import *

class SwiftClient(ObjectStorageClient):

    def __init__(self, storage_url: str, credentials: dict = {}) -> None:
        self.OBJECT_STORAGE_URL = storage_url[0:-1] if storage_url.endswith('/') else storage_url # Remove trailing /
        self.authenticate(credentials)

    def read_credentials_from_env(self, credentials: dict):
        """
        Import OpenStack credentials from environment variables
        - credentials: optionally provide an object with the credentials
        """
        self.OS_AUTH_URL= credentials.get('OS_AUTH_URL') or os.getenv("OS_AUTH_URL")
        self.OS_TENANT_ID= credentials.get('OS_TENANT_ID') or os.getenv("OS_TENANT_ID")
        self.OS_REGION_NAME= credentials.get('OS_REGION_NAME') or os.getenv("OS_REGION_NAME")
        self.OS_PROJECT_DOMAIN_NAME= credentials.get('OS_PROJECT_DOMAIN_NAME') or os.getenv("OS_PROJECT_DOMAIN_NAME")
        self.OS_USER_DOMAIN_NAME= credentials.get('OS_USER_DOMAIN_NAME') or os.getenv("OS_USER_DOMAIN_NAME")
        self.OS_IDENTITY_API_VERSION= credentials.get('OS_IDENTITY_API_VERSION') or os.getenv("OS_IDENTITY_API_VERSION")
        self.OS_TENANT_NAME= credentials.get('OS_TENANT_NAME') or os.getenv("OS_TENANT_NAME")
        self.OS_PASSWORD= credentials.get('OS_PASSWORD') or os.getenv("OS_PASSWORD")
        self.OS_USERNAME= credentials.get('OS_USERNAME') or os.getenv("OS_USERNAME")
        self.OS_PROJECT_NAME= credentials.get('OS_PROJECT_NAME') or os.getenv("OS_PROJECT_NAME")
        self.OS_PROJECT_ID= credentials.get('OS_PROJECT_ID') or os.getenv("OS_PROJECT_ID")

        # OS_PROJECT_NAME and OS_PROJECT_ID are the new names for OS_TENANT_NAME and OS_TENANT_ID
        if self.OS_PROJECT_NAME == None: self.OS_PROJECT_NAME = self.OS_TENANT_NAME;
        if self.OS_PROJECT_ID == None: self.OS_PROJECT_ID = self.OS_TENANT_ID;

        self.OS_AUTH_TOKEN  = os.getenv("OS_AUTH_TOKEN") # Check if an auth token was provided in the environment

        if not self.OS_AUTH_TOKEN and not self.OS_AUTH_URL:
            print("The environment does not seem to contain OpenStack credentials or token")
            return False

        return True

    def authenticate(self, credentials : dict = {}) -> bool:
        """Retreives a usable authentication token"""
        if not self.read_credentials_from_env(credentials):
            return False

        auth_url = self.OS_AUTH_URL + ('' if self.OS_AUTH_URL.endswith('/') else '/') + "auth/tokens"
        r = requests.post(auth_url, json={
            "auth": {
                "identity": {
                "methods": ["password"],
                    "password": {
                        "user": {
                            "domain": {"name": self.OS_USER_DOMAIN_NAME},
                            "name": self.OS_USERNAME,
                            "password": self.OS_PASSWORD
                        }
                    }
                },
                "scope": {
                    "project": {
                        "domain": {"name": self.OS_PROJECT_DOMAIN_NAME},
                            "name": self.OS_PROJECT_NAME
                    }
                }
            }
        })

        if r.status_code == 201:
            self.OS_AUTH_TOKEN = r.headers.get('X-Subject-Token')
            return True
        else:
            print(f"AuthenticationRequestFailed: HttpResponseStatus={r.status_code} with content {r.content}")
            return False

    def container_info(self, container_name: str = None) -> ContainerInfo:
        if not container_name: container_name = self.container_name;
        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        r = requests.head(url, headers={'X-Auth-Token': self.OS_AUTH_TOKEN})
        meta = {}
        for h in r.headers:
            if h.startswith('X-Object-Meta-'):
                meta[h.removeprefix('X-Object-Meta-').lower()] = r.headers[h]
        
        if r.status_code == 204:
            return ContainerInfo(
                name=container_name,
                count=r.headers['X-Container-Object-Count'],
                bytes=r.headers['Content-Length']
            )
        elif r.status_code != 404:
            print(f"ERROR: get_object_info({container_name}) got status code: {r.status_code} {r.content}")


    def container_list(self, prefix: str = None) -> list[ContainerInfo]:
        url = f"{self.OBJECT_STORAGE_URL}"
        params = {"format":"json"}
        if prefix: params['prefix'] = prefix
        r = requests.get(url, params=params, headers={'X-Auth-Token': self.OS_AUTH_TOKEN})
        objList = r.json()
        return [ContainerInfo(o.get('name'), o.get('bytes'), o.get('count')) for o in objList]

    def container_create(self, container_name: str) -> bool:
        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        headers={'X-Auth-Token': self.OS_AUTH_TOKEN}
        r = requests.put(url, headers=headers)
        if r.status_code not in [201, 202]:
            print('container_create() status code:', r.status_code)
        return r.status_code == 201

    def container_delete(self, container_name: str, force: bool = False) -> bool:
        if force:
            # First delete all objects in the container, otherwise the delete request will fail
            objects = self.object_list(container_name=container_name)
            for o in objects:
                self.object_delete(self.object_path(o.name, container_name))

        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        headers={'X-Auth-Token': self.OS_AUTH_TOKEN}
        r = requests.delete(url, headers=headers)
        if r.status_code not in [204, 404, 409]:
            print('container_delete() status code:', r.status_code)
        return r.status_code == 204 or r.status_code == 404

    def _object_get_metadata(self, object_name: str, container_name: str = None) -> dict:
        """Return an objet's metadata (key-value string pairs where the key is lowercased)"""
        if not container_name: container_name = self.container_name;
        info = self.object_info(object_name, container_name)
        if info is not None:
           return info.metadata


    def object_info(self, object_name: str, container_name: str = None) -> ObjectInfo:
        """Return an objet's info (including metadata)"""
        if not container_name: container_name = self.container_name;
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, container_name)}"
        r = requests.head(url, headers={'X-Auth-Token': self.OS_AUTH_TOKEN})
        meta = {}
        for h in r.headers:
            if h.startswith('X-Object-Meta-'):
                meta[h.removeprefix('X-Object-Meta-').lower()] = r.headers[h]
        
        if r.status_code == 200:
            return ObjectInfo(
                name=object_name,
                bytes=int(r.headers['Content-Length']),
                hash=r.headers['Etag'],
                content_type=r.headers['Content-Type'],
                metadata=meta
            )
        elif r.status_code != 404:
            print(f"ERROR: get_object_info({object_name}) got status code: {r.status_code} {r.content}")

    def object_replace_metadata(self, object_name: str, meta: dict) -> bool:
        """
        Function to set all the object's metadata
        - `meta`: dict of key-value string pairs. Keys are case insensitive.
        """
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, self.container_name)}"
        headers = {'X-Auth-Token': self.OS_AUTH_TOKEN}
        for m in meta:
            headers[f'X-Object-Meta-{m}'] = meta[m]

        r = requests.post(url, headers=headers)
        return r.status_code == 202

    def object_set_metadata(self, object_name: str, key: str, value: str) -> bool:
        """Sets a single metadata key-value pair on the specified object"""
        meta = self._object_get_metadata(object_name)
        meta[key] = value
        return self.object_replace_metadata(object_name, meta)

    def object_delete_metadata(self, object_name: str, key: str) -> dict:
        """Delete a single element in the metadata on the specified object"""
        meta = self._object_get_metadata(object_name)
        if key in meta:
            del meta[key]
        return self.object_replace_metadata(object_name, meta)

    def object_path(self, object_name: str, container_name = None) -> str:
        """Build the object path"""
        path = object_name if object_name.startswith('/') else '/' + object_name
        if not container_name: container_name = self.container_name;
        if container_name != None:
            path = '/' + container_name + path
            path = path.replace('//', '/')
        return path

    def object_upload(self, stream, object_name: str, meta: dict={}) -> bool:
        """Upload a stream, optionally specifying some metadata to apply to the object"""
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, self.container_name)}"
        headers={'X-Auth-Token': self.OS_AUTH_TOKEN}
        for m in meta:
            headers[f'X-Object-Meta-{m}'] = meta[m] # Add metadata
        r = requests.put(url, headers=headers, data=stream)
        if r.status_code != 201:
            print('Upload status code:', r.status_code)
        return r.status_code == 201

    def object_download(self, object_name: str, stream) -> bool:
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, self.container_name)}"
        r = requests.get(url, headers={'X-Auth-Token': self.OS_AUTH_TOKEN}, stream=True)
        if r.status_code == 200:
            for chunk in r.iter_content():
                stream.write(chunk)
            return True
        else:
            print(f"Request status is {r.status_code} with content {r.content}")
            return False # Could not download

    def object_list(self,
        fetch_metadata: bool = False,
        prefix: str = None,
        delimiter: str = None,
        container_name: str = None,
    ) -> list[ObjectInfo]:
        # See https://docs.openstack.org/api-ref/object-store/?expanded=show-container-details-and-list-objects-detail#show-container-details-and-list-objects

        if container_name is None: container_name = self.container_name
        
        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        params = {"format":"json"}
        if prefix: params['prefix'] = prefix
        if delimiter: params['delimiter'] = delimiter
        r = requests.get(url, params=params, headers={'X-Auth-Token': self.OS_AUTH_TOKEN})

        if r.status_code != 200:
            return []
        else:
            objects = r.json()
            objects = [ObjectInfo(o.get('name'), o.get('bytes'), o.get('hash'), o.get('content_type'), None) for o in objects]

            if fetch_metadata:
                for obj in objects:
                    obj.metadata = self._object_get_metadata(obj.name, container_name)

            return objects

    def object_delete(self, object_name):
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, self.container_name)}"
        r = requests.delete(url, headers={'X-Auth-Token': self.OS_AUTH_TOKEN})
        return r.status_code == 204 or r.status_code == 404
