#!/bin/python
#
#   OpenStack Swift Client
#   API Reference: https://docs.openstack.org/api-ref/object-store/
#

import os, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .ObjectStorageClient import *

class SwiftClient(ObjectStorageClient):

    def __init__(self, region: str, credentials: dict = {}) -> None:
        self.OBJECT_STORAGE_URL = None
        self.region = region
        self.session = requests.Session()
        self.session.hooks = {'response': [self._response_hook]} # Set a response hook to handle authentication errors
        self.authenticate(credentials)

    # This hook is called before each response is returned from
    # the request execution. We use it to handle authentication
    # failure. If there is an auth failure, we re-authenticate
    # and retry the request before raising an exception if we
    # still have a failure. This allows to renew an expired
    # token transparently without raising an error.
    def _response_hook(self, resp: requests.Response, *args, **kwargs):
        """Called before returning the response"""
        auth_status_codes = [401, 403]
        if resp.status_code in auth_status_codes:
            self.session.close()
            self.authenticate() # Renew token
            req = resp.request.copy() # Copy the original request
            req.headers['X-Auth-Token'] = self.OS_AUTH_TOKEN # Update the auth token
            req.hooks = None # To avoid infinite retry, we clear the hooks
            res : requests.Response = requests.Session().send(request=req)
            if res.status_code in auth_status_codes:
                raise AuthorizationError
            else:
                return res
        return resp


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
        
        if not self.OS_AUTH_URL:
            print("The environment does not seem to contain OpenStack credentials or token")
            raise AuthorizationError

    def authenticate(self, credentials : dict = {}) -> bool:
        """Retreives a usable authentication token"""
        self.read_credentials_from_env(credentials)

        # See https://docs.openstack.org/api-ref/identity/v3/#authentication-and-token-management
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
            # Retreive the auth header from the server reply
            self.OS_AUTH_TOKEN = r.headers.get('X-Subject-Token')
            # Assign the auth header to the client session
            self.session.headers['X-Auth-Token'] = self.OS_AUTH_TOKEN

            # Retreive the storage URL from the server reply
            self.OBJECT_STORAGE_URL = None
            catalog = r.json().get('token', {}).get('catalog', [])
            endpoints = next((e['endpoints'] for e in catalog if e['type'] == 'object-store'), None)
            if endpoints is not None:
                self.OBJECT_STORAGE_URL = next((e['url'] for e in endpoints if e['interface'] == 'public' and e['region'] == self.region), None)

            if self.OBJECT_STORAGE_URL is None:
                raise ObjectStorageClientError(f"Storage URL not found in server reply for region '{self.region}'")

            return True
        else:
            # print(f"AuthenticationRequestFailed: HttpResponseStatus={r.status_code} with content {r.content}")
            raise AuthorizationError(f"HttpResponseStatus={r.status_code} ResponseContent={r.content}")

    def container_info(self, container_name: str) -> ContainerInfo|None:
        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        r = self.session.head(url)
        meta = {}
        for h in r.headers:
            if h.startswith('X-Object-Meta-'):
                meta[h.removeprefix('X-Object-Meta-').lower()] = r.headers[h]
        
        if r.status_code == 204:
            return ContainerInfo(
                name=container_name,
                count=int(r.headers.get('X-Container-Object-Count')),
                bytes=int(r.headers.get('Content-Length')) if r.headers.get('Content-Length') is not None else None
            )
        elif r.status_code != 404:
            print(f"ERROR: get_object_info({container_name}) got status code: {r.status_code} {r.content}")


    def container_list(self, prefix: str = None) -> list[ContainerInfo]:
        url = f"{self.OBJECT_STORAGE_URL}"
        params = {"format":"json"}
        if prefix: params['prefix'] = prefix
        r = self.session.get(url, params=params)
        objList = r.json()
        return [ContainerInfo(o.get('name'), o.get('bytes'), o.get('count')) for o in objList]

    def container_create(self, container_name: str) -> bool:
        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        r = self.session.put(url)
        if r.status_code not in [201, 202]:
            print('container_create() status code:', r.status_code)
        return r.status_code == 201

    def container_delete(self, container_name: str, force: bool = False) -> bool:
        if force:
            # First delete all objects in the container, otherwise the delete request will fail
            objects = self.object_list(container_name=container_name)
            for o in objects:
                self.object_delete(o.name, container_name)

        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        r = self.session.delete(url)
        if r.status_code not in [204, 404, 409]:
            print('container_delete() status code:', r.status_code)
        if r.status_code in [204, 404]:
            # Success
            self.container_name = None
            return True
        else:
            return False


    def object_info(self, object_name: str, container_name: str = None) -> ObjectInfo|None:
        """Return an objet's info (including metadata)"""
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, container_name)}"
        r = self.session.head(url)
        meta = {}
        for h in r.headers:
            if h.lower().startswith('x-object-meta-'):
                meta[h.lower().removeprefix('x-object-meta-')] = r.headers[h]
        
        if r.status_code == 200:
            return ObjectInfo(
                name=object_name,
                bytes=int(r.headers.get('Content-Length', 0)),
                hash=r.headers.get('Etag'),
                content_type=r.headers.get('Content-Type'),
                metadata=meta,
                last_modified=float(r.headers.get('X-Timestamp'))
            )
        elif r.status_code == 204:
            return None
        elif r.status_code != 404:
            print(f"ERROR: get_object_info({object_name}) got status code: {r.status_code} {r.content}")

    def object_replace_metadata(self, object_name: str, metadata: dict, container_name: str = None) -> bool:
        """
        Function to set all the object's metadata
        - `meta`: dict of key-value string pairs. Keys are case insensitive.
        """
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, container_name)}"
        headers = {'X-Auth-Token': self.OS_AUTH_TOKEN}
        for m in metadata:
            headers[f'X-Object-Meta-{m}'] = metadata[m]

        r = self.session.post(url, headers=headers)
        return r.status_code == 202

    def object_upload(self, stream, object_name: str, metadata: dict={}, container_name: str = None) -> bool:
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, container_name)}"
        headers={'X-Auth-Token': self.OS_AUTH_TOKEN}
        for m in metadata:
            headers[f'X-Object-Meta-{m}'] = metadata[m] # Add metadata
        r = self.session.put(url, headers=headers, data=stream)
        if r.status_code != 201:
            print('Upload status code:', r.status_code)
        return r.status_code == 201

    def object_download(self, object_name: str, stream, container_name: str = None) -> bool:
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, container_name)}"
        r = self.session.get(url, stream=True)
        if r.status_code == 200:
            for chunk in r.iter_content():
                stream.write(chunk)
            return True
        else:
            # print(f"Request status is {r.status_code} with content {r.content}")
            return False # Could not download

    def object_list(self,
        fetch_metadata: bool = False,
        prefix: str = None,
        delimiter: str = None,
        container_name: str = None,
    ) -> list[ObjectInfo]:
        # See https://docs.openstack.org/api-ref/object-store/?expanded=show-container-details-and-list-objects-detail#show-container-details-and-list-objects

        if container_name is None:
            container_name = self.container_name

        if container_name is None:
            raise ContainerNotSpecified
        
        url = f"{self.OBJECT_STORAGE_URL}/{container_name}"
        params = {"format":"json"}
        if prefix: params['prefix'] = prefix
        if delimiter: params['delimiter'] = delimiter
        r = self.session.get(url, params=params)

        if r.status_code != 200:
            return []
        else:
            objects = r.json()
            for i in range(0, len(objects)):
                o = objects[i]
                if 'subdir' in o:
                    objects[i] = SubdirInfo(o['subdir'])
                else:
                    # By default the endpoint returns a ISO string in the format "2022-12-13T18:05:00.378500" (UTC).
                    # If the trailing +00 is not added, python assumes it is a local timestamp, not UTC.
                    iso = o.get('last_modified')
                    if '+' not in iso.split('T')[1] and '-' not in iso.split('T')[1]:
                        iso += '+00:00' # Ensure we have an offset specified
                    
                    objects[i] = ObjectInfo(
                        name=o.get('name'),
                        bytes=o.get('bytes'),
                        hash=o.get('hash'),
                        content_type=o.get('content_type'),
                        metadata=None,
                        last_modified=datetime.fromisoformat(iso).timestamp()
                    )

            if fetch_metadata:
                obj_indices = [i for i, obj in enumerate(objects) if isinstance(obj, ObjectInfo)]
                with ThreadPoolExecutor() as executor:
                    future_to_index = {executor.submit(self.object_info, objects[i].name, container_name=container_name): i for i in obj_indices}
                    for future in as_completed(future_to_index):
                        i = future_to_index[future]
                        try:
                            meta_obj = future.result()
                            if meta_obj:
                                objects[i].metadata = meta_obj.metadata
                        except Exception:
                            pass

            return objects

    def object_delete(self, object_name: str, container_name:str = None) -> bool:
        if container_name is None:
            container_name = self.container_name
        # print('object_delete()', object_name)
        url = f"{self.OBJECT_STORAGE_URL}{self.object_path(object_name, container_name)}"
        r = self.session.delete(url)
        return r.status_code == 204 or r.status_code == 404
