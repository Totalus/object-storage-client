
# Universal Object Storage Client

The purpose of thie project is to provide a python client that provides a simple straightforward API to manage objects on various storage backend (AWS S3, OpenStack Swift, etc.). Providing a common API allows other projects to easily support multiple storage backend or change storage backend without needing to change the code and avoid vendor lockdown.

Storage backend currently supported:
- OpenStack Swift

## Install / Upgrade

`pip install --upgrade https://github.com/Totalus/object-storage-client/tarball/main`

## Storage backends

### OpenStack Swift

```py
from universal_osc import SwiftClient

client = SwiftClient(storage_url="https://<storage-domain>/v1/AUTH_<more_stuff_here>", container_name="my-container")
```

The client will automatically pick up the credentials if they are in the environment (if you run `source openrc.sh`). You can also provide them manually:

```py
# Provide the required credentials
client = SwiftClient(storage_url="", container_name="my-container",
    credentials={
        # Required credentials (example)
        "OS_AUTH_URL": "https://<auth-domain>/v3/"
        "OS_USER_DOMAIN_NAME": "Default"
        "OS_USERNAME": "<your-openstack-username>"
        "OS_PASSWORD": "<your-openstack-password>"
        "OS_PROJECT_DOMAIN_NAME": "Default"             # or OS_TENANT_NAME
        "OS_PROJECT_NAME": "1234567890123456"           # or OS_TENANT_ID
    }
)
```

The above credentials are required to authenticate to the storage backend and retreive an authentication token. You can however provide directly a valid authentication token through `OS_AUTH_TOKEN` instead.

## API usage

Once the storage backend is configured, the api used is the same for any storage backend.

```py
# Select the bucket to work on
client.use_container('my-bucket') 

# Return the list of objects in this bucket
client.object_list() 

# Upload a file, set the metadata key 'version' to '1.0'
client.upload_file(localFilePath='./my-file.txt', object_name='subdir/my-file.txt', meta={ 'version': '1.0' })

# Update the 'version' metadata to '1.1'
client.object_set_metadata('subdir/my-file.txt' 'version', '1.1')

# and more...
```

Refer to [`ObjectStorageClient.py`](./src/ObjectStorageClient.py) for the full list of available methods and their description.

## Adding storage backend

To add new storage backends, you simply need to subclass the ObjectStorageClient class and implement the abstract methods. The constructor of the subclass can be used to set the credentials.
