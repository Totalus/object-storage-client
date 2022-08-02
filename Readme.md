
# Universal Object Storage Client

The purpose of thie project is to provide a python client that provides a simple straightforward API to manage objects on various object storage backends (AWS S3, OpenStack Swift, etc.). Providing a common API allows other projects to easily support multiple storage backends or change storage backend without needing to change the code, which prevents vendor lock-in.

Storage backend currently supported:
- OpenStack Swift
- AWS S3 (not implemented yet)

## Install / Upgrade

`pip install --upgrade https://github.com/Totalus/object-storage-client/tarball/main`

## Storage backends

### OpenStack Swift

```py
from obs_client import SwiftClient

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
# Create a container
client.container_create('my-bucket')

# Select the container to work on
client.use_container('my-bucket')

# Return the list of objects in the selected container
objects = client.object_list()
for o in objects:
    print(f"{o.name} ({o.size} bytes)")

info = client.object_info(objects[0].name)
print(f"metadata for {object[0].name} : {object[0].metadata}")

# Upload a file (equivalent to client.upload_file())
with open('file.txt', 'rb') as f:
    client.object_upload(f, 'my-object.txt')

# Download a file (equivalent to client.download_file())
with open('file.txt', 'wb') as f:
    client.object_downalod('my-object.txt', f)

# Send file content to stdout
client.object_download('my-object.txt', sys.stdout.buffer)
```

Refer to [`ObjectStorageClient.py`](./src/ObjectStorageClient.py) for the full list of available methods and their description.


## Adding storage backend

To add new storage backends, subclass the `ObjectStorageClient` class and implement the abstract methods. The constructor of the subclass can be used to set the credentials.

## Tests

`python -m test.tests <storage-url>`