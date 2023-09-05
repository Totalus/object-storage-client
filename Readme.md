
# Universal Object Storage Client

The purpose of thie project is to provide a python client that provides a simple straightforward API to manage objects on various object storage backends (AWS S3, OpenStack Swift, etc.). Providing a common API allows other projects to easily support multiple storage backends or change storpage backend without needing to change the code, which prevents vendor lock-in.

Storage backend currently supported:
- OpenStack Swift
- AWS S3

## Install / Upgrade

Latest stable version (v2): `pip install --upgrade https://github.com/Totalus/object-storage-client/tarball/v2`

Specific release: `pip install --upgrade <release-tar-gz>` (available [releases](https://github.com/Totalus/object-storage-client/releases))

## Storage backends

### AWS S3

```py
from obs_client import S3Client

client = S3Client(location="us-west-2")
```

The `location` parameter is mainly used by `container_create()` to decide where to create the container, but you can work with existing containers in any location.

The S3Client is based on `boto3` which picks up the credentials automatically from the environment or a credential file. Refer to its [documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).


### OpenStack Swift

```py
from obs_client import SwiftClient

client = SwiftClient(region="<your-openstack-swift-container-region>")
```

The client will automatically pick up the credentials if they are in the environment. You can also provide them manually:

```py
# Provide the required credentials
client = SwiftClient(region="<your-openstack-swift-container-region>",
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

The above credentials are required to authenticate to the storage backend and retreive an authentication token.

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
    client.object_download('my-object.txt', f)

# Send file content to stdout
client.object_download('my-object.txt', sys.stdout.buffer)
```

Refer to [`ObjectStorageClient.py`](./src/ObjectStorageClient.py) for the full list of available methods and their description.


## CLI usage

The library can also be used as a CLI to interact with your storage backend.

Quick usage example:
```bash
# Print command list
$ python -m obs_client --help

# Print help for specific command
$ python -m obs_client <command> --help

# Since we're lazy, lets create an alias to avoid re-typing `python -m obs_client` each time
alias obs='python -m obs_client'
# Now we can use `obs` instead of `python -m obs_client`, much better

# Test configuration and connectivity / print help on how to configure
$ obs test-config

# Configure storage backend (only one of the two below should be set)
export OBS_SWIFT_REGION='GHB'     # For Openstack Swift
export OS_S3_LOCATION='us-west-2' # For AWS

# Once configured we can test again to see if it is connected
$ obs test-config

# List containers
$ obs container-list

# Create a container
$ obs container-create my-container

# Upload a file (with metadata)
$ obs upload --file my-file.txt my-container/my-file.txt --meta key1=value1 --meta key2=value2

# Download a file
$ obs download my-container/my-file.txt --file my-file.txt

# Print object or container info
$ obs info my-container
$ obs info my-container/my-object.txt

# List objects with a given prefix
$ obs list my-container         # List all objects in my-container
$ obs list my-container/obj_    # List all objects in my-container that have the prefix 'obj_'

# Browse object storage as a file system
$ obs ls
$ obs ls my-container
$ obs ls my-container/my-*

# There are more commands available, you can list them with the `--help` option
$ obs --help
```

## Adding storage backend

To add new storage backends, subclass the `ObjectStorageClient` class and implement the abstract methods. The constructor of the subclass can be used to set the credentials and other backend-specific parameters.

## Tests

The test suite helps to keep a consistent behavior for each implementation.

- Test S3Client: `python -m tests.tests s3 <location>`
- Test SwifClient: `python -m tests.tests swift <swift-region>`