
import argparse, os

from .SwiftClient import *
from .S3Client import *


parser = argparse.ArgumentParser(
    prog="obs_client",
    description="Object Storage Client CLI tool that simplify managing object storage",
    epilog="This is an open source project: https://github.com/Totalus/object-storage-client"
)


subparsers = parser.add_subparsers(dest="command", required=True)

sp = subparsers.add_parser('test-config')
sp = subparsers.add_parser('container-list')

sp = subparsers.add_parser('container-create')
sp.add_argument('container', help="Container name")

sp = subparsers.add_parser('container-delete', help="Delete a container")
sp.add_argument('container', help="Container name")
sp.add_argument('--force', action="store_true", help="Delete container and all of its objects")

sp = subparsers.add_parser('container-info')
sp.add_argument('container', help="Container name")

sp = subparsers.add_parser('upload', help="Upload a file or stream")
sp.add_argument('file', help="File path")
sp.add_argument('object', help="Object name")
sp.add_argument('--container', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('download')
sp.add_argument('object', help="Object name")
sp.add_argument('file', help="File path")
sp.add_argument('--container', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('object-info')
sp.add_argument('object', help="Object name")
sp.add_argument('--container', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('object-delete')
sp.add_argument('object', help="Object name")
sp.add_argument('--container', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('ls', help="List containers and objects as if it was the file system.")
sp.add_argument('path', nargs='?')

sp = subparsers.add_parser('object-set-metadata')
sp = subparsers.add_parser('object-delete-metadata')
sp = subparsers.add_parser('object-replace-metadata')

args = parser.parse_args()

CONFIGURATION_HELP_TEXT = """
# For AWS S3 backend
    Set the following environment variables: 
        - OBS_STORAGE=s3
        - OBS_S3_LOCATION=<your-aws-location>
    Ensure your AWS credentials are available

# For Openstack Swift
    Set the following environment variables:
        - OBS_STORAGE=swift
        - OBS_SWIFT_URL=<your-openstack-swift-storage-url>
"""

def verify_configuration() -> ObjectStorageClient:
    storage_type = os.environ.get('OBS_STORAGE')
    if storage_type is None:
        print('Storage type not configured. Run the `test-config` command.')
        print(CONFIGURATION_HELP_TEXT)
        exit()
    elif storage_type.lower() == 'swift':
        if os.environ.get('OBS_SWIFT_URL') is None:
            print('OBS_SWIFT_URL envrionment variable must be defined when using OpenStack Swift storage backend (OBS_STORAGE=swift)')
            exit()
    elif storage_type.lower() == 's3':
        if os.environ.get('OBS_S3_LOCATION') is None:
            print('OBS_S3_LOCATION envrionment variable must be defined when using AWS S3 (OBS_STORAGE=s3)')
            exit()
    else:
        print('Unknow storage backend:', storage_type)
        print(CONFIGURATION_HELP_TEXT)
        exit()

    if storage_type.lower() == "swift":
        return SwiftClient(storage_url=os.environ.get('OBS_SWIFT_URL'))
    elif storage_type.lower() == "s3":
        return S3Client(location=os.environ.get('OBS_S3_LOCATION'))


if __name__ == "__main__":

    client = verify_configuration() # Returns the client (or exists the script on misconfiguration)

    if args.command == "test-config":
        storage_type = os.environ.get('OBS_STORAGE') # Should be valid at this point
        if storage_type.lower() == 'swift':
            print(f'Connecting to OpenStack Swift (url={os.environ.get("OBS_SWIFT_URL")})')
        elif storage_type.lower() == 's3':
            print('Connecting to AWS S3')
        
        client.container_list() # Assume it throws an error on failure
        print('Connection is working!')
        
    elif args.command == "container-list":
        res = client.container_list()
        print(f'Container list ({len(res)} containers)')
        for i in range(0, len(res)):
            size_str = (str(round(res[i].bytes/1024/1024)) + ' Mb').rjust(10)
            print(f" {i+1}) {res[i].name.ljust(50)} {size_str} ({res[i].count} objects)")

    elif args.command == "container-create":
        if client.container_create(args.container):
            print('Container created:', args.container)

    elif args.command == "container-delete":
        info = client.container_info(args.container)
        if info is None:
            print(f'Container `{args.container}` does not exist.')
            exit()

        if args.force:
            if info.count != 0:
                confirm = input(f'WARNING: You are about to delete a non-empty container. Are you sure you want to delete the container "{info.name}" and all its {info.count} objects ? [y/N]: ')
                if confirm.lower() not in ['y', 'yes']:
                    print('Aborting operation')
                    exit()
        elif info.count:
            print(f'Container contains {info.count} objects. Use --force to delete a non-empty container.')
            exit()

        if client.container_delete(args.container, args.force):
            print('Container deleted:', args.container)
    
    elif args.command == "container-info":
        info = client.container_info(args.container)

        if info is None:
            print(f'Container {args.container} does not exist')
            exit()

        if info.count is not None:
            count = info.count
        if info.bytes is not None:
            size = info.bytes

        if info.count is None or info.bytes is None:
            info = client.container_list(args.container)[0]
            if info.count is not None:
                count = info.count
            if info.bytes is not None:
                size = info.bytes

        print('Container:', info.name)
        if count is not None:
            print('Object count:', count)
        if size is not None:
            print('Total size (bytes):', size)
        
    elif args.command == "upload":
        object_path = args.object
        if args.container is not None:
            container = args.container
        else:
            # Get container from the object path
            container = object_path.split('/')[0]
            object_path = '/'.join(object_path.split('/')[1:])

        print(f'Uploading `{object_path}` to container `{container}`')
        with open(args.file, 'rb') as f:
            if client.object_upload(f, object_path, container_name=container):
                print(f'Upload complete: {container}/{object_path}')
            else:
                print('Upload failed')
        
    elif args.command == "download":
        object_path = args.object
        if args.container is not None:
            container = args.container
        else:
            # Get container from the object path
            container = object_path.split('/')[0]
            object_path = '/'.join(object_path.split('/')[1:])

        print(f'Downloading {container}/{object_path} to {args.file}')
        with open(args.file, 'wb') as f:
            if client.object_download(object_path, f, container_name=container):
                print('Download complete:', args.file)
            else:
                print('Download failed')

    elif args.command == "object-info":
        object_path = args.object
        if args.container is not None:
            container = args.container
        else:
            # Get container from the object path
            container = object_path.split('/')[0]
            object_path = '/'.join(object_path.split('/')[1:])

        info = client.object_info(object_path, container)

        print(f'Name: {info.name}')
        print(f'Size: {info.bytes} bytes')
        print(f'Content-Type: {info.content_type}')
        print(f'Hash: {info.hash}')
        if info.metadata is None or len(info.metadata.keys()) == 0:
            print(f'Metadata: none')
        else:
            print(f'Metadata:')
            for k in info.metadata:
                print(f' {k}: {info.metadata[k]}')

    elif args.command == "object-delete":
        object_path = args.object
        if args.container is not None:
            container = args.container
        else:
            # Get container from the object path
            container = object_path.split('/')[0]
            object_path = '/'.join(object_path.split('/')[1:])

        info = client.object_info(object_name=object_path, container_name=container)
        if info is None:
            print(f'Object `{object_path}` does not exist in container `{container}`')
            exit()

        if client.object_delete(object_name=object_path, container_name=container):
            print(f'Object deleted: {container}/{object_path}')
        else:
            print(f'Object delete failure')
    
    elif args.command == "ls":
        if args.path is None:
            res = client.container_list()

            print(len(res), 'containers')
            for i in res:
                size_str = (str(round(i.bytes/1024/1024)) + ' Mb').rjust(10)
                print(f"{i.name.ljust(50)} {size_str} ({i.count} objects)")
        else:
            container = args.path.split('/')[0]
            object_path: str = '/'.join(args.path.split('/')[1:])

            if object_path.endswith('*'): # Wildcard search
                object_path = object_path[0:-1]
            elif not object_path.endswith('/'):
                object_path += '/' # Assume folder name

            res = client.object_list(container_name=container, delimiter='/', prefix=object_path)
            prefix_to_remove = '/'.join(object_path.split('/')[0:-1])
            if len(prefix_to_remove) > 0: prefix_to_remove += '/'

            print('count', len(res))
            for i in res:
                if type(i) == SubdirInfo:
                    subdir = i.subdir[len(prefix_to_remove):]
                    print(f'- {subdir}')
                else: # ObjectInfo
                    name = i.name[len(prefix_to_remove):]
                    print(f'- {name.ljust(50)}  {str(i.bytes).rjust(10)} bytes')