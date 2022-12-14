
import argparse, os, sys

from .SwiftClient import *
from .S3Client import *


CLI_VERSION = 0.1

parser = argparse.ArgumentParser(
    prog="obs_client",
    description="Object Storage Client CLI tool that simplify managing object storage",
    epilog="This is an open source project: https://github.com/Totalus/object-storage-client"
)

subparsers = parser.add_subparsers(dest="command", required=True, metavar='<command>', title="Commands", help="Operation to execute")

sp = subparsers.add_parser('version', help='Print CLI versino')

sp = subparsers.add_parser('test-config', help="Test configuration and connectivity to the storage backend")
sp = subparsers.add_parser('container-list', help="List containers (see also the `ls` command)")

sp = subparsers.add_parser('container-create', help="Create a container")
sp.add_argument('container', metavar='<container>' , help="Container name")

sp = subparsers.add_parser('container-delete', help="Delete a container")
sp.add_argument('container', metavar='<container>' , help="Container name")
sp.add_argument('--force', action="store_true", help="Delete container and all of its objects")

sp = subparsers.add_parser('container-info', help="Print container details")
sp.add_argument('container', metavar='<container>' , help="Container name")

sp = subparsers.add_parser('upload', help="Upload a file (or from stdin if --file unspecified)")
sp.add_argument('--file', '-f', metavar='<file path>', help="Local file to upload")
sp.add_argument('object', metavar='<object path>', help="Target object path. If --container is not specified, the first part of the <object path> is assumed to be the container name (i.e. `<object path> = <container name>/<object name>`)")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")
sp.add_argument('--meta', '-m', metavar='<key>=<value>', help="Metadata key-value pairs", action="append", default=[])

sp = subparsers.add_parser('download', help="Download a file")
sp.add_argument('object', metavar='<object path>', help="Object to download (`<container name>/<object name>`, unless --container is specified)")
sp.add_argument('--file', metavar='<file path>', help="Target file")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('object-info', help="Get object info")
sp.add_argument('object', metavar='<object name>', help="Object name")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('object-delete', help="Delete an object")
sp.add_argument('object', metavar='<object name>', help="Object name")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('ls', help="List containers and objects as if it was the file system.")
sp.add_argument('path', nargs='?')

# sp = subparsers.add_parser('object-set-metadata')
# sp = subparsers.add_parser('object-delete-metadata')
# sp = subparsers.add_parser('object-replace-metadata')

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

    if args.command == 'version':
        print(f'Universal Object Storage CLI v{CLI_VERSION}')
        exit()

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

        meta = {}
        for m in args.meta:
            if len(m.split('=')) == 2:
                meta[m.split('=')[0]] = m.split('=')[1]
            else:
                print(f'Metadata synthax error: `{m}`')
                exit()

        if args.file is not None:
            print(f'Uploading: {container}/{object_path}')
            with open(args.file, 'rb') as f:
                if client.object_upload(f, object_path, container_name=container, metadata=meta):
                    print(f'Upload complete: {container}/{object_path}')
                else:
                    print('Upload failed')
        else:
            print(f'Uploading {container}/{object_path} from stdin')
            if client.object_upload(sys.stdin.buffer, object_path, container_name=container, metadata=meta):
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

        if args.file:
            print(f'Downloading {container}/{object_path} to {args.file}')
            with open(args.file, 'wb') as f:
                if client.object_download(object_path, f, container_name=container):
                    print('Download complete:', args.file)
                else:
                    print('Download failed')
        else:
            if not client.object_download(object_path, sys.stdout.buffer, container_name=container):
                print('Download failed', file=sys.stderr)

    elif args.command == "object-info":
        object_path = args.object
        if args.container is not None:
            container = args.container
        else:
            # Get container from the object path
            container = object_path.split('/')[0]
            object_path = '/'.join(object_path.split('/')[1:])

        info = client.object_info(object_path, container)

        print(f'   Container: {container}')
        print(f'        Name: {info.name}')
        print(f'        Size: {info.bytes} bytes')
        print(f'Content-Type: {info.content_type}')
        print(f'        Hash: {info.hash}')
        if info.metadata is None or len(info.metadata.keys()) == 0:
            print(f'    Metadata: (none)')
        else:
            print(f'    Metadata:')
            for k in info.metadata:
                print(f'       - {k} = {info.metadata[k]}')

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

            print(f'--- {len(res)} containers ---')
            for i in res:
                size_str = (str(round(i.bytes/1024/1024)) + ' Mb').rjust(10)
                print(f"{i.name.ljust(50)} {size_str} ({i.count} objects)")
        else:
            container = args.path.split('/')[0]
            object_path: str = '/'.join(args.path.split('/')[1:])

            if object_path.endswith('*'): # Wildcard search
                object_path = object_path[0:-1]
            elif not object_path.endswith('/') and len(object_path) > 0 and client.object_info(container_name=container, object_name=object_path) == None:
                object_path += '/' # Assume folder name

            res = client.object_list(container_name=container, delimiter='/', prefix=object_path)
            prefix_to_remove = '/'.join(object_path.split('/')[0:-1])
            if len(prefix_to_remove) > 0: prefix_to_remove += '/'

            for i in res:
                if type(i) == SubdirInfo:
                    subdir = i.subdir[len(prefix_to_remove):]
                    print(f'{subdir}')
                else: # ObjectInfo
                    name = i.name[len(prefix_to_remove):]
                    print(f'{name.ljust(50)}  {str(i.bytes).rjust(10)} bytes')