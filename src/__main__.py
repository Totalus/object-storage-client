#
#   CLI code
#

import argparse, os, sys

from .SwiftClient import *
from .S3Client import *


CLI_VERSION = "0.6"
LIB_VERSION = "2.0.7 " # Sync with version in setup.cfg

parser = argparse.ArgumentParser(
    prog="obs_client",
    description="Object Storage Client CLI tool that simplify managing object storage",
    epilog="This is an open source project: https://github.com/Totalus/object-storage-client"
)

subparsers = parser.add_subparsers(dest="command", required=True, metavar='<command>', title="Commands", help="Operation to execute")

sp = subparsers.add_parser('version', help='Print version')

sp = subparsers.add_parser('test-config', help="Test configuration and connectivity to the storage backend")
sp = subparsers.add_parser('container-list', help="List containers (see also the `ls` command)")

sp = subparsers.add_parser('container-create', help="Create a container")
sp.add_argument('container', metavar='<container>' , help="Container name")

sp = subparsers.add_parser('container-delete', help="Delete a container")
sp.add_argument('container', metavar='<container>' , help="Container name")
sp.add_argument('--force', action="store_true", help="Delete container and all of its objects")

sp = subparsers.add_parser('upload', help="Upload a file (or from stdin if --file unspecified)")
sp.add_argument('--file', '-f', metavar='<file path>', help="Local file to upload")
sp.add_argument('object', metavar='<object path>', help="Target object path. If --container is not specified, the first part of the <object path> is assumed to be the container name (i.e. `<object path> = <container name>/<object name>`)")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")
sp.add_argument('--meta', '-m', metavar='<key>=<value>', help="Metadata key-value pairs", action="append", default=[])

sp = subparsers.add_parser('download', help="Download a file")
sp.add_argument('object', metavar='<object path>', help="Object to download (`<container name>/<object name>`, unless --container is specified)")
sp.add_argument('--file', metavar='<file path>', help="Target file")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('object-download-url', help="Generate a signed temporary download link for an object")
sp.add_argument('object', metavar='<object path>', help="Object to download (`<container name>/<object name>`, unless --container is specified)")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")
sp.add_argument('--expires-in', '-e', metavar='<seconds>', help="Link will become invalid after this number of seconds")

sp = subparsers.add_parser('object-delete', help="Delete an object")
sp.add_argument('object', metavar='<object name>', help="Object name")
sp.add_argument('--container', metavar='<container name>', help="Container name. Optionally you can specify the container name in the object path instead (ex: <container>/<object_name>)")

sp = subparsers.add_parser('ls', help="List containers and objects as if it was the file system.")
sp.add_argument('path', nargs='?')

sp = subparsers.add_parser('info', help="Get object or container info")
sp.add_argument('path', metavar='<container>/<object>', help="Container or object path")

sp = subparsers.add_parser('list', help="List objects that match the given prefix")
sp.add_argument('path', metavar='<container>/<prefix>', help="Path prefix", nargs='?')

# sp = subparsers.add_parser('object-set-metadata')
# sp = subparsers.add_parser('object-delete-metadata')
# sp = subparsers.add_parser('object-replace-metadata')

args = parser.parse_args()

CONFIGURATION_HELP_TEXT = """
# For AWS S3
    - Set the following environment variable: export OBS_S3_LOCATION=<your-aws-location>
    - Ensure your AWS credentials are available
    - Optionally set OBS_S3_ENDPOINT_URL=<custom-s3-endpoint-url> for non-AWS hosted S3 compatible storages

# For Openstack Swift
    - Set the following environment variables: export OBS_SWIFT_REGION=<your-openstack-swift-storage-region>
    - Ensure your OpenStack credentials are available in the environment
"""

def verify_configuration() -> ObjectStorageClient:

    swift_region = os.environ.get('OBS_SWIFT_REGION')
    s3_location = os.environ.get('OBS_S3_LOCATION')

    if swift_region is not None and s3_location is not None:
        print('OBS_SWIFT_REGION and OBS_S3_LOCATION cannot be both defined in the environment')
        print(CONFIGURATION_HELP_TEXT)
        exit()
    
    if swift_region is None and s3_location is None:
        print('Storage backend not configured:')
        print(CONFIGURATION_HELP_TEXT)
        exit()

    if swift_region is not None:
        return SwiftClient(region=os.environ.get('OBS_SWIFT_REGION'))
    elif s3_location is not None:
        return S3Client(
            location=os.environ.get('OBS_S3_LOCATION'),
            endpoint_url=os.environ.get('OBS_S3_ENDPOINT_URL')
        )

if __name__ == "__main__":

    if args.command == 'version':
        print(f'Universal Object Storage CLI: {CLI_VERSION}')
        print(f'Universal Object Storage LIB: {LIB_VERSION}')
        exit()

    client = verify_configuration() # Returns the client (or exits the script on misconfiguration)

    if args.command == "test-config":
        if isinstance(client, SwiftClient):
            print(f'Connecting to OpenStack Swift (region={client.region})')
        elif isinstance(client, S3Client):
            print(f'Connecting to AWS S3 (location={client.location}{f", endpoint={client.endpoint_url}" if client.endpoint_url else ""})')

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

    elif args.command == "object-download-url":
        object_path = args.object
        if args.container is not None:
            container = args.container
        else:
            # Get container from the object path
            container = object_path.split('/')[0]
            object_path = '/'.join(object_path.split('/')[1:])

        url = client.object_generate_download_url(object_path, container, expires_in_seconds=args.expires_in)
        print(url)
        

    elif args.command == "info":
        path : str = args.path
        
        container = path.split('/')[0]
        
        if path.endswith('/') or '/' not in path:
            # Can be a container
            info = client.container_info(container)

            if info is not None:
                count = info.count
                size = info.bytes

                if count is None or bytes is None:
                    info = client.container_list(container)[0]
                    count = count if count else info.count
                    size = size if size else info.bytes

                print(f'----- Container info -----')
                print('Container Name     :', info.name)
                if count is not None:
                    print('Object count       :', count)
                if size is not None:
                    print('Total size (bytes) :', size)
            exit()

        if '/' in path:
            # Could be an object
            object = '/'.join(path.split('/')[1:])
            info = client.object_info(object_name=object, container_name=container)

            if info is not None:
                print(f'----- Object info -----')
                print(f'Object Name  : {info.name}')
                print(f'Container    : {container}')
                print(f'Size         : {info.bytes} bytes')
                print(f'Content-Type : {info.content_type}')
                print(f'Hash         : {info.hash}')
                if info.metadata is None or len(info.metadata.keys()) == 0:
                    print(f'Metadata     : (none)')
                else:
                    print(f'Metadata     :')
                    for k in info.metadata:
                        print(f' - {k} = "{info.metadata[k]}"')
                exit()

        print(f'Specified object or container not found: {path}')
        exit()

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
            if len(res) > 0:
                maxLen = max([ len(i.name) for i in res])
            for i in res:
                size_str = "" if not i.bytes else f" {str(round(i.bytes/1024/1024))} Mb".rjust(10)
                object_count_str = "" if not i.count else f" ({i.count} objects)"
                print(f"{i.name.ljust(maxLen + 10)}{size_str}{object_count_str}")
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
            if len(res) > 0:
                maxLen = max([ len(i.subdir if type(i) == SubdirInfo else i.name) for i in res])
            for i in res:
                if type(i) == SubdirInfo:
                    subdir = i.subdir[len(prefix_to_remove):]
                    print(f'{subdir}')
                else: # ObjectInfo
                    name = i.name[len(prefix_to_remove):]
                    print(f'{name.ljust(maxLen)}  {str(i.bytes).rjust(10)} bytes')
    
    elif args.command == "list":
        if args.path is None:
            res = client.container_list()

            print(f'--- {len(res)} containers ---')
            if len(res) > 0:
                maxLen = max([ len(i.name) for i in res])
            for i in res:
                size_str = "" if not i.bytes else f" {str(round(i.bytes/1024/1024))} Mb".rjust(10)
                object_count_str = "" if not i.count else f" ({i.count} objects)"
                print(f"{i.name.ljust(maxLen + 10)}{size_str}{object_count_str}")
        else:
            container = args.path.split('/')[0]
            object_path: str = '/'.join(args.path.split('/')[1:])

            res = client.object_list(container_name=container, prefix=object_path)

            print(f'--- {len(res)} objects ---')

            if len(res) > 0:
                maxLen = max([ len(i.subdir if type(i) == SubdirInfo else i.name) for i in res])
            for i in res:
                if type(i) == SubdirInfo:
                    print(f'{i.subdir}')
                else: # ObjectInfo
                    print(f'{i.name.ljust(maxLen)}  {str(i.bytes).rjust(10)} bytes')