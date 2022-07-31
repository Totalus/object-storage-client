
from setuptools import setup

VERSION = '0.0.3'
PACKAGE_NAME = 'universal-osc'
AUTHOR = 'Totalus'

setup(
    version=VERSION,
    name=PACKAGE_NAME,
    author=AUTHOR,
    url="https://github.com/Totalus/object-storage-client",
    package_dir={ 'universal_osc': 'src'}
)