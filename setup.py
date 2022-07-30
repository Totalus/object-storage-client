
from setuptools import setup

VERSION = '0.0.1'
PACKAGE_NAME = 'universal-osc'
AUTHOR = 'Totalus'

setup(
    version=VERSION,
    name=PACKAGE_NAME,
    author=AUTHOR,
    package_dir={'gosc': 'src'}
)