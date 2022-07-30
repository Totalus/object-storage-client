
from setuptools import setup

VERSION = '0.0.2'
PACKAGE_NAME = 'universal-osc'
AUTHOR = 'Totalus'

setup(
    version=VERSION,
    name=PACKAGE_NAME,
    author=AUTHOR,
    package_dir={PACKAGE_NAME: 'src'}
)