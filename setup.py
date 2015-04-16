#!/usr/bin/env python
from setuptools import setup
from codecs import open
install_requires = ['trollius', 'six', 'mock']
try:
    import __pypy__
    __pypy__
except ImportError:
    install_requires.append('hiredis')

with open("README.rst", 'r') as fh:
    description = fh.read()

setup(
    name='trollius_redis',
    author='Jonathan Slenders, Ben Jolitz',
    version='0.0.1',
    license='LICENSE.txt',
    url='https://github.com/benjolitz/trollius-redis',

    description='PEP 3156 implementation of the redis protocol.',
    long_description=description,
    packages=['trollius_redis'],
    install_requires=install_requires
)
