#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import sys

if sys.version_info < (3,0):
    install_requires = ['trollius']
else:
    raise ValueError("Py2 only")

setup(
        name='asyncio_redis',
        author='Jonathan Slenders',
        version='0.13.4',
        license='LICENSE.txt',
        url='https://github.com/benjolitz/asyncio-redis-python2',

        description='PEP 3156 implementation of the redis protocol.',
        long_description=open("README.rst").read(),
        packages=['asyncio_redis'],
        install_requires=install_requires,
        extra_require = {
            'hiredis': ['hiredis'],
        }
)
