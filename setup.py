#!/usr/bin/env python
from setuptools import setup
from codecs import open
install_requires = ['trollius']
try:
    import __pypy__
    __pypy__
except ImportError:
    install_requires.append('hiredis')

setup(
    name='trollius_redis',
    author='Jonathan Slenders and Ben Jolitz',
    version='0.13.4',
    license='LICENSE.txt',
    url='https://github.com/benjolitz/trollius-redis',

    description='PEP 3156 implementation of the redis protocol.',
    long_description=open("README.rst", 'rb').read(),
    packages=['trollius_redis'],
    install_requires=install_requires
)
