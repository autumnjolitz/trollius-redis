#!/usr/bin/env python
from setuptools import setup
from codecs import open

install_requires = ['trollius', 'six']

try:
    from unittest import mock
    mock
except ImportError:
    install_requires.append('mock')

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
    author_email="ben.jolitz+trollius_redis@gmail.com",
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Database',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: BSD License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    version='0.1.1',
    license='LICENSE.txt',
    url='https://github.com/benjolitz/trollius-redis',

    description='PEP 3156 implementation of the redis protocol.',
    long_description=description,
    packages=['trollius_redis'],
    install_requires=install_requires
)
