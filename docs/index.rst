.. trollius_redis documentation master file, created by
   sphinx-quickstart on Thu Oct 31 08:50:13 2013.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

trollius_redis
=============

Asynchronous Redis client for Python trollius.

Port of .._asyncio-redis: https://github.com/jonathanslenders/asyncio-redis

.. _PEP 3156: http://legacy.python.org/dev/peps/pep-3156/
.. _GitHub: https://github.com/benjolitz/trollius-redis

This Redis library is a completely asynchronous, non-blocking client for a
Redis server.

It depends on the asyncio way of doing things (PEP 3156) but uses trollius so you can use Python 2.

If you're new to asyncio, it can be helpful to check out
`the asyncio documentation`_ first.

.. _the asyncio documentation: http://docs.python.org/dev/library/asyncio.html

Features
--------

- Works for the trollius (PEP3156) event loop
- No dependencies except trollius
- Connection pooling and pipelining
- Automatic conversion from native Python types (unicode or bytes) to Redis types (bytes).
- Blocking calls and transactions supported
- Pubsub support
- Streaming of multi bulk replies
- Completely tested

Installation
------------

::

    pip install trollius-redis


Start by taking a look at :ref:`some examples<redis-examples>`.


Author and License
------------------

The ``trollius_redis`` package is a port done by Ben Jolitz
of the ``asyncio_redis`` package originally written by Jonathan Slenders.  It's BSD
licensed and freely available. Feel free to improve this package and
`send a pull request`_.

.. _send a pull request: https://github.com/benjolitz/trollius-redis


Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


.. toctree::
   :maxdepth: 2

   pages/examples
   pages/reference
