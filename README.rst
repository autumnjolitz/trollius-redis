Redis client for Python trollius
===========================================================================
(ported from `asyncio-redis`_)

|Build Status| |Wheel Status|


Supports
---------
- CPython 2.7, 3.3-3.5
- PyPy
- PyPy3


Description
------------


Redis client for the `PEP 3156`_ Python event loop ported to Trollius.

.. _PEP 3156: http://legacy.python.org/dev/peps/pep-3156/

This Redis library is a completely asynchronous, non-blocking client for a
Redis server. It depends on trollius (asyncio compatible for PEP 3156). It
supports Python 2 and 3 Trollius-using developers.

If you're new to asyncio, it can be helpful to check out
`the asyncio documentation`_ first.

.. _the asyncio documentation: http://docs.python.org/dev/library/asyncio.html

To see the original awesome driver that I ported from, I advise you to take a look at Jonathan Slenders `asyncio-redis`_.

.. _asyncio-redis: https://github.com/jonathanslenders/asyncio-redis.git


Features
--------

- Works for the trollius asyncio-compatible (PEP3156) event loop
- No dependencies except trollius
- Connection pooling
- Automatic conversion from unicode (Python) to bytes (inside Redis.)
- Bytes and str protocols.
- Completely tested
- Blocking calls and transactions supported
- Streaming of some multi bulk replies
- Pubsub support


Installation
------------

.. code::

    pip install trollius-redis

Documentation
-------------

View documentation at `read-the-docs`_

.. _read-the-docs: http://trollius-redis.readthedocs.org/en/latest/


The connection class
--------------------

A ``trollius_redis.Connection`` instance will take care of the connection and
will automatically reconnect, using a new transport when the connection drops.
This connection class also acts as a proxy to a ``trollius_redis.RedisProtocol``
instance; any Redis command of the protocol can be called directly at the
connection.


.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example():
        # Create Redis connection
        connection = yield From(trollius_redis.Connection.create(host=u'localhost', port=6379))

        # Set a key
        yield From(connection.set(u'my_key', u'my_value'))

        # When finished, close the connection.
        connection.close()

    if __name__ == '__main__':
        loop = trollius.get_event_loop()
        loop.run_until_complete(example())


Connection pooling
------------------

Requests will automatically be distributed among all connections in a pool. If
a connection is blocking because of --for instance-- a blocking rpop, another
connection will be used for new commands.


.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example():
        # Create Redis connection
        connection = yield From(trollius_redis.Pool.create(host=u'localhost', port=6379, poolsize=10))

        # Set a key
        yield From(connection.set(u'my_key', u'my_value'))

        # When finished, close the connection pool.
        connection.close()


Transactions example
--------------------

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example():
        # Create Redis connection
        connection = yield From(trollius_redis.Pool.create(host=u'localhost', port=6379, poolsize=10))

        # Create transaction
        transaction = yield From(connection.multi())

        # Run commands in transaction (they return future objects)
        f1 = yield From(transaction.set(u'key', u'value'))
        f2 = yield From(transaction.set(u'another_key', u'another_value'))

        # Commit transaction
        yield From(transaction.execute())

        # Retrieve results
        result1 = yield From(f1)
        result2 = yield From(f2)

        # When finished, close the connection pool.
        connection.close()

It's recommended to use a large enough poolsize. A connection will be occupied
as long as there's a transaction running in there.


Pubsub example
--------------

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example():
        # Create connection
        connection = yield From(trollius_redis.Connection.create(host=u'localhost', port=6379))

        # Create subscriber.
        subscriber = yield From(connection.start_subscribe())

        # Subscribe to channel.
        yield From(subscriber.subscribe([u'our-channel']))

        # Inside a while loop, wait for incoming events.
        while True:
            reply = yield From(subscriber.next_published())
            print(u'Received: ', repr(reply.value), u'on channel', reply.channel)

        # When finished, close the connection.
        connection.close()


LUA Scripting example
---------------------

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    code = \
    u"""
    local value = redis.call('GET', KEYS[1])
    value = tonumber(value)
    return value * ARGV[1]
    """

    @trollius.coroutine
    def example():
        connection = yield From(trollius_redis.Connection.create(host=u'localhost', port=6379))

        # Set a key
        yield From(connection.set(u'my_key', u'2'))

        # Register script
        multiply = yield From(connection.register_script(code))

        # Run script
        script_reply = yield From(multiply.run(keys=[u'my_key'], args=[u'5']))
        result = yield From(script_reply.return_value())
        print(result) # prints 2 * 5

        # When finished, close the connection.
        connection.close()


Example using the Protocol class
--------------------------------

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example():
        loop = trollius.get_event_loop()

        # Create Redis connection
        transport, protocol = yield From(loop.create_connection(
                    trollius_redis.RedisProtocol, u'localhost', 6379))

        # Set a key
        yield From(protocol.set(u'my_key', u'my_value'))

        # Get a key
        result = yield From(protocol.get(u'my_key'))
        print(result)

        # Close transport when finished.
        transport.close()

    if __name__ == '__main__':
        trollius.get_event_loop().run_until_complete(example())


.. |Build Status| image:: https://travis-ci.org/benjolitz/trollius-redis.svg?branch=master
    :target: https://travis-ci.org/benjolitz/trollius-redis
    :alt: Build Status from Travis-CI


.. |Wheel Status| image:: https://pypip.in/wheel/trollius_redis/badge.svg
    :target: https://pypi.python.org/pypi/trollius_redis/
    :alt: Wheel Status

