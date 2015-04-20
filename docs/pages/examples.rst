.. _redis-examples:

Examples
=========

The :class:`Connection <trollius_redis.Connection>` class
--------------------------------------------------------

A :class:`Connection <trollius_redis.Connection>` instance will take care of the
connection and will automatically reconnect, using a new transport when the
connection drops. This connection class also acts as a proxy to at 
:class:`RedisProtocol <trollius_redis.RedisProtocol>` instance; any Redis
command of the protocol can be called directly at the connection.

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

See :ref:`the reference <redis-reference>` to learn more about the other Redis
commands.


Connection pooling
------------------

Requests will automatically be distributed among all connections in a
:class:`Pool <trollius_redis.Pool>`. If a connection is blocking because of
--for instance-- a blocking rpop, another connection will be used for new
commands.

.. note:: This is the recommended way to connect to the Redis server.

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example():
        # Create Redis connection
        connection = yield From(trollius_redis.Pool.create(
            host=u'localhost', port=6379, poolsize=10))

        # Set a key
        yield From(connection.set(u'my_key', u'my_value'))

        # When finished, close the connection pool.
        connection.close()


Transactions
------------

A transaction can be started by calling :func:`multi
<trollius_redis.RedisProtocol.multi>`. This returns a :class:`Transaction
<trollius_redis.Transaction>` instance which is in fact just a proxy to the
:class:`RedisProtocol <trollius_redis.RedisProtocol>`, except that every Redis
method of the protocol now became a coroutine that returns a future. The
results of these futures can be retrieved after the transaction is commited
with :func:`exec <trollius_redis.Transaction.exec>`.

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    @trollius.coroutine
    def example(loop):
        # Create Redis connection
        connection = yield From(trollius_redis.Pool.create(
            host=u'localhost', port=6379, poolsize=10))

        # Create transaction
        transaction = yield From(connection.multi())

        # Run commands in transaction (they return future objects)
        f1 = yield From(transaction.set(u'key', u'value'))
        f2 = yield From(transaction.set(u'another_key', u'another_value'))

        # Commit transaction
        yield From(transaction.exec())

        # Retrieve results
        result1 = yield From(f1)
        result2 = yield From(f2)

        # When finished, close the connection pool.
        connection.close()


It's recommended to use a large enough poolsize. A connection will be occupied
as long as there's a transaction running in there.


Pubsub
------

By calling :func:`start_subscribe
<trollius_redis.RedisProtocol.start_subscribe>` (either on the protocol, through
the :class:`Connection <trollius_redis.Connection>` class or through the :class:`Pool
<trollius_redis.Pool>` class), you can start a pubsub listener.

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
        yield From(subscriber.subscribe([ u'our-channel' ]))

        # Inside a while loop, wait for incoming events.
        while True:
            reply = yield From(subscriber.next_published())
            print(u'Received: ', repr(reply.value), u'on channel', reply.channel)

        # When finished, close the connection.
        connection.close()


LUA Scripting
-------------

The :func:`register_script <trollius_redis.RedisProtocol.register_script>`
function -- which can be used to register a LUA script -- returns a
:class:`Script <trollius_redis.Script>` instance. You can call its :func:`run
<trollius_redis.Script.run>` method to execute this script.


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
        connection = yield From(trollius_redis.Connection.create(
            host=u'localhost', port=6379))

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


Raw bytes or UTF-8
------------------

The redis protocol only knows about bytes, but normally you want to use strings
in your Python code. ``trollius_redis`` is helpful and installs an encoder that
does this conversion automatically, using the UTF-8 codec. However, sometimes
you want to access raw bytes. This is possible by passing a
:class:`BytesEncoder <trollius_redis.encoders.BytesEncoder>` instance to the
connection, pool or protocol.

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    from trollius_redis.encoders import BytesEncoder

    @trollius.coroutine
    def example():
        # Create Redis connection
        connection = yield From(trollius_redis.Connection.create(
            host=u'localhost', port=6379, encoder=BytesEncoder()))

        # Set a key
        yield From(connection.set(b'my_key', b'my_value'))

        # When finished, close the connection.
        connection.close()


Scanning for keys
-----------------

Redis has a few nice scanning utilities to discover keys in the database. They
are rather low-level, but ``trollius_redis`` exposes a simple
:class:`~trollius_redis.cursors.Cursor` class that allows you to iterate over
all the keys matching a certain pattern. Each call of the
:func:`~trollius_redis.cursors.Cursor.fetchone` coroutine will return the next
match. You don't have have to worry about accessing the server every x pages.

The following example will print all the keys in the database:

.. code:: python

    import trollius
    from trollius import From
    import trollius_redis

    from trollius_redis.encoders import BytesEncoder

    @trollius.coroutine
    def example():
        cursor = yield From(protocol.scan(match=u'*'))
        while True:
            item = yield From(cursor.fetchone())
            if item is None:
                break
            else:
                print(item)


See the scanning utilities: :func:`~trollius_redis.RedisProtocol.scan`,
:func:`~trollius_redis.RedisProtocol.sscan`,
:func:`~trollius_redis.RedisProtocol.hscan` and
:func:`~trollius_redis.RedisProtocol.zscan`


The :class:`RedisProtocol <trollius_redis.RedisProtocol>` class
--------------------------------------------------------------

The most low level way of accessing the redis server through this library is
probably by creating a connection with the `RedisProtocol` yourself. You can do
it as follows:

.. code:: python

    import asyncio
    import trollius_redis

    @asyncio.coroutine
    def example():
        loop = asyncio.get_event_loop()

        # Create Redis connection
        transport, protocol = yield From(loop.create_connection(
                    trollius_redis.RedisProtocol, u'localhost', 6379))

        # Set a key
        yield From(protocol.set(u'my_key', u'my_value'))

        # Get a key
        result = yield From(protocol.get(u'my_key'))
        print(result)

    if __name__ == '__main__':
        asyncio.get_event_loop().run_until_complete(example())


.. note:: It is not recommended to use the Protocol class directly, because the
          low-level Redis implementation could change. Prefer the
          :class:`Connection <trollius_redis.Connection>` or :class:`Pool
          <trollius_redis.Pool>` class as demonstrated above if possible.
