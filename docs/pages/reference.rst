.. _redis-reference:

Reference
=========

You can either use the :class:`RedisProtocol <trollius_redis.RedisProtocol>`
class directly, use the :class:`Connection <trollius_redis.Connection>` class,
or use the :class:`Pool <trollius_redis.Pool>` wrapper which also offers
connection pooling.

The Protocol
------------

.. autoclass:: trollius_redis.RedisProtocol
    :members:
    :undoc-members:
    :exclude-members: data_received, eof_received, connection_lost

.. autoclass:: trollius_redis.HiRedisProtocol
    :members:


Encoders
----------

.. autoclass:: trollius_redis.encoders.BaseEncoder
    :members:
    :undoc-members:

.. autoclass:: trollius_redis.encoders.UTF8Encoder
    :members:

.. autoclass:: trollius_redis.encoders.BytesEncoder
    :members:


Connection
----------

.. autoclass:: trollius_redis.Connection
    :members:
    :exclude-members: register_script

Connection pool
---------------

.. autoclass:: trollius_redis.Pool
    :members:

Command replies
---------------

.. autoclass:: trollius_redis.replies.StatusReply
    :members:

.. autoclass:: trollius_redis.replies.DictReply
    :members:

.. autoclass:: trollius_redis.replies.ListReply
    :members:

.. autoclass:: trollius_redis.replies.SetReply
    :members:

.. autoclass:: trollius_redis.replies.ZRangeReply
    :members:

.. autoclass:: trollius_redis.replies.PubSubReply
    :members:

.. autoclass:: trollius_redis.replies.BlockingPopReply
    :members:

.. autoclass:: trollius_redis.replies.InfoReply
    :members:

.. autoclass:: trollius_redis.replies.ClientListReply
    :members:


Cursors
-------

.. autoclass:: trollius_redis.cursors.Cursor
    :members:

.. autoclass:: trollius_redis.cursors.SetCursor
    :members:

.. autoclass:: trollius_redis.cursors.DictCursor
    :members:

.. autoclass:: trollius_redis.cursors.ZCursor
    :members:


Utils
-----

.. autoclass:: trollius_redis.ZScoreBoundary
    :members:

.. autoclass:: trollius_redis.Transaction
    :members:

.. autoclass:: trollius_redis.Subscription
    :members:

.. autoclass:: trollius_redis.Script
    :members:

.. autoclass:: trollius_redis.ZAggregate
    :members:


Exceptions
----------

.. autoclass:: trollius_redis.exceptions.TransactionError
    :members:

.. autoclass:: trollius_redis.exceptions.NotConnectedError
    :members:

.. autoclass:: trollius_redis.exceptions.TimeoutError
    :members:

.. autoclass:: trollius_redis.exceptions.ConnectionLostError
    :members:

.. autoclass:: trollius_redis.exceptions.NoAvailableConnectionsInPoolError
    :members:

.. autoclass:: trollius_redis.exceptions.ScriptKilledError
    :members:

.. autoclass:: trollius_redis.exceptions.NoRunningScriptError
    :members:
