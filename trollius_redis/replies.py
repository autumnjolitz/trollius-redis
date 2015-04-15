import trollius as asyncio
from trollius import From, Return
__all__ = (
    'BlockingPopReply',
    'DictReply',
    'ListReply',
    'PubSubReply',
    'SetReply',
    'StatusReply',
    'ZRangeReply',
    'ConfigPairReply',
    'InfoReply',
)


class StatusReply(object):
    """
    Wrapper for Redis status replies.
    (for messages like OK, QUEUED, etc...)
    """
    def __init__(self, status):
        self.status = status

    def __repr__(self):
        return 'StatusReply(status=%r)' % self.status

    def __eq__(self, other):
        try:
            return self.status == other.status
        except:
            raise ValueError("Cannot compare StatusReply with type {0}".format(type(other)))


class DictReply(object):
    """
    Container for a dict reply.

    The content can be retrieved by calling
    :func:`~trollius_redis.replies.DictReply.asdict` which returns a Python
    dictionary. Or by iterating over it:

    ::

        for f in dict_reply:
            key, value = yield from f
            print(key, value)
    """
    def __init__(self, multibulk_reply):
        self._result = multibulk_reply

    def _parse(self, key, value):
        return key, value

    def __iter__(self):
        """ Yield a list of futures that yield { key: value } tuples. """

        @asyncio.coroutine
        def getter(f):
            """ Coroutine which processes one item. """
            key, value = yield From(f)
            key, value = self._parse(key, value)
            raise Return(key, value)

        for _ in range(self._result.count // 2):
            read_future = self._result._read(count=2)
            yield asyncio.async(getter(read_future), loop=self._result._loop)

    @asyncio.coroutine
    def asdict(self):
        """
        Return the result as a Python dictionary.
        """
        data = yield From(self._result._read(count=self._result.count))
        raise Return(dict(
            self._parse(k, v) for k, v in zip(data[::2], data[1::2])))

    def __repr__(self):
        return '%s(length=%r)' % (
            self.__class__.__name__, int(self._result.count / 2))


class ZRangeReply(DictReply):
    """
    Container for a zrange query result.
    """
    def _parse(self, key, value):
        # Mapping { key: score_as_float }
        return key, float(value)


class SetReply(object):
    """
    Redis set result.
    The content can be retrieved by calling
    :func:`~trollius_redis.replies.SetReply.asset` or by iterating over it

    ::

        for f in set_reply:
            item = yield from f
            print(item)
    """
    def __init__(self, multibulk_reply):
        self._result = multibulk_reply

    def __iter__(self):
        """ Yield a list of futures. """
        return iter(self._result)

    @asyncio.coroutine
    def asset(self):
        """ Return the result as a Python ``set``.  """
        data = yield From(self._result._read(count=self._result.count))
        raise Return(set(data))

    def __repr__(self):
        return 'SetReply(length=%r)' % (self._result.count)


class ListReply(object):
    """
    Redis list result.
    The content can be retrieved by calling
    :func:`~trollius_redis.replies.ListReply.aslist` or by iterating over it
    or by iterating over it

    ::

        for f in list_reply:
            item = yield from f
            print(item)
    """
    def __init__(self, multibulk_reply):
        self._result = multibulk_reply

    def __iter__(self):
        """ Yield a list of futures. """
        return iter(self._result)

    @asyncio.coroutine
    def aslist(self):
        """ Return the result as a Python ``list``. """
        data = yield From(self._result._read(count=self._result.count))
        raise Return(data)

    def __repr__(self):
        return 'ListReply(length=%r)' % (self._result.count, )


class BlockingPopReply(object):
    """
    :func:`~trollius_redis.RedisProtocol.blpop` or
    :func:`~trollius_redis.RedisProtocol.brpop` reply
    """
    def __init__(self, list_name, value):
        self._list_name = list_name
        self._value = value

    @property
    def list_name(self):
        """ List name. """
        return self._list_name

    @property
    def value(self):
        """ Popped value """
        return self._value

    def __repr__(self):
        return 'BlockingPopReply(list_name=%r, value=%r)' % (
            self.list_name, self.value)


class ConfigPairReply(object):
    """ :func:`~trollius_redis.RedisProtocol.config_get` reply. """
    def __init__(self, parameter, value):
        self._paramater = parameter
        self._value = value

    @property
    def parameter(self):
        """ Config parameter name. """
        return self._paramater

    @property
    def value(self):
        """ Config parameter value. """
        return self._value

    def __repr__(self):
        return 'ConfigPairReply(parameter=%r, value=%r)' % (
            self.parameter, self.value)


class InfoReply(object):
    """ :func:`~trollius_redis.RedisProtocol.info` reply. """
    def __init__(self, data):
        self._data = data  # TODO: implement parser logic


class ClientListReply(object):
    """ :func:`~trollius_redis.RedisProtocol.client_list` reply. """
    def __init__(self, data):
        self._data = data  # TODO: implement parser logic


class PubSubReply(object):
    """ Received pubsub message. """
    def __init__(self, channel, value, pattern=None):
        self._channel = channel
        self._value = value
        self._pattern = pattern

    @property
    def channel(self):
        """ Channel name """
        return self._channel

    @property
    def value(self):
        """ Received PubSub value """
        return self._value

    @property
    def pattern(self):
        """ The pattern to which we subscribed or `None` otherwise """
        return self._pattern

    def __repr__(self):
        return 'PubSubReply(channel=%r, value=%r)' % (self.channel, self.value)

    def __eq__(self, other):
        return (self._channel == other._channel and
                self._value == other._value and
                self._pattern == other._pattern)


class EvalScriptReply(object):
    """
    :func:`~trollius_redis.RedisProtocol.evalsha` reply.

    Lua scripts can return strings/bytes (NativeType), but also ints, lists or
    even nested data structures.
    """
    def __init__(self, protocol, value):
        self._protocol = protocol
        self._value = value

    @asyncio.coroutine
    def return_value(self):
        """
        Coroutine that returns a Python representation of the script's return
        value.
        """
        from trollius_redis.protocol import MultiBulkReply

        @asyncio.coroutine
        def decode(obj):
            if isinstance(obj, int):
                raise Return(obj)

            elif isinstance(obj, bytes):
                raise Return(self._protocol.decode_to_native(self._value))

            elif isinstance(obj, MultiBulkReply):
                # Unpack MultiBulkReply recursively as Python list.
                result = []
                for f in obj:
                    item = yield From(f)
                    result.append((yield From(decode(item))))
                raise Return(result)

            else:
                # Nonetype, or decoded bytes.
                raise Return(obj)
        r = yield From(decode(self._value))
        raise Return(r)
