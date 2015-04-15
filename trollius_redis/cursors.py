import trollius as asyncio
from trollius import From, Return
from collections import deque

__all__ = (
    'Cursor',
    'DictCursor',
    'SetCursor',
    'ZCursor',
)


class Cursor(object):
    """
    Cursor for walking through the results of a :func:`scan
    <trollius_redis.RedisProtocol.scan>` query.
    """
    def __init__(self, name, scanfunc):
        self._queue = deque()
        self._cursor = 0
        self._name = name
        self._scanfunc = scanfunc
        self._done = False

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._name)

    @asyncio.coroutine
    def _fetch_more(self):
        """ Get next chunk of keys from Redis """
        if not self._done:
            chunk = yield From(self._scanfunc(self._cursor))
            self._cursor = chunk.new_cursor_pos

            if chunk.new_cursor_pos == 0:
                self._done = True

            for i in chunk.items:
                self._queue.append(i)

    @asyncio.coroutine
    def fetchone(self):
        """
        Coroutines that returns the next item.
        It returns `None` after the last item.
        """
        if not self._queue and not self._done:
            yield From(self._fetch_more())

        if self._queue:
            raise Return(self._queue.popleft())

    @asyncio.coroutine
    def fetchall(self):
        """ Coroutine that reads all the items in one list. """
        results = []

        while True:
            i = yield From(self.fetchone())
            if i is None:
                break
            else:
                results.append(i)

        raise Return(results)


class SetCursor(Cursor):
    """
    Cursor for walking through the results of a :func:`sscan
    <trollius_redis.RedisProtocol.sscan>` query.
    """
    @asyncio.coroutine
    def fetchall(self):
        result = yield From(super(SetCursor, self).fetchall())
        raise Return(set(result))


class DictCursor(Cursor):
    """
    Cursor for walking through the results of a :func:`hscan
    <trollius_redis.RedisProtocol.hscan>` query.
    """
    def _parse(self, key, value):
        return key, value

    @asyncio.coroutine
    def fetchone(self):
        """
        Get next { key: value } tuple
        It returns `None` after the last item.
        """
        key = yield From(super(DictCursor, self).fetchone())
        value = yield From(super(DictCursor, self).fetchone())

        if key is not None:
            key, value = self._parse(key, value)
            raise Return({key: value})

    @asyncio.coroutine
    def fetchall(self):
        """ Coroutine that reads all the items in one dictionary. """
        results = {}

        while True:
            i = yield From(self.fetchone())
            if i is None:
                break
            else:
                results.update(i)

        raise Return(results)


class ZCursor(DictCursor):
    """
    Cursor for walking through the results of a :func:`zscan
    <trollius_redis.RedisProtocol.zscan>` query.
    """
    def _parse(self, key, value):
        # Mapping { key: score_as_float }
        return key, float(value)
