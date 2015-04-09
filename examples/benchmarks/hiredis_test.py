#!/usr/bin/env python
"""
Compare how fast HiRedisProtocol is compared to the pure Python implementation
for a few different benchmarks.
"""
from __future__ import print_function
import trollius as asyncio
from trollius import From
import asyncio_redis
import time

try:
    import hiredis
except ImportError:
    hiredis = None

from asyncio_redis.protocol import HiRedisProtocol


@asyncio.coroutine
def test1(connection):
    """ Del/get/set of keys """
    yield From(connection.delete(['key']))
    yield From(connection.set('key', 'value'))
    result = yield From(connection.get('key'))
    assert result == 'value'


@asyncio.coroutine
def test2(connection):
    """ Get/set of a hash of 100 items (with _asdict) """
    d = {unicode(i): unicode(i) for i in xrange(100)}

    yield From(connection.delete(['key']))
    yield From(connection.hmset('key', d))
    result = yield From(connection.hgetall_asdict('key'))
    assert result == d


@asyncio.coroutine
def test3(connection):
    """ Get/set of a hash of 100 items (without _asdict) """
    d = {unicode(i): unicode(i) for i in xrange(100)}

    yield From(connection.delete(['key']))
    yield From(connection.hmset('key', d))

    result = yield From(connection.hgetall('key'))
    d2 = {}

    for f in result:
        k, v = yield From(f)
        d2[k] = v

    assert d2 == d


@asyncio.coroutine
def test4(connection):
    """ sadd/smembers of a set of 100 items. (with _asset) """
    s = {unicode(i) for i in xrange(100)}

    yield From(connection.delete(['key']))
    yield From(connection.sadd('key', list(s)))

    s2 = yield From(connection.smembers_asset('key'))
    assert s2 == s


@asyncio.coroutine
def test5(connection):
    """ sadd/smembers of a set of 100 items. (without _asset) """
    s = {unicode(i) for i in xrange(100)}

    yield From(connection.delete(['key']))
    yield From(connection.sadd('key', list(s)))

    result = yield From(connection.smembers('key'))
    s2 = set()

    for f in result:
        i = yield From(f)
        s2.add(i)

    assert s2 == s


benchmarks = [
    (1000, test1),
    (100, test2),
    (100, test3),
    (100, test4),
    (100, test5),
]


def run():
    connection = yield From(
        asyncio_redis.Connection.create(host='localhost', port=6379))
    if hiredis:
        hiredis_connection = yield From(
            asyncio_redis.Connection.create(
                host='localhost', port=6379, protocol_class=HiRedisProtocol))

    try:
        for count, f in benchmarks:
            print('%ix %s' % (count, f.__doc__))

            # Benchmark without hredis
            start = time.time()
            for i in xrange(count):
                yield From(f(connection))
            print('      Pure Python: ', time.time() - start)

            # Benchmark with hredis
            if hiredis:
                start = time.time()
                for i in xrange(count):
                    yield From(f(hiredis_connection))
                print('      hiredis:     ', time.time() - start)
                print()
            else:
                print('      hiredis:     (not available)')
    finally:
        connection.close()
        if hiredis:
            hiredis_connection.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
