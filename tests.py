#!/usr/bin/env python
# encoding: utf8
from trollius.futures import Future
from trollius.tasks import gather
from trollius.test_utils import run_briefly
import six
from trollius_redis import (
    Connection,
    Error,
    ErrorReply,
    HiRedisProtocol,
    NoAvailableConnectionsInPoolError,
    NoRunningScriptError,
    NotConnectedError,
    Pool,
    RedisProtocol,
    Script,
    ScriptKilledError,
    Subscription,
    Transaction,
    TransactionError,
    ZScoreBoundary,
)
from trollius_redis.replies import (
    BlockingPopReply,
    ClientListReply,
    ConfigPairReply,
    DictReply,
    EvalScriptReply,
    InfoReply,
    ListReply,
    PubSubReply,
    SetReply,
    StatusReply,
    ZRangeReply,
)
from trollius_redis.exceptions import TimeoutError, ConnectionLostError
from trollius_redis.cursors import Cursor
from trollius_redis.encoders import BytesEncoder

import trollius as asyncio
from trollius import From, Return
import unittest
import os
import gc
import logging

logger = logging.getLogger('trollius')
logger.setLevel(500)
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setLevel(logging.INFO)
try:
    import hiredis
except ImportError:
    hiredis = None
from six.moves import range

PORT = int(os.environ.get('REDIS_PORT', 6379))
HOST = os.environ.get('REDIS_HOST', 'localhost')
START_REDIS_SERVER = bool(os.environ.get('START_REDIS_SERVER', False))


@asyncio.coroutine
def connect(loop, protocol=RedisProtocol):
    '''
    Connect to redis server. Return transport/protocol pair.
    '''
    if PORT:
        transport, protocol = yield From(loop.create_connection(
            lambda: protocol(loop=loop), HOST, PORT))
        raise Return(transport, protocol)
    else:
        transport, protocol = yield From(loop.create_unix_connection(
            lambda: protocol(loop=loop), HOST))
        raise Return(transport, protocol)


def redis_test(function):
    '''
    Decorator for methods (which are coroutines) in RedisProtocolTest

    Wraps the coroutine inside `run_until_complete`.
    '''
    function = asyncio.coroutine(function)

    def wrapper(self):
        @asyncio.coroutine
        def c():
            # Create connection
            transport, protocol = \
                yield From(connect(self.loop, self.protocol_class))

            # Run test
            try:
                yield From(function(self, transport, protocol))

            # Close connection
            finally:
                transport.close()

        self.loop.run_until_complete(c())
    return wrapper


class TestCase(unittest.TestCase):
    def tearDown(self):
        # Collect garbage on tearDown. (This can print ResourceWarnings.)
        result = gc.collect()


class RedisProtocolTest(TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = RedisProtocol

    # @redis_test
    # def test_ping(self, transport, protocol):
    #     result = yield From(protocol.ping())
    #     self.assertEqual(result, StatusReply(u'PONG'))
    #     self.assertEqual(repr(result), "StatusReply(status=u'PONG')")

    # @redis_test
    # def test_echo(self, transport, protocol):
    #     result = yield From(protocol.echo(u'my string'))
    #     self.assertEqual(result, u'my string')

    # @redis_test
    # def test_set_and_get(self, transport, protocol):
    #     # Set
    #     value = yield From(protocol.set(u'my_key', u'my_value'))
    #     self.assertEqual(value, StatusReply('OK'))

    #     # Get
    #     value = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(value, u'my_value')

    #     # Getset
    #     value = yield From(protocol.getset(u'my_key', u'new_value'))
    #     self.assertEqual(value, u'my_value')

    #     value = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(value, u'new_value')

    # @redis_test
    # def test_extended_set(self, transport, protocol):
    #     yield From(protocol.delete([u'my_key', u'other_key']))
    #     # set with expire only if not exists
    #     value = yield From(protocol.set(u'my_key', u'my_value',
    #                                     expire=10, only_if_not_exists=True))

    #     self.assertEqual(value, StatusReply(u'OK'))
    #     value = yield From(protocol.ttl(u'my_key'))
    #     self.assertIn(value, (10, 9))

    #     # check NX flag for SET command
    #     value = yield From(protocol.set(u'my_key', u'my_value',
    #                                     expire=10, only_if_not_exists=True))
    #     self.assertIsNone(value)

    #     # check XX flag for SET command
    #     value = yield From(
    #         protocol.set(u'other_key', u'some_value', only_if_exists=True))

    #     self.assertIsNone(value)

    #     # set with pexpire only if key exists
    #     value = yield From(protocol.set(u'my_key', u'other_value',
    #                                     pexpire=20000, only_if_exists=True))

    #     self.assertEqual(value, StatusReply(u'OK'))

    #     value = yield From(protocol.get(u'my_key'))

    #     self.assertEqual(value, u'other_value')

    #     value = yield From(protocol.ttl(u'my_key'))
    #     self.assertIn(value, (20, 19))

    # @redis_test
    # def test_setex(self, transport, protocol):
    #     # Set
    #     value = yield From(protocol.setex(u'my_key', 10, u'my_value'))
    #     self.assertEqual(value, StatusReply('OK'))

    #     # TTL
    #     value = yield From(protocol.ttl(u'my_key'))
    #     self.assertIn(value, (10, 9))  # may be some delay

    #     # Get
    #     value = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(value, u'my_value')

    # @redis_test
    # def test_setnx(self, transport, protocol):
    #     yield From(protocol.delete([u'my_key']))

    #     # Setnx while key does not exists
    #     value = yield From(protocol.setnx(u'my_key', u'my_value'))
    #     self.assertEqual(value, True)

    #     # Get
    #     value = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(value, u'my_value')

    #     # Setnx if key exists
    #     value = yield From(protocol.setnx(u'my_key', u'other_value'))
    #     self.assertEqual(value, False)

    #     # Get old value
    #     value = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(value, u'my_value')

    # @redis_test
    # def test_special_characters(self, transport, protocol):
    #     # Test some special unicode values and spaces.
    #     value = u'my value with special chars " # éçåø´¨åø´h '

    #     result = yield From(protocol.set(u'my key with spaces', value))
    #     result = yield From(protocol.get(u'my key with spaces'))
    #     self.assertEqual(result, value)

    #     # Test newlines
    #     value = u'ab\ncd\ref\r\ngh'
    #     result = yield From(protocol.set(u'my-key', value))
    #     result = yield From(protocol.get(u'my-key'))
    #     self.assertEqual(result, value)

    # @redis_test
    # def test_mget(self, transport, protocol):
    #     # mget
    #     yield From(protocol.set(u'my_key', u'a'))
    #     yield From(protocol.set(u'my_key2', u'b'))
    #     result = yield From(
    #         protocol.mget([u'my_key', u'my_key2', u'not_exists']))
    #     self.assertIsInstance(result, ListReply)
    #     result = yield From(result.aslist())
    #     self.assertEqual(result, [u'a', u'b', None])

    # @redis_test
    # def test_strlen(self, transport, protocol):
    #     yield From(protocol.delete([u'my_key']))
    #     yield From(protocol.delete([u'my_key2']))
    #     yield From(protocol.delete([u'my_key3']))
    #     yield From(protocol.set(u'my_key', u'my_value'))
    #     yield From(protocol.hset(u'my_key3', u'a', u'b'))

    #     # strlen
    #     value = yield From(protocol.strlen(u'my_key'))
    #     self.assertEqual(value, len(u'my_value'))

    #     value = yield From(protocol.strlen(u'my_key2'))
    #     self.assertEqual(value, 0)

    #     with self.assertRaises(ErrorReply):
    #         yield From(protocol.strlen(u'my_key3'))
    #     # Redis exception: b'ERR Operation against a key
    #     # holding the wrong kind of value')

    # @redis_test
    # def test_exists_and_delete(self, transport, protocol):
    #     # Set
    #     yield From(protocol.set(u'my_key', u'aaa'))
    #     value = yield From(protocol.append(u'my_key', u'bbb'))
    #     self.assertEqual(value, 6)  # Total length
    #     value = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(value, u'aaabbb')

    # @redis_test
    # def test_exists_and_delete2(self, transport, protocol):
    #     # Exists
    #     value = yield From(protocol.exists(u'unknown_key'))
    #     self.assertEqual(value, False)

    #     # Set
    #     value = yield From(protocol.set(u'known_key', u'value'))
    #     value = yield From(protocol.exists(u'known_key'))
    #     self.assertEqual(value, True)

    #     # Delete
    #     value = yield From(protocol.set(u'known_key2', u'value'))
    #     value = yield From(protocol.delete([u'known_key', u'known_key2']))
    #     self.assertEqual(value, 2)

    #     value = yield From(protocol.delete([u'known_key']))
    #     self.assertEqual(value, 0)

    #     value = yield From(protocol.exists(u'known_key'))
    #     self.assertEqual(value, False)

    # @redis_test
    # def test_rename(self, transport, protocol):
    #     # Set
    #     value = yield From(protocol.set(u'old_key', u'value'))
    #     value = yield From(protocol.exists(u'old_key'))
    #     self.assertEqual(value, True)

    #     # Rename
    #     value = yield From(protocol.rename(u'old_key', u'new_key'))
    #     self.assertEqual(value, StatusReply('OK'))

    #     value = yield From(protocol.exists(u'old_key'))
    #     self.assertEqual(value, False)
    #     value = yield From(protocol.exists(u'new_key'))
    #     self.assertEqual(value, True)

    #     value = yield From(protocol.get(u'old_key'))
    #     self.assertEqual(value, None)
    #     value = yield From(protocol.get(u'new_key'))
    #     self.assertEqual(value, 'value')

    #     # RenameNX
    #     yield From(protocol.delete([u'key3']))
    #     value = yield From(protocol.renamenx(u'new_key', u'key3'))
    #     self.assertEqual(value, 1)

    #     yield From(protocol.set(u'key4', u'existing-value'))
    #     value = yield From(protocol.renamenx(u'key3', u'key4'))
    #     self.assertEqual(value, 0)

    # @redis_test
    # def test_expire(self, transport, protocol):
    #     # Set
    #     value = yield From(protocol.set(u'key', u'value'))

    #     # Expire (10s)
    #     value = yield From(protocol.expire(u'key', 10))
    #     self.assertEqual(value, 1)

    #     value = yield From(protocol.exists(u'key'))
    #     self.assertEqual(value, True)

    #     # TTL
    #     value = yield From(protocol.ttl(u'key'))
    #     self.assertIsInstance(value, int)
    #     self.assertLessEqual(value, 10)

    #     # PTTL
    #     value = yield From(protocol.pttl(u'key'))
    #     self.assertIsInstance(value, int)
    #     self.assertLessEqual(value, 10 * 1000)

    #     # Pexpire
    #     value = yield From(protocol.pexpire(u'key', 10*1000))
    #     self.assertEqual(value, 1)  # XXX: check this
    #     value = yield From(protocol.pttl(u'key'))
    #     self.assertLessEqual(value, 10 * 1000)

    #     # Expire (1s) and wait
    #     value = yield From(protocol.expire(u'key', 1))
    #     value = yield From(protocol.exists(u'key'))
    #     self.assertEqual(value, True)

    #     yield From(asyncio.sleep(2, loop=self.loop))

    #     value = yield From(protocol.exists(u'key'))
    #     self.assertEqual(value, False)

    #     # Test persist
    #     yield From(protocol.set(u'key', u'value'))
    #     yield From(protocol.expire(u'key', 1))
    #     value = yield From(protocol.persist(u'key'))
    #     self.assertEqual(value, 1)
    #     value = yield From(protocol.persist(u'key'))
    #     self.assertEqual(value, 0)

    #     yield From(asyncio.sleep(2, loop=self.loop))

    #     value = yield From(protocol.exists(u'key'))
    #     self.assertEqual(value, True)

    #     # Test expireat
    #     value = yield From(protocol.expireat(u'key', 1293840000))
    #     self.assertIsInstance(value, int)

    #     # Test pexpireat
    #     value = yield From(protocol.pexpireat(u'key', 1555555555005))
    #     self.assertIsInstance(value, int)

    # @redis_test
    # def test_set(self, transport, protocol):
    #     # Create set
    #     value = yield From(protocol.delete([u'our_set']))
    #     value = yield From(protocol.sadd(u'our_set', [u'a', u'b']))
    #     value = yield From(protocol.sadd(u'our_set', [u'c']))
    #     self.assertEqual(value, 1)

    #     # scard
    #     value = yield From(protocol.scard(u'our_set'))
    #     self.assertEqual(value, 3)

    #     # Smembers
    #     value = yield From(protocol.smembers(u'our_set'))
    #     self.assertIsInstance(value, SetReply)
    #     self.assertEqual(repr(value), u"SetReply(length=3)")
    #     value = yield From(value.asset())
    #     self.assertEqual(value, {u'a', u'b', u'c'})

    #     # sismember
    #     value = yield From(protocol.sismember(u'our_set', u'a'))
    #     self.assertEqual(value, True)
    #     value = yield From(protocol.sismember(u'our_set', u'd'))
    #     self.assertEqual(value, False)

    #     # Intersection, union and diff
    #     yield From(protocol.delete([u'set2']))
    #     yield From(protocol.sadd(u'set2', [u'b', u'c', u'd', u'e']))

    #     value = yield From(protocol.sunion([u'our_set', 'set2']))
    #     self.assertIsInstance(value, SetReply)
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'a', u'b', u'c', u'd', u'e']))

    #     value = yield From(protocol.sinter([u'our_set', 'set2']))
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'b', u'c']))

    #     value = yield From(protocol.sdiff([u'our_set', 'set2']))
    #     self.assertIsInstance(value, SetReply)
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'a']))
    #     value = yield From(protocol.sdiff([u'set2', u'our_set']))
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'd', u'e']))

    #     # Interstore
    #     value = yield From(
    #         protocol.sinterstore(u'result', [u'our_set', 'set2']))
    #     self.assertEqual(value, 2)
    #     value = yield From(protocol.smembers(u'result'))
    #     self.assertIsInstance(value, SetReply)
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'b', u'c']))

    #     # Unionstore
    #     value = yield From(
    #         protocol.sunionstore(u'result', [u'our_set', 'set2']))
    #     self.assertEqual(value, 5)
    #     value = yield From(protocol.smembers(u'result'))
    #     self.assertIsInstance(value, SetReply)
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'a', u'b', u'c', u'd', u'e']))

    #     # Sdiffstore
    #     value = yield From(
    #         protocol.sdiffstore(u'result', [u'set2', 'our_set']))
    #     self.assertEqual(value, 2)
    #     value = yield From(protocol.smembers(u'result'))
    #     self.assertIsInstance(value, SetReply)
    #     value = yield From(value.asset())
    #     self.assertEqual(value, set([u'd', u'e']))

    # @redis_test
    # def test_srem(self, transport, protocol):
    #     yield From(protocol.delete([u'our_set']))
    #     yield From(protocol.sadd(u'our_set', [u'a', u'b', u'c', u'd']))

    #     # Call srem
    #     result = yield From(protocol.srem(u'our_set', [u'b', u'c']))
    #     self.assertEqual(result, 2)

    #     result = yield From(protocol.smembers(u'our_set'))
    #     self.assertIsInstance(result, SetReply)
    #     result = yield From(result.asset())
    #     self.assertEqual(result, set([u'a', u'd']))

    # @redis_test
    # def test_spop(self, transport, protocol):
    #     @asyncio.coroutine
    #     def setup():
    #         yield From(protocol.delete([u'my_set']))
    #         yield From(protocol.sadd(u'my_set', [u'value1']))
    #         yield From(protocol.sadd(u'my_set', [u'value2']))

    #     # Test spop
    #     yield From(setup())
    #     result = yield From(protocol.spop(u'my_set'))
    #     self.assertIn(result, [u'value1', u'value2'])
    #     result = yield From(protocol.smembers(u'my_set'))
    #     self.assertIsInstance(result, SetReply)
    #     result = yield From(result.asset())
    #     self.assertEqual(len(result), 1)

    #     # Test srandmember
    #     yield From(setup())
    #     result = yield From(protocol.srandmember(u'my_set'))
    #     self.assertIsInstance(result, SetReply)
    #     result = yield From(result.asset())
    #     self.assertIn(list(result)[0], [u'value1', u'value2'])
    #     result = yield From(protocol.smembers(u'my_set'))
    #     self.assertIsInstance(result, SetReply)
    #     result = yield From(result.asset())
    #     self.assertEqual(len(result), 2)

    # @redis_test
    # def test_type(self, transport, protocol):
    #     # Setup
    #     yield From(protocol.delete([u'key1']))
    #     yield From(protocol.delete([u'key2']))
    #     yield From(protocol.delete([u'key3']))

    #     yield From(protocol.set(u'key1', u'value'))
    #     yield From(protocol.lpush(u'key2', [u'value']))
    #     yield From(protocol.sadd(u'key3', [u'value']))

    #     # Test types
    #     value = yield From(protocol.type(u'key1'))
    #     self.assertEqual(value, StatusReply('string'))

    #     value = yield From(protocol.type(u'key2'))
    #     self.assertEqual(value, StatusReply('list'))

    #     value = yield From(protocol.type(u'key3'))
    #     self.assertEqual(value, StatusReply('set'))

    # @redis_test
    # def test_list(self, transport, protocol):
    #     # Create list
    #     yield From(protocol.delete([u'my_list']))
    #     value = yield From(protocol.lpush(u'my_list', [u'v1', u'v2']))
    #     value = yield From(protocol.rpush(u'my_list', [u'v3', u'v4']))
    #     self.assertEqual(value, 4)

    #     # lrange
    #     value = yield From(protocol.lrange(u'my_list'))
    #     self.assertIsInstance(value, ListReply)
    #     self.assertEqual(repr(value), u"ListReply(length=4)")
    #     value = yield From(value.aslist())
    #     self.assertEqual(value, [u'v2', 'v1', 'v3', 'v4'])

    #     # lset
    #     value = yield From(protocol.lset(u'my_list', 3, u'new-value'))
    #     self.assertEqual(value, StatusReply('OK'))

    #     value = yield From(protocol.lrange(u'my_list'))
    #     self.assertIsInstance(value, ListReply)
    #     value = yield From(value.aslist())
    #     self.assertEqual(value, [u'v2', 'v1', 'v3', 'new-value'])

    #     # lindex
    #     value = yield From(protocol.lindex(u'my_list', 1))
    #     self.assertEqual(value, 'v1')
    #     value = yield From(protocol.lindex(u'my_list', 10))  # Unknown index
    #     self.assertEqual(value, None)

    #     # Length
    #     value = yield From(protocol.llen(u'my_list'))
    #     self.assertEqual(value, 4)

    #     # Remove element from list.
    #     value = yield From(protocol.lrem(u'my_list', value=u'new-value'))
    #     self.assertEqual(value, 1)

    #     # Pop
    #     value = yield From(protocol.rpop(u'my_list'))
    #     self.assertEqual(value, u'v3')
    #     value = yield From(protocol.lpop(u'my_list'))
    #     self.assertEqual(value, u'v2')
    #     value = yield From(protocol.lpop(u'my_list'))
    #     self.assertEqual(value, u'v1')
    #     value = yield From(protocol.lpop(u'my_list'))
    #     self.assertEqual(value, None)

    #     # Blocking lpop
    #     test_order = []

    #     @asyncio.coroutine
    #     def blpop():
    #         test_order.append('#1')
    #         value = yield From(protocol.blpop([u'my_list']))
    #         self.assertIsInstance(value, BlockingPopReply)
    #         self.assertEqual(value.list_name, u'my_list')
    #         self.assertEqual(value.value, u'value')
    #         test_order.append('#3')
    #     f = asyncio.async(blpop(), loop=self.loop)

    #     transport2, protocol2 = yield From(connect(self.loop))

    #     test_order.append('#2')
    #     yield From(protocol2.rpush(u'my_list', [u'value']))
    #     yield From(f)
    #     self.assertEqual(test_order, ['#1', '#2', '#3'])

    #     # Blocking rpop
    #     @asyncio.coroutine
    #     def blpop():
    #         value = yield From(protocol.brpop([u'my_list']))
    #         self.assertIsInstance(value, BlockingPopReply)
    #         self.assertEqual(value.list_name, u'my_list')
    #         self.assertEqual(value.value, u'value2')
    #     f = asyncio.async(blpop(), loop=self.loop)

    #     yield From(protocol2.rpush(u'my_list', [u'value2']))
    #     yield From(f)

    #     transport2.close()

    # @redis_test
    # def test_brpoplpush(self, transport, protocol):
    #     yield From(protocol.delete([u'from']))
    #     yield From(protocol.delete([u'to']))
    #     yield From(protocol.lpush(u'to', [u'1']))

    #     @asyncio.coroutine
    #     def brpoplpush():
    #         result = yield From(protocol.brpoplpush(u'from', u'to'))
    #         self.assertEqual(result, u'my_value')
    #     f = asyncio.async(brpoplpush(), loop=self.loop)

    #     transport2, protocol2 = yield From(connect(self.loop))
    #     yield From(protocol2.rpush(u'from', [u'my_value']))
    #     yield From(f)

    #     transport2.close()

    # @redis_test
    # def test_blocking_timeout(self, transport, protocol):
    #     yield From(protocol.delete([u'from']))
    #     yield From(protocol.delete([u'to']))

    #     # brpoplpush
    #     with self.assertRaises(TimeoutError) as e:
    #         result = yield From(protocol.brpoplpush(u'from', u'to', 1))
    #     self.assertIn('Timeout in brpoplpush', e.exception.args[0])

    #     # brpop
    #     with self.assertRaises(TimeoutError) as e:
    #         result = yield From(protocol.brpop([u'from'], 1))
    #     self.assertIn('Timeout in blocking pop', e.exception.args[0])

    #     # blpop
    #     with self.assertRaises(TimeoutError) as e:
    #         result = yield From(protocol.blpop([u'from'], 1))
    #     self.assertIn('Timeout in blocking pop', e.exception.args[0])

    # @redis_test
    # def test_linsert(self, transport, protocol):
    #     # Prepare
    #     yield From(protocol.delete([u'my_list']))
    #     yield From(protocol.rpush(u'my_list', [u'1']))
    #     yield From(protocol.rpush(u'my_list', [u'2']))
    #     yield From(protocol.rpush(u'my_list', [u'3']))

    #     # Insert after
    #     result = yield From(protocol.linsert(u'my_list', u'1', u'A'))
    #     self.assertEqual(result, 4)
    #     result = yield From(protocol.lrange(u'my_list'))
    #     self.assertIsInstance(result, ListReply)
    #     result = yield From(result.aslist())
    #     self.assertEqual(result, [u'1', u'A', u'2', u'3'])

    #     # Insert before
    #     result = yield From(protocol.linsert(
    #         u'my_list', u'3', u'B', before=True))
    #     self.assertEqual(result, 5)
    #     result = yield From(protocol.lrange(u'my_list'))
    #     self.assertIsInstance(result, ListReply)
    #     result = yield From(result.aslist())
    #     self.assertEqual(result, [u'1', u'A', u'2', u'B', u'3'])

    # @redis_test
    # def test_rpoplpush(self, transport, protocol):
    #     # Prepare
    #     yield From(protocol.delete([u'my_list']))
    #     yield From(protocol.delete([u'my_list2']))
    #     yield From(protocol.lpush(u'my_list', [u'value']))
    #     yield From(protocol.lpush(u'my_list2', [u'value2']))

    #     value = yield From(protocol.llen(u'my_list'))
    #     value2 = yield From(protocol.llen(u'my_list2'))
    #     self.assertEqual(value, 1)
    #     self.assertEqual(value2, 1)

    #     # rpoplpush
    #     result = yield From(protocol.rpoplpush(u'my_list', u'my_list2'))
    #     self.assertEqual(result, u'value')
    #     result = yield From(protocol.rpoplpush(u'my_list', u'my_list2'))
    #     self.assertEqual(result, None)

    # @redis_test
    # def test_pushx(self, transport, protocol):
    #     yield From(protocol.delete([u'my_list']))

    #     # rpushx
    #     result = yield From(protocol.rpushx(u'my_list', u'a'))
    #     self.assertEqual(result, 0)

    #     yield From(protocol.rpush(u'my_list', [u'a']))
    #     result = yield From(protocol.rpushx(u'my_list', u'a'))
    #     self.assertEqual(result, 2)

    #     # lpushx
    #     yield From(protocol.delete([u'my_list']))
    #     result = yield From(protocol.lpushx(u'my_list', u'a'))
    #     self.assertEqual(result, 0)

    #     yield From(protocol.rpush(u'my_list', [u'a']))
    #     result = yield From(protocol.lpushx(u'my_list', u'a'))
    #     self.assertEqual(result, 2)

    # @redis_test
    # def test_ltrim(self, transport, protocol):
    #     yield From(protocol.delete([u'my_list']))
    #     yield From(protocol.lpush(u'my_list', [u'a']))
    #     yield From(protocol.lpush(u'my_list', [u'b']))
    #     result = yield From(protocol.ltrim(u'my_list'))
    #     self.assertEqual(result, StatusReply('OK'))

    # @redis_test
    # def test_hashes(self, transport, protocol):
    #     yield From(protocol.delete([u'my_hash']))

    #     # Set in hash
    #     result = yield From(protocol.hset(u'my_hash', u'key', u'value'))
    #     self.assertEqual(result, 1)
    #     result = yield From(protocol.hset(u'my_hash', u'key2', u'value2'))
    #     self.assertEqual(result, 1)

    #     # hlen
    #     result = yield From(protocol.hlen(u'my_hash'))
    #     self.assertEqual(result, 2)

    #     # hexists
    #     result = yield From(protocol.hexists(u'my_hash', u'key'))
    #     self.assertEqual(result, True)
    #     result = yield From(protocol.hexists(u'my_hash', u'unknown_key'))
    #     self.assertEqual(result, False)

    #     # Get from hash
    #     result = yield From(protocol.hget(u'my_hash', u'key2'))
    #     self.assertEqual(result, u'value2')
    #     result = yield From(protocol.hget(u'my_hash', u'unknown-key'))
    #     self.assertEqual(result, None)

    #     result = yield From(protocol.hgetall(u'my_hash'))
    #     self.assertIsInstance(result, DictReply)
    #     self.assertEqual(repr(result), u"DictReply(length=2)")
    #     result = yield From(result.asdict())
    #     self.assertEqual(result, {u'key': u'value', u'key2': u'value2'})

    #     result = yield From(protocol.hkeys(u'my_hash'))
    #     self.assertIsInstance(result, SetReply)
    #     result = yield From(result.asset())
    #     self.assertIsInstance(result, set)
    #     self.assertEqual(result, {u'key', u'key2'})

    #     result = yield From(protocol.hvals(u'my_hash'))
    #     self.assertIsInstance(result, ListReply)
    #     result = yield From(result.aslist())
    #     self.assertIsInstance(result, list)
    #     self.assertEqual(set(result), {u'value', u'value2'})

    #     # HDel
    #     result = yield From(protocol.hdel(u'my_hash', [u'key2']))
    #     self.assertEqual(result, 1)
    #     result = yield From(protocol.hdel(u'my_hash', [u'key2']))
    #     self.assertEqual(result, 0)

    #     result = yield From(protocol.hkeys(u'my_hash'))
    #     self.assertIsInstance(result, SetReply)
    #     result = yield From(result.asset())
    #     self.assertEqual(result, {u'key'})

    # @redis_test
    # def test_keys(self, transport, protocol):
    #     # Create some keys in this 'namespace'
    #     yield From(protocol.set(u'our-keytest-key1', u'a'))
    #     yield From(protocol.set(u'our-keytest-key2', u'a'))
    #     yield From(protocol.set(u'our-keytest-key3', u'a'))

    #     # Test 'keys'
    #     multibulk = yield From(protocol.keys(u'our-keytest-key*'))
    #     all_keys = []
    #     for f in multibulk:
    #         r = yield From(f)
    #         all_keys.append(r)
    #     self.assertEqual(
    #         set(all_keys),
    #         {
    #             u'our-keytest-key1',
    #             u'our-keytest-key2',
    #             u'our-keytest-key3'
    #         })

    # @redis_test
    # def test_hmset_get(self, transport, protocol):
    #     yield From(protocol.delete([u'my_hash']))
    #     yield From(protocol.hset(u'my_hash', u'a', u'1'))

    #     # HMSet
    #     result = yield From(protocol.hmset(
    #         u'my_hash', {u'b': u'2', u'c': u'3'}))
    #     self.assertEqual(result, StatusReply('OK'))

    #     # HMGet
    #     result = yield From(protocol.hmget(u'my_hash', [u'a', u'b', u'c']))
    #     self.assertIsInstance(result, ListReply)
    #     result = yield From(result.aslist())
    #     self.assertEqual(result, [u'1', u'2', u'3'])

    #     result = yield From(protocol.hmget(u'my_hash', [u'c', u'b']))
    #     self.assertIsInstance(result, ListReply)
    #     result = yield From(result.aslist())
    #     self.assertEqual(result, [u'3', u'2'])

    #     # Hsetnx
    #     result = yield From(protocol.hsetnx(u'my_hash', u'b', u'4'))
    #     self.assertEqual(result, 0)  # Existing key. Not set
    #     result = yield From(protocol.hget(u'my_hash', u'b'))
    #     self.assertEqual(result, u'2')

    #     result = yield From(protocol.hsetnx(u'my_hash', u'd', u'5'))
    #     self.assertEqual(result, 1)  # New key, set
    #     result = yield From(protocol.hget(u'my_hash', u'd'))
    #     self.assertEqual(result, u'5')

    # @redis_test
    # def test_hincr(self, transport, protocol):
    #     yield From(protocol.delete([u'my_hash']))
    #     yield From(protocol.hset(u'my_hash', u'a', u'10'))

    #     # hincrby
    #     result = yield From(protocol.hincrby(u'my_hash', u'a', 2))
    #     self.assertEqual(result, 12)

    #     # hincrbyfloat
    #     result = yield From(protocol.hincrbyfloat(u'my_hash', u'a', 3.7))
    #     self.assertEqual(result, 15.7)

    # @redis_test
    # def test_pubsub(self, transport, protocol):
    #     @asyncio.coroutine
    #     def listener():
    #         # Subscribe
    #         transport2, protocol2 = yield From(connect(self.loop))

    #         self.assertEqual(protocol2.in_pubsub, False)
    #         subscription = yield From(protocol2.start_subscribe())
    #         self.assertIsInstance(subscription, Subscription)
    #         self.assertEqual(protocol2.in_pubsub, True)
    #         yield From(subscription.subscribe([u'our_channel']))

    #         value = yield From(subscription.next_published())
    #         self.assertIsInstance(value, PubSubReply)
    #         self.assertEqual(value.channel, u'our_channel')
    #         self.assertEqual(value.value, u'message1')

    #         value = yield From(subscription.next_published())
    #         self.assertIsInstance(value, PubSubReply)
    #         self.assertEqual(value.channel, u'our_channel')
    #         self.assertEqual(value.value, u'message2')
    #         self.assertEqual(
    #             repr(value),
    #             "PubSubReply(channel=u'our_channel', value=u'message2')")

    #         raise Return(transport2)

    #     f = asyncio.async(listener(), loop=self.loop)

    #     @asyncio.coroutine
    #     def sender():
    #         value = yield From(protocol.publish(u'our_channel', u'message1'))
    #         # Nr of clients that received the message
    #         self.assertGreaterEqual(value, 1)
    #         value = yield From(protocol.publish(u'our_channel', u'message2'))
    #         self.assertGreaterEqual(value, 1)

    #         # Test pubsub_channels
    #         result = yield From(protocol.pubsub_channels())
    #         self.assertIsInstance(result, ListReply)
    #         result = yield From(result.aslist())
    #         self.assertIn(u'our_channel', result)

    #         result = yield From(protocol.pubsub_channels_aslist(u'our_c*'))
    #         self.assertIn(u'our_channel', result)

    #         result = yield From(protocol.pubsub_channels_aslist(
    #             u'unknown-channel-prefix*'))
    #         self.assertEqual(result, [])

    #         # Test pubsub numsub.
    #         result = yield From(protocol.pubsub_numsub(
    #             [u'our_channel', u'some_unknown_channel']))
    #         self.assertIsInstance(result, DictReply)
    #         result = yield From(result.asdict())
    #         self.assertEqual(len(result), 2)
    #         self.assertGreater(int(result[u'our_channel']), 0)
    #         # XXX: the cast to int is required, because the redis
    #         #      protocol currently returns strings instead of
    #         #      integers for the count. See:
    #         #      https://github.com/antirez/redis/issues/1561
    #         self.assertEqual(int(result[u'some_unknown_channel']), 0)

    #         # Test pubsub numpat
    #         result = yield From(protocol.pubsub_numpat())
    #         self.assertIsInstance(result, int)

    #     yield From(asyncio.sleep(.5, loop=self.loop))
    #     yield From(sender())
    #     transport2 = yield From(f)
    #     transport2.close()

    # @redis_test
    # def test_pubsub_many(self, transport, protocol):
    #     '''
    #     Create a listener that listens to several channels.
    #     '''
    #     @asyncio.coroutine
    #     def listener():
    #         # Subscribe
    #         transport2, protocol2 = yield From(connect(self.loop))

    #         self.assertEqual(protocol2.in_pubsub, False)
    #         subscription = yield From(protocol2.start_subscribe())
    #         yield From(subscription.subscribe([u'channel1', u'channel2']))
    #         yield From(subscription.subscribe([u'channel3', u'channel4']))

    #         results = []
    #         for i in range(4):
    #             results.append((yield From(subscription.next_published())))

    #         self.assertEqual(
    #             results,
    #             [
    #                 PubSubReply(u'channel1', u'message1'),
    #                 PubSubReply(u'channel2', u'message2'),
    #                 PubSubReply(u'channel3', u'message3'),
    #                 PubSubReply(u'channel4', u'message4'),
    #             ])

    #         transport2.close()

    #     f = asyncio.async(listener(), loop=self.loop)

    #     @asyncio.coroutine
    #     def sender():
    #         # Should not be received
    #         yield From(protocol.publish(u'channel5', u'message5'))

    #         # These for should be received.
    #         yield From(protocol.publish(u'channel1', u'message1'))
    #         yield From(protocol.publish(u'channel2', u'message2'))
    #         yield From(protocol.publish(u'channel3', u'message3'))
    #         yield From(protocol.publish(u'channel4', u'message4'))

    #     yield From(asyncio.sleep(.5, loop=self.loop))
    #     yield From(sender())
    #     yield From(f)

    # @redis_test
    # def test_pubsub_patterns(self, transport, protocol):
    #     '''
    #     Test a pubsub connection that subscribes to a pattern.
    #     '''
    #     @asyncio.coroutine
    #     def listener():
    #         # Subscribe to two patterns
    #         transport2, protocol2 = yield From(connect(self.loop))

    #         subscription = yield From(protocol2.start_subscribe())
    #         yield From(subscription.psubscribe([u'h*llo', u'w?rld']))

    #         # Receive messages
    #         results = []
    #         for i in range(4):
    #             results.append((yield From(subscription.next_published())))

    #         self.assertEqual(
    #             results, [
    #                 PubSubReply(u'hello', u'message1', pattern=u'h*llo'),
    #                 PubSubReply(u'heello', u'message2', pattern=u'h*llo'),
    #                 PubSubReply(u'world', u'message3', pattern=u'w?rld'),
    #                 PubSubReply(u'wArld', u'message4', pattern=u'w?rld'),
    #             ])

    #         transport2.close()

    #     f = asyncio.async(listener(), loop=self.loop)

    #     @asyncio.coroutine
    #     def sender():
    #         # Should not be received
    #         yield From(protocol.publish(u'other-channel', u'message5'))

    #         # These for should be received.
    #         yield From(protocol.publish(u'hello', u'message1'))
    #         yield From(protocol.publish(u'heello', u'message2'))
    #         yield From(protocol.publish(u'world', u'message3'))
    #         yield From(protocol.publish(u'wArld', u'message4'))

    #     yield From(asyncio.sleep(.5, loop=self.loop))
    #     yield From(sender())
    #     yield From(f)

    # @redis_test
    # def test_incr(self, transport, protocol):
    #     yield From(protocol.set(u'key1', u'3'))

    #     # Incr
    #     result = yield From(protocol.incr(u'key1'))
    #     self.assertEqual(result, 4)
    #     result = yield From(protocol.incr(u'key1'))
    #     self.assertEqual(result, 5)

    #     # Incrby
    #     result = yield From(protocol.incrby(u'key1', 10))
    #     self.assertEqual(result, 15)

    #     # Decr
    #     result = yield From(protocol.decr(u'key1'))
    #     self.assertEqual(result, 14)

    #     # Decrby
    #     result = yield From(protocol.decrby(u'key1', 4))
    #     self.assertEqual(result, 10)

    # @redis_test
    # def test_bitops(self, transport, protocol):
    #     yield From(protocol.set(u'a', u'fff'))
    #     yield From(protocol.set(u'b', u'555'))

    #     a = six.byte2int(b'f')
    #     b = six.byte2int(b'5')

    #     # Calculate set bits in the character 'f'
    #     set_bits = len([c for c in six.text_type(bin(a)) if c == u'1'])

    #     # Bitcount
    #     result = yield From(protocol.bitcount(u'a'))
    #     self.assertEqual(result, set_bits * 3)

    #     # And
    #     result = yield From(protocol.bitop_and(u'result', [u'a', u'b']))
    #     self.assertEqual(result, 3)
    #     result = yield From(protocol.get(u'result'))
    #     self.assertEqual(result, chr(a & b) * 3)

    #     # Or
    #     result = yield From(protocol.bitop_or(u'result', [u'a', u'b']))
    #     self.assertEqual(result, 3)
    #     result = yield From(protocol.get(u'result'))
    #     self.assertEqual(result, chr(a | b) * 3)

    #     # Xor
    #     result = yield From(protocol.bitop_xor(u'result', [u'a', u'b']))
    #     self.assertEqual(result, 3)
    #     result = yield From(protocol.get(u'result'))
    #     self.assertEqual(result, chr(a ^ b) * 3)

    #     # Not
    #     result = yield From(protocol.bitop_not(u'result', u'a'))
    #     self.assertEqual(result, 3)

    #         # Check result using bytes protocol
    #     bytes_transport, bytes_protocol = yield From(connect(
    #         self.loop,
    #         lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw)))
    #     result = yield From(bytes_protocol.get(b'result'))
    #     self.assertIsInstance(result, bytes)
    #     self.assertEqual(
    #         result, bytes(bytearray((~a % 256, ~a % 256, ~a % 256))))

    #     bytes_transport.close()

    # @redis_test
    # def test_setbit(self, transport, protocol):
    #     yield From(protocol.set(u'a', u'fff'))

    #     value = yield From(protocol.getbit(u'a', 3))
    #     self.assertIsInstance(value, bool)
    #     self.assertEqual(value, False)

    #     value = yield From(protocol.setbit(u'a', 3, True))
    #     self.assertIsInstance(value, bool)
    #     self.assertEqual(value, False)  # Set returns the old value.

    #     value = yield From(protocol.getbit(u'a', 3))
    #     self.assertIsInstance(value, bool)
    #     self.assertEqual(value, True)

    # @redis_test
    # def test_zscore(self, transport, protocol):
    #     yield From(protocol.delete([u'myzset']))

    #     # Test zscore return value for NIL server response
    #     value = yield From(protocol.zscore(u'myzset', u'key'))
    #     self.assertIsNone(value)

    #     # zadd key 4.0
    #     result = yield From(protocol.zadd(u'myzset', {u'key': 4}))
    #     self.assertEqual(result, 1)

    #     # Test zscore value for existing zset members
    #     value = yield From(protocol.zscore(u'myzset', u'key'))
    #     self.assertEqual(value, 4.0)

    # @redis_test
    # def test_zset(self, transport, protocol):
    #     yield From(protocol.delete(['myzset']))

    #     # Test zadd
    #     result = yield From(protocol.zadd(
    #         u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))
    #     self.assertEqual(result, 3)

    #     # Test zcard
    #     result = yield From(protocol.zcard(u'myzset'))
    #     self.assertEqual(result, 3)

    #     # Test zrank
    #     result = yield From(protocol.zrank(u'myzset', u'key'))
    #     self.assertEqual(result, 0)
    #     result = yield From(protocol.zrank(u'myzset', u'key3'))
    #     self.assertEqual(result, 2)

    #     result = yield From(protocol.zrank(u'myzset', u'unknown-key'))
    #     self.assertEqual(result, None)

    #     # Test revrank
    #     result = yield From(protocol.zrevrank(u'myzset', u'key'))
    #     self.assertEqual(result, 2)
    #     result = yield From(protocol.zrevrank(u'myzset', u'key3'))
    #     self.assertEqual(result, 0)

    #     result = yield From(protocol.zrevrank(u'myzset', u'unknown-key'))
    #     self.assertEqual(result, None)

    #     # Test zrange
    #     result = yield From(protocol.zrange(u'myzset'))
    #     self.assertIsInstance(result, ZRangeReply)
    #     self.assertEqual(repr(result), u"ZRangeReply(length=3)")

    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r, {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(protocol.zrange(u'myzset'))
    #     self.assertIsInstance(result, ZRangeReply)

    #     etalon = [(u'key', 4.0), (u'key2', 5.0), (u'key3', 5.5)]
    #     for i, f in enumerate(result):  # Ordering matters
    #         d = yield From(f)
    #         self.assertEqual(d, etalon[i])

    #     # Test zrange_asdict
    #     result = yield From(protocol.zrange_asdict(u'myzset'))
    #     self.assertEqual(result, {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     # Test zrange with negative indexes
    #     result = yield From(protocol.zrange(u'myzset', -2, -1))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r, {u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(protocol.zrange(u'myzset', -2, -1))
    #     self.assertIsInstance(result, ZRangeReply)

    #     for f in result:
    #         d = yield From(f)
    #         self.assertIn(d, [(u'key2', 5.0), (u'key3', 5.5)])

    #     # Test zrangebyscore
    #     result = yield From(protocol.zrangebyscore(u'myzset'))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r,
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(protocol.zrangebyscore(
    #         u'myzset', min=ZScoreBoundary(4.5)))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r, {u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(protocol.zrangebyscore(
    #         u'myzset', max=ZScoreBoundary(5.5)))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r,
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})
    #     result = yield From(protocol.zrangebyscore(
    #         u'myzset', max=ZScoreBoundary(5.5, exclude_boundary=True)))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r,
    #         {u'key': 4.0, u'key2': 5.0})

    #     # Test zrevrangebyscore (identical to zrangebyscore, unless we call
    #     # aslist)
    #     result = yield From(protocol.zrevrangebyscore(u'myzset'))
    #     self.assertIsInstance(result, DictReply)
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r,
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     self.assertEqual(
    #         (yield From(protocol.zrevrangebyscore_asdict(u'myzset'))),
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(
    #         protocol.zrevrangebyscore(u'myzset', min=ZScoreBoundary(4.5)))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r, {u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(
    #         protocol.zrevrangebyscore(u'myzset', max=ZScoreBoundary(5.5)))
    #     self.assertIsInstance(result, DictReply)
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r,
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})
    #     result = yield From(
    #         protocol.zrevrangebyscore(
    #             u'myzset', max=ZScoreBoundary(5.5, exclude_boundary=True)))
    #     r = yield From(result.asdict())
    #     self.assertEqual(
    #         r, {u'key': 4.0, u'key2': 5.0})

    # @redis_test
    # def test_zrevrange(self, transport, protocol):
    #     yield From(protocol.delete([u'myzset']))

    #     # Test zadd
    #     result = yield From(protocol.zadd(
    #         u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))
    #     self.assertEqual(result, 3)

    #     # Test zrevrange
    #     result = yield From(protocol.zrevrange(u'myzset'))
    #     self.assertIsInstance(result, ZRangeReply)
    #     self.assertEqual(repr(result), u"ZRangeReply(length=3)")
    #     self.assertEqual(
    #         (yield From(result.asdict())),
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     self.assertEqual(
    #         (yield From(protocol.zrevrange_asdict(u'myzset'))),
    #         {u'key': 4.0, u'key2': 5.0, u'key3': 5.5})

    #     result = yield From(protocol.zrevrange(u'myzset'))
    #     self.assertIsInstance(result, ZRangeReply)

    #     etalon = [(u'key3', 5.5), (u'key2', 5.0), (u'key', 4.0)]
    #     for i, f in enumerate(result):  # Ordering matters
    #         d = yield From(f)
    #         self.assertEqual(d, etalon[i])

    # @redis_test
    # def test_zset_zincrby(self, transport, protocol):
    #     yield From(protocol.delete([u'myzset']))
    #     yield From(protocol.zadd(
    #         u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))

    #     # Test zincrby
    #     result = yield From(protocol.zincrby(u'myzset', 1.1, u'key'))
    #     self.assertEqual(result, 5.1)

    #     result = yield From(protocol.zrange(u'myzset'))
    #     self.assertEqual(
    #         (yield From(result.asdict())),
    #         {u'key': 5.1, u'key2': 5.0, u'key3': 5.5})

    # @redis_test
    # def test_zset_zrem(self, transport, protocol):
    #     yield From(protocol.delete([u'myzset']))
    #     yield From(protocol.zadd(
    #         u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))

    #     # Test zrem
    #     result = yield From(protocol.zrem(u'myzset', [u'key']))
    #     self.assertEqual(result, 1)

    #     result = yield From(protocol.zrem(u'myzset', [u'key']))
    #     self.assertEqual(result, 0)

    #     result = yield From(protocol.zrange(u'myzset'))
    #     self.assertEqual(
    #         (yield From(result.asdict())),
    #         {u'key2': 5.0, u'key3': 5.5})

    @redis_test
    def test_zset_zrembyscore(self, transport, protocol):
        # Test zremrangebyscore (1)
        yield From(protocol.delete([u'myzset']))
        yield From(protocol.zadd(
            u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))

        result = yield From(protocol.zremrangebyscore(
            u'myzset', min=ZScoreBoundary(5.0)))
        self.assertEqual(result, 2)
        result = yield From(protocol.zrange(u'myzset'))
        self.assertEqual((yield From(result.asdict())), {u'key': 4.0})

        # Test zremrangebyscore (2)
        yield From(protocol.delete([u'myzset']))
        yield From(protocol.zadd(
            u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))

        result = yield From(protocol.zremrangebyscore(
            u'myzset', max=ZScoreBoundary(5.0)))
        self.assertEqual(result, 2)
        result = yield From(protocol.zrange(u'myzset'))
        self.assertEqual((yield From(result.asdict())), {u'key3': 5.5})

    @redis_test
    def test_zset_zremrangebyrank(self, transport, protocol):
        @asyncio.coroutine
        def setup():
            yield From(protocol.delete([u'myzset']))
            yield From(protocol.zadd(
                u'myzset', {u'key': 4, u'key2': 5, u'key3': 5.5}))

        # Test zremrangebyrank (1)
        yield From(setup())
        result = yield From(protocol.zremrangebyrank(u'myzset'))
        self.assertEqual(result, 3)
        result = yield From(protocol.zrange(u'myzset'))
        self.assertEqual((yield From(result.asdict())), {})

        # Test zremrangebyrank (2)
        yield From(setup())
        result = yield From(protocol.zremrangebyrank(u'myzset', min=2))
        self.assertEqual(result, 1)
        result = yield From(protocol.zrange(u'myzset'))
        self.assertEqual(
            (yield From(result.asdict())), {u'key': 4.0, u'key2': 5.0})

        # Test zremrangebyrank (3)
        yield From(setup())
        result = yield From(protocol.zremrangebyrank(u'myzset', max=1))
        self.assertEqual(result, 2)
        result = yield From(protocol.zrange(u'myzset'))
        self.assertEqual((yield From(result.asdict())), {u'key3': 5.5})

    # @redis_test
    # def test_zunionstore(self, transport, protocol):
    #     yield From(protocol.delete([u'set_a', u'set_b']))
    #     yield From(protocol.zadd(
    #         u'set_a', {u'key': 4, u'key2': 5, u'key3': 5.5}))
    #     yield From(protocol.zadd(
    #         u'set_b', {u'key': -1, u'key2': 1.1, u'key4': 9}))

    #     # Call zunionstore
    #     result = yield From(protocol.zunionstore(
    #         u'union_key', [u'set_a', u'set_b']))
    #     self.assertEqual(result, 4)
    #     result = yield From(protocol.zrange(u'union_key'))
    #     result = yield From(result.asdict())
    #     self.assertEqual(
    #         result, {u'key': 3.0, u'key2': 6.1, u'key3': 5.5, u'key4': 9.0})

    #     # Call zunionstore with weights.
    #     result = yield From(protocol.zunionstore(
    #         u'union_key', [u'set_a', u'set_b'], [1, 1.5]))
    #     self.assertEqual(result, 4)
    #     result = yield From(protocol.zrange(u'union_key'))
    #     result = yield From(result.asdict())
    #     self.assertEqual(
    #         result, {u'key': 2.5, u'key2': 6.65, u'key3': 5.5, u'key4': 13.5})

    # @redis_test
    # def test_zinterstore(self, transport, protocol):
    #     yield From(protocol.delete([u'set_a', u'set_b']))
    #     yield From(protocol.zadd(
    #         u'set_a', {u'key': 4, u'key2': 5, u'key3': 5.5}))
    #     yield From(protocol.zadd(
    #         u'set_b', {u'key': -1, u'key2': 1.5, u'key4': 9}))

    #     # Call zinterstore
    #     result = yield From(protocol.zinterstore(
    #         u'inter_key', [u'set_a', u'set_b']))
    #     self.assertEqual(result, 2)
    #     result = yield From(protocol.zrange(u'inter_key'))
    #     result = yield From(result.asdict())
    #     self.assertEqual(result, {u'key': 3.0, u'key2': 6.5})

    #     # Call zinterstore with weights.
    #     result = yield From(protocol.zinterstore(
    #         u'inter_key', [u'set_a', u'set_b'], [1, 1.5]))
    #     self.assertEqual(result, 2)
    #     result = yield From(protocol.zrange(u'inter_key'))
    #     result = yield From(result.asdict())
    #     self.assertEqual(result, {u'key': 2.5, u'key2': 7.25, })

    # @redis_test
    # def test_randomkey(self, transport, protocol):
    #     yield From(protocol.set(u'key1', u'value'))
    #     result = yield From(protocol.randomkey())
    #     self.assertIsInstance(result, six.text_type)

    # @redis_test
    # def test_dbsize(self, transport, protocol):
    #     result = yield From(protocol.dbsize())
    #     self.assertIsInstance(result, (int, long))

    # @redis_test
    # def test_client_names(self, transport, protocol):
    #     # client_setname
    #     result = yield From(protocol.client_setname(u'my-connection-name'))
    #     self.assertEqual(result, StatusReply('OK'))

    #     # client_getname
    #     result = yield From(protocol.client_getname())
    #     self.assertEqual(result, u'my-connection-name')

    #     # client list
    #     result = yield From(protocol.client_list())
    #     self.assertIsInstance(result, ClientListReply)

    # @redis_test
    # def test_lua_script(self, transport, protocol):
    #     code = '''
    #     local value = redis.call('GET', KEYS[1])
    #     value = tonumber(value)
    #     return value * ARGV[1]
    #     '''
    #     yield From(protocol.set('foo', '2'))

    #     # Register script
    #     script = yield From(protocol.register_script(code))
    #     self.assertIsInstance(script, Script)

    #     # Call script.
    #     result = yield From(script.run(keys=['foo'], args=['5']))
    #     self.assertIsInstance(result, EvalScriptReply)
    #     result = yield From(result.return_value())
    #     self.assertEqual(result, 10)

    #     # Test evalsha directly
    #     result = yield From(protocol.evalsha(
    #         script.sha, keys=['foo'], args=['5']))
    #     self.assertIsInstance(result, EvalScriptReply)
    #     result = yield From(result.return_value())
    #     self.assertEqual(result, 10)

    #     # Test script exists
    #     result = yield From(protocol.script_exists(
    #         [script.sha, script.sha, 'unknown-script']))
    #     self.assertEqual(result, [True, True, False])

    #     # Test script flush
    #     result = yield From(protocol.script_flush())
    #     self.assertEqual(result, StatusReply('OK'))

    #     result = yield From(protocol.script_exists(
    #         [script.sha, script.sha, 'unknown-script']))
    #     self.assertEqual(result, [False, False, False])

    #     # Test another script where evalsha returns a string.
    #     code2 = '''
    #     return "text"
    #     '''
    #     script2 = yield From(protocol.register_script(code2))
    #     result = yield From(protocol.evalsha(script2.sha))
    #     self.assertIsInstance(result, EvalScriptReply)
    #     result = yield From(result.return_value())
    #     self.assertIsInstance(result, six.text_type)
    #     self.assertEqual(result, u'text')

    # @redis_test
    # def test_script_return_types(self, transport, protocol):
    #     #  Test whether LUA scripts are returning correct return values.
    #     script_and_return_values = {
    #         u'return "string" ': "string",  # str
    #         u'return 5 ': 5,  # int
    #         u'return ': None,  # NoneType
    #         u'return {1, 2, 3}': [1, 2, 3],  # list

    #         # Complex nested data structure.
    #         u'return {1, 2, "text", {3, { 4, 5 }, 6, { 7, 8 } } }':
    #         [1, 2, "text", [3, [4, 5], 6, [7, 8]]],
    #     }
    #     for code, return_value in script_and_return_values.items():
    #         # Register script
    #         script = yield From(protocol.register_script(code))

    #         # Call script.
    #         scriptreply = yield From(script.run())
    #         result = yield From(scriptreply.return_value())
    #         self.assertEqual(result, return_value)

    # @redis_test
    # def test_script_kill(self, transport, protocol):
    #     # Test script kill (when nothing is running.)
    #     with self.assertRaises(NoRunningScriptError):
    #         result = yield From(protocol.script_kill())

    #     # Test script kill (when a while/true is running.)

    #     @asyncio.coroutine
    #     def run_while_true():
    #         code = '''
    #         local i = 0
    #         while true do
    #             i = i + 1
    #         end
    #         '''
    #         transport, protocol = yield From(connect(self.loop, RedisProtocol))

    #         script = yield From(protocol.register_script(code))
    #         with self.assertRaises(ScriptKilledError):
    #             yield From(script.run())

    #         transport.close()

    #     # (start script)
    #     f = asyncio.async(run_while_true(), loop=self.loop)
    #     yield From(asyncio.sleep(.5, loop=self.loop))

    #     result = yield From(protocol.script_kill())
    #     self.assertEqual(result, StatusReply('OK'))

    #     # Wait for the other coroutine to finish.
    #     yield From(f)

    # @redis_test
    # def test_transaction(self, transport, protocol):
    #     # Prepare
    #     yield From(protocol.set(u'my_key', u'a'))
    #     yield From(protocol.set(u'my_key2', u'b'))
    #     yield From(protocol.set(u'my_key3', u'c'))
    #     yield From(protocol.delete([u'my_hash']))
    #     yield From(protocol.hmset(u'my_hash', {u'a': u'1', u'b': u'2', u'c': u'3'}))

    #     # Start transaction
    #     self.assertEqual(protocol.in_transaction, False)
    #     transaction = yield From(protocol.multi())
    #     self.assertIsInstance(transaction, Transaction)
    #     self.assertEqual(protocol.in_transaction, True)

    #     # Run commands
    #     f1 = yield From(transaction.get(u'my_key'))
    #     f2 = yield From(transaction.mget([u'my_key', u'my_key2']))
    #     f3 = yield From(transaction.get(u'my_key3'))
    #     f4 = yield From(transaction.mget([u'my_key2', u'my_key3']))
    #     f5 = yield From(transaction.hgetall(u'my_hash'))

    #     for f in [f1, f2, f3, f4, f5]:
    #         self.assertIsInstance(f, Future)

    #     # Running commands directly on protocol should fail.
    #     with self.assertRaises(Error) as e:
    #         yield From(protocol.set(u'a', u'b'))
    #     self.assertEqual(
    #         e.exception.args[0],
    #         'Cannot run command inside transaction (use the '
    #         'Transaction object instead)')

    #     # Calling subscribe inside transaction should fail.
    #     with self.assertRaises(Error) as e:
    #         yield From(transaction.start_subscribe())
    #     self.assertEqual(
    #         e.exception.args[0],
    #         'Cannot start pubsub listener when a protocol is in use.')

    #     # Complete transaction
    #     result = yield From(transaction.execute())
    #     self.assertEqual(result, None)
    #     self.assertEqual(protocol.in_transaction, False)

    #     # Read futures
    #     r1 = yield From(f1)
    #     # 2 & 3 switched by purpose. (order shouldn't matter.)
    #     r3 = yield From(f3)
    #     r2 = yield From(f2)
    #     r4 = yield From(f4)
    #     r5 = yield From(f5)

    #     r2 = yield From(r2.aslist())
    #     r4 = yield From(r4.aslist())
    #     r5 = yield From(r5.asdict())

    #     self.assertEqual(r1, u'a')
    #     self.assertEqual(r2, [u'a', u'b'])
    #     self.assertEqual(r3, u'c')
    #     self.assertEqual(r4, [u'b', u'c'])
    #     self.assertEqual(r5, {u'a': u'1', u'b': u'2', u'c': u'3'})

    # @redis_test
    # def test_discard_transaction(self, transport, protocol):
    #     yield From(protocol.set(u'my_key', u'a'))

    #     transaction = yield From(protocol.multi())
    #     yield From(transaction.set(u'my_key', 'b'))

    #     # Discard
    #     result = yield From(transaction.discard())
    #     self.assertEqual(result, None)

    #     result = yield From(protocol.get(u'my_key'))
    #     self.assertEqual(result, u'a')

    #     # Calling anything on the transaction after discard should fail.
    #     with self.assertRaises(Error) as e:
    #         result = yield From(transaction.get(u'my_key'))
    #     self.assertEqual(
    #         e.exception.args[0], 'Transaction already finished or invalid.')

    # @redis_test
    # def test_nesting_transactions(self, transport, protocol):
    #     # That should fail.
    #     transaction = yield From(protocol.multi())

    #     with self.assertRaises(Error) as e:
    #         transaction = yield From(transaction.multi())
    #     self.assertEqual(e.exception.args[0], 'Multi calls can not be nested.')

    # # @redis_test
    # # def test_password(self, transport, protocol):
    # #     # Set password
    # #     result = yield From(protocol.config_set('requirepass', 'newpassword'))
    # #     self.assertIsInstance(result, StatusReply)

    # #     # Further redis queries should fail without re-authenticating.
    # #     with self.assertRaises(ErrorReply) as e:
    # #         yield From(protocol.set('my-key', 'value'))
    # #     self.assertEqual(
    # #         e.exception.args[0], 'NOAUTH Authentication required.')

    # #     # Reconnect:
    # #     result = yield From(protocol.auth('newpassword'))
    # #     self.assertIsInstance(result, StatusReply)

    # #     # Redis queries should work again.
    # #     result = yield From(protocol.set('my-key', 'value'))
    # #     self.assertIsInstance(result, StatusReply)

    # #     # Try connecting through new Protocol instance.
    # #     transport2, protocol2 = yield From(connect(
    # #         self.loop,
    # #         lambda **kw: RedisProtocol(password='newpassword', **kw)))
    # #     result = yield From(protocol2.set('my-key', 'value'))
    # #     self.assertIsInstance(result, StatusReply)
    # #     transport2.close()

    # #     # Reset password
    # #     result = yield From(protocol.config_set('requirepass', ''))
    # #     self.assertIsInstance(result, StatusReply)

    # @redis_test
    # def test_condfig(self, transport, protocol):
    #     # Config get
    #     result = yield From(protocol.config_get(u'loglevel'))
    #     self.assertIsInstance(result, ConfigPairReply)
    #     self.assertEqual(result.parameter, u'loglevel')
    #     self.assertIsInstance(result.value, six.text_type)

    #     # Config set
    #     result = yield From(protocol.config_set(u'loglevel', result.value))
    #     self.assertIsInstance(result, StatusReply)

    #     # Resetstat
    #     result = yield From(protocol.config_resetstat())
    #     self.assertIsInstance(result, StatusReply)

    #     # XXX: config_rewrite not tested.

    # @redis_test
    # def test_info(self, transport, protocol):
    #     result = yield From(protocol.info())
    #     self.assertIsInstance(result, InfoReply)
    #     # TODO: implement and test InfoReply class

    #     result = yield From(protocol.info(u'CPU'))
    #     self.assertIsInstance(result, InfoReply)

    # @redis_test
    # def test_scan(self, transport, protocol):
    #     # Run scan command
    #     cursor = yield From(protocol.scan(match=u'*'))
    #     self.assertIsInstance(cursor, Cursor)

    #     # Walk through cursor
    #     received = []
    #     while True:
    #         i = yield From(cursor.fetchone())
    #         if not i:
    #             break

    #         self.assertIsInstance(i, six.text_type)
    #         received.append(i)

    #     # The amount of keys should equal 'dbsize'
    #     dbsize = yield From(protocol.dbsize())
    #     self.assertEqual(dbsize, len(received))

    #     # Test fetchall
    #     cursor = yield From(protocol.scan(match='*'))
    #     received2 = yield From(cursor.fetchall())
    #     self.assertIsInstance(received2, list)
    #     self.assertEqual(set(received), set(received2))

    # @redis_test
    # def test_set_scan(self, transport, protocol):
    #     '''
    #     Test sscan '''
    #     size = 1000
    #     items = ['value-%i' % i for i in range(size)]

    #     # Create a huge set
    #     yield From(protocol.delete(['my-set']))
    #     yield From(protocol.sadd('my-set', items))

    #     # Scan this set.
    #     cursor = yield From(protocol.sscan('my-set'))

    #     received = []
    #     while True:
    #         i = yield From(cursor.fetchone())
    #         if not i:
    #             break

    #         self.assertIsInstance(i, six.text_type)
    #         received.append(i)

    #     # Check result
    #     self.assertEqual(len(received), size)
    #     self.assertEqual(set(received), set(items))

    #     # Test fetchall
    #     cursor = yield From(protocol.sscan('my-set'))
    #     received2 = yield From(cursor.fetchall())
    #     self.assertIsInstance(received2, set)
    #     self.assertEqual(set(received), received2)

    # @redis_test
    # def test_dict_scan(self, transport, protocol):
    #     '''
    #     Test hscan '''
    #     size = 1000
    #     items = {u'key-%i' % i: u'values-%i' % i for i in range(size)}

    #     # Create a huge set
    #     yield From(protocol.delete([u'my-dict']))
    #     yield From(protocol.hmset(u'my-dict', items))

    #     # Scan this set.
    #     cursor = yield From(protocol.hscan(u'my-dict'))

    #     received = {}
    #     while True:
    #         i = yield From(cursor.fetchone())
    #         if not i:
    #             break

    #         self.assertIsInstance(i, dict)
    #         received.update(i)

    #     # Check result
    #     self.assertEqual(len(received), size)
    #     self.assertEqual(received, items)

    #     # Test fetchall
    #     cursor = yield From(protocol.hscan(u'my-dict'))
    #     received2 = yield From(cursor.fetchall())
    #     self.assertIsInstance(received2, dict)
    #     self.assertEqual(received, received2)

    # @redis_test
    # def test_sorted_dict_scan(self, transport, protocol):
    #     '''
    #     Test zscan '''
    #     size = 1000
    #     items = {u'key-%i' % i: (i + 0.1) for i in range(size)}

    #     # Create a huge set
    #     yield From(protocol.delete([u'my-z']))
    #     yield From(protocol.zadd(u'my-z', items))

    #     # Scan this set.
    #     cursor = yield From(protocol.zscan(u'my-z'))

    #     received = {}
    #     while True:
    #         i = yield From(cursor.fetchone())
    #         if not i:
    #             break

    #         self.assertIsInstance(i, dict)
    #         received.update(i)

    #     # Check result
    #     self.assertEqual(len(received), size)
    #     self.assertEqual(received, items)

    #     # Test fetchall
    #     cursor = yield From(protocol.zscan(u'my-z'))
    #     received2 = yield From(cursor.fetchall())
    #     self.assertIsInstance(received2, dict)
    #     self.assertEqual(received, received2)

    # @redis_test
    # def test_alternate_gets(self, transport, protocol):
    #     '''
    #     Test _asdict/_asset/_aslist suffixes.
    #     '''
    #     # Prepare
    #     yield From(protocol.set(u'my_key', u'a'))
    #     yield From(protocol.set(u'my_key2', u'b'))

    #     yield From(protocol.delete([u'my_set']))
    #     yield From(protocol.sadd(u'my_set', [u'value1']))
    #     yield From(protocol.sadd(u'my_set', [u'value2']))

    #     yield From(protocol.delete([u'my_hash']))
    #     yield From(protocol.hmset(u'my_hash', {u'a': u'1', u'b': u'2', u'c': u'3'}))

    #     # Test mget_aslist
    #     result = yield From(protocol.mget_aslist(['my_key', 'my_key2']))
    #     self.assertEqual(result, [u'a', u'b'])
    #     self.assertIsInstance(result, list)

    #     # Test keys_aslist
    #     result = yield From(protocol.keys_aslist('some-prefix-'))
    #     self.assertIsInstance(result, list)

    #     # Test smembers
    #     result = yield From(protocol.smembers_asset(u'my_set'))
    #     self.assertEqual(result, {u'value1', u'value2'})
    #     self.assertIsInstance(result, set)

    #     # Test hgetall_asdict
    #     result = yield From(protocol.hgetall_asdict('my_hash'))
    #     self.assertEqual(result, {u'a': u'1', u'b': u'2', u'c': u'3'})
    #     self.assertIsInstance(result, dict)

    #     # test all inside a transaction.
    #     transaction = yield From(protocol.multi())
    #     f1 = yield From(transaction.mget_aslist(['my_key', 'my_key2']))
    #     f2 = yield From(transaction.smembers_asset(u'my_set'))
    #     f3 = yield From(transaction.hgetall_asdict('my_hash'))
    #     yield From(transaction.execute())

    #     result1 = yield From(f1)
    #     result2 = yield From(f2)
    #     result3 = yield From(f3)

    #     self.assertEqual(result1, [u'a', u'b'])
    #     self.assertIsInstance(result1, list)

    #     self.assertEqual(result2, {u'value1', u'value2'})
    #     self.assertIsInstance(result2, set)

    #     self.assertEqual(result3, {u'a': u'1', u'b': u'2', u'c': u'3'})
    #     self.assertIsInstance(result3, dict)

    # @redis_test
    # def test_cancellation(self, transport, protocol):
    #     '''
    #     Test CancelledError: when a query gets cancelled.
    #     '''
    #     yield From(protocol.delete(['key']))

    #     # Start a coroutine that runs a blocking command for 3seconds
    #     @asyncio.coroutine
    #     def run():
    #         yield From(protocol.brpop(['key'], 3))
    #     f = asyncio.async(run(), loop=self.loop)

    #     # We cancel the coroutine before the answer arrives.
    #     yield From(asyncio.sleep(.5, loop=self.loop))
    #     f.cancel()

    #     # Now there's a cancelled future in protocol._queue, the
    #     # protocol._push_answer function should notice that and ignore the
    #     # incoming result from our `brpop` in this case.
    #     yield From(protocol.set('key', 'value'))

    # @redis_test
    # def test_watch_1(self, transport, protocol):
    #     '''
    #     Test a transaction, using watch
    #     (Test using the watched key inside the transaction.)
    #     '''
    #     yield From(protocol.set(u'key', u'0'))
    #     yield From(protocol.set(u'other_key', u'0'))

    #     # Test
    #     self.assertEqual(protocol.in_transaction, False)
    #     t = yield From(protocol.multi(watch=['other_key']))
    #     self.assertEqual(protocol.in_transaction, True)

    #     yield From(t.set(u'key', u'value'))
    #     yield From(t.set(u'other_key', u'my_value'))
    #     yield From(t.execute())

    #     # Check
    #     self.assertEqual(protocol.in_transaction, False)

    #     result = yield From(protocol.get(u'key'))
    #     self.assertEqual(result, u'value')
    #     result = yield From(protocol.get(u'other_key'))
    #     self.assertEqual(result, u'my_value')

    # @redis_test
    # def test_watch_2(self, transport, protocol):
    #     '''
    #     Test using the watched key outside the transaction.
    #     (the transaction should fail in this case.)
    #     '''
    #     # Setup
    #     transport2, protocol2 = yield From(connect(self.loop))

    #     yield From(protocol.set(u'key', u'0'))
    #     yield From(protocol.set(u'other_key', u'0'))

    #     # Test
    #     t = yield From(protocol.multi(watch=[u'other_key']))
    #     yield From(protocol2.set(u'other_key', u'other_value'))
    #     yield From(t.set(u'other_key', u'value'))

    #     with self.assertRaises(TransactionError):
    #         yield From(t.execute())

    #     # Check
    #     self.assertEqual(protocol.in_transaction, False)
    #     result = yield From(protocol.get(u'other_key'))
    #     self.assertEqual(result, u'other_value')

    #     transport2.close()


class RedisBytesProtocolTest(TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = \
            lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw)

    @redis_test
    def test_bytes_protocol(self, transport, protocol):
        # When passing string instead of bytes, this protocol should raise an
        # exception.
        with self.assertRaises(TypeError):
            result = yield From(protocol.set(u'key', u'value'))

        # Setting bytes
        result = yield From(protocol.set(b'key', b'value'))
        self.assertEqual(result, StatusReply('OK'))

        # Getting bytes
        result = yield From(protocol.get(b'key'))
        self.assertEqual(result, b'value')

    @redis_test
    def test_pubsub(self, transport, protocol):
        '''
        Test pubsub with BytesEncoder. Channel names and data are now bytes.
        '''
        @asyncio.coroutine
        def listener():
            # Subscribe
            proto_spawn = \
                lambda **kw: RedisProtocol(encoder=BytesEncoder(), **kw)
            transport2, protocol2 = yield From(
                connect(self.loop, protocol=proto_spawn))

            subscription = yield From(protocol2.start_subscribe())
            yield From(subscription.subscribe([b'our_channel']))
            value = yield From(subscription.next_published())
            self.assertEqual(value.channel, b'our_channel')
            self.assertEqual(value.value, b'message1')

            raise Return(transport2)

        @asyncio.coroutine
        def sender():
            value = yield From(protocol.publish(b'our_channel', b'message1'))

        f = asyncio.async(listener(), loop=self.loop)
        yield From(asyncio.sleep(.5, loop=self.loop))
        yield From(sender())
        transport2 = yield From(f)
        transport2.close()


class NoTypeCheckingTest(TestCase):
    def test_protocol(self):
        # Override protocol, disabling type checking.
        def factory(**kw):
            return RedisProtocol(
                encoder=BytesEncoder(), enable_typechecking=False, **kw)

        loop = asyncio.get_event_loop()

        @asyncio.coroutine
        def test():
            transport, protocol = yield From(connect(loop, protocol=factory))

            # Setting values should still work.
            result = yield From(protocol.set(b'key', b'value'))
            self.assertEqual(result, StatusReply('OK'))

            transport.close()

        loop.run_until_complete(test())


class RedisConnectionTest(TestCase):
    '''
    Test connection class.
    '''
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def test_connection(self):
        @asyncio.coroutine
        def test():
            # Create connection
            connection = yield From(Connection.create(host=HOST, port=PORT))
            self.assertEqual(
                repr(connection),
                "Connection(host=%r, port=%r)" % (HOST, PORT))
            self.assertEqual(connection._closing, False)

            # Test get/set
            yield From(connection.set(u'key', u'value'))
            result = yield From(connection.get(u'key'))
            self.assertEqual(result, u'value')

            connection.close()

            # Test closing flag
            self.assertEqual(connection._closing, True)

        self.loop.run_until_complete(test())


# class RedisPoolTest(TestCase):
#     '''
#     Test connection pooling.
#     '''
#     def setUp(self):
#         self.loop = asyncio.get_event_loop()

#     def test_pool(self):
#         '''
#         Test creation of Connection instance.
#         '''
#         @asyncio.coroutine
#         def test():
#             # Create pool
#             connection = yield From(Pool.create(host=HOST, port=PORT))
#             self.assertEqual(
#                 repr(connection),
#                 "Pool(host=%r, port=%r, poolsize=1)" % (HOST, PORT))

#             # Test get/set
#             yield From(connection.set('key', 'value'))
#             result = yield From(connection.get('key'))
#             self.assertEqual(result, 'value')

#             # Test default poolsize
#             self.assertEqual(connection.poolsize, 1)

#             connection.close()

#         self.loop.run_until_complete(test())

#     def test_connection_in_use(self):
#         '''
#         When a blocking call is running, it's impossible to use the same
#         protocol for another call.
#         '''
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             connection = yield From(Pool.create(host=HOST, port=PORT))
#             self.assertEqual(connection.connections_in_use, 0)

#             # Wait for ever. (This blocking pop doesn't return.)
#             yield From(connection.delete(['unknown-key']))
#             f = asyncio.async(
#                 connection.blpop(['unknown-key']), loop=self.loop)
#             # Sleep to make sure that the above coroutine started executing.
#             yield From(asyncio.sleep(.1, loop=self.loop))

#             # Run command in other thread.
#             with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
#                 yield From(connection.set('key', 'value'))
#             self.assertIn(
#                 'No available connections in the pool', e.exception.args[0])

#             self.assertEqual(connection.connections_in_use, 1)

#             connection.close()

#             # Consume this future (which now contains ConnectionLostError)
#             with self.assertRaises(ConnectionLostError):
#                 yield From(f)

#         self.loop.run_until_complete(test())

#     def test_parallel_requests(self):
#         '''
#         Test a blocking pop and a set using a connection pool.
#         '''
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             connection = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=2))
#             yield From(connection.delete(['my-list']))

#             results = []

#             # Sink: receive items using blocking pop
#             @asyncio.coroutine
#             def sink():
#                 for i in range(0, 5):
#                     reply = yield From(connection.blpop(['my-list']))
#                     self.assertIsInstance(reply, BlockingPopReply)
#                     self.assertIsInstance(reply.value, six.text_type)
#                     results.append(reply.value)
#                     self.assertIn(
#                         u"BlockingPopReply(list_name='my-list', value='",
#                         repr(reply))

#             # Source: Push items on the queue
#             @asyncio.coroutine
#             def source():
#                 for i in range(0, 5):
#                     yield From(connection.rpush('my-list', [str(i)]))
#                     yield From(asyncio.sleep(.5, loop=self.loop))

#             # Run both coroutines.
#             f1 = asyncio.async(source(), loop=self.loop)
#             f2 = asyncio.async(sink(), loop=self.loop)
#             yield From(gather(f1, f2))

#             # Test results.
#             self.assertEqual(results, [str(i) for i in range(0, 5)])

#             connection.close()

#         self.loop.run_until_complete(test())

#     def test_select_db(self):
#         '''
#         Connect to two different DBs.
#         '''
#         @asyncio.coroutine
#         def test():
#             c1 = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=10, db=1))
#             c2 = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=10, db=2))

#             c3 = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=10, db=1))
#             c4 = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=10, db=2))

#             yield From(c1.set('key', 'A'))
#             yield From(c2.set('key', 'B'))

#             r1 = yield From(c3.get('key'))
#             r2 = yield From(c4.get('key'))

#             self.assertEqual(r1, 'A')
#             self.assertEqual(r2, 'B')

#             for c in [c1, c2, c3, c4]:
#                 c.close()

#         self.loop.run_until_complete(test())

#     def test_in_use_flag(self):
#         '''
#         Do several blocking calls and see whether in_use increments.
#         '''
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             connection = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=10))
#             for i in range(0, 10):
#                 yield From(connection.delete(['my-list-%i' % i]))

#             @asyncio.coroutine
#             def sink(i):
#                 the_list, result = yield From(
#                     connection.blpop(['my-list-%i' % i]))

#             futures = []
#             for i in range(0, 10):
#                 self.assertEqual(connection.connections_in_use, i)
#                 futures.append(asyncio.async(sink(i), loop=self.loop))
#                 # Sleep to make sure that the above coroutine started
#                 # executing.
#                 yield From(asyncio.sleep(.1, loop=self.loop))

#             # One more blocking call should fail.
#             with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
#                 yield From(connection.delete(['my-list-one-more']))
#                 yield From(connection.blpop(['my-list-one-more']))
#             self.assertIn(
#                 'No available connections in the pool', e.exception.args[0])

#             connection.close()

#             # Consume this futures (which now contain ConnectionLostError)
#             with self.assertRaises(ConnectionLostError):
#                 yield From(asyncio.gather(*futures))

#         self.loop.run_until_complete(test())

#     def test_lua_script_in_pool(self):
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             connection = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=3))

#             # Register script
#             script = yield From(connection.register_script("return 100"))
#             self.assertIsInstance(script, Script)

#             # Run script
#             scriptreply = yield From(script.run())
#             result = yield From(scriptreply.return_value())
#             self.assertEqual(result, 100)

#             connection.close()

#         self.loop.run_until_complete(test())

#     def test_transactions(self):
#         '''
#         Do several transactions in parallel.
#         '''
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             connection = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=3))

#             t1 = yield From(connection.multi())
#             t2 = yield From(connection.multi())
#             yield From(connection.multi())

#             # Fourth transaction should fail. (Pool is full)
#             with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
#                 yield From(connection.multi())
#             self.assertIn(
#                 'No available connections in the pool', e.exception.args[0])

#             # Run commands in transaction
#             yield From(t1.set(u'key', u'value'))
#             yield From(t2.set(u'key2', u'value2'))

#             # Commit.
#             yield From(t1.execute())
#             yield From(t2.execute())

#             # Check
#             result1 = yield From(connection.get(u'key'))
#             result2 = yield From(connection.get(u'key2'))

#             self.assertEqual(result1, u'value')
#             self.assertEqual(result2, u'value2')

#             connection.close()

#         self.loop.run_until_complete(test())

#     def test_connection_reconnect(self):
#         '''
#         Test whether the connection reconnects.
#         (needs manual interaction.)
#         '''
#         @asyncio.coroutine
#         def test():
#             connection = yield From(
#                 Pool.create(host=HOST, port=PORT, poolsize=1))
#             yield From(connection.set('key', 'value'))

#             # Try the reconnect cycle several times. (Be sure that the
#             # `connection_lost` callback doesn't set variables that avoid
#             # reconnection a second time.)
#             for i in range(3):
#                 transport = connection._connections[0].transport
#                 transport.close()

#                 # Give asyncio time to reconnect.
#                 yield From(asyncio.sleep(1, loop=self.loop))

#                 # Test get/set
#                 yield From(connection.set('key', 'value'))

#             connection.close()

#         self.loop.run_until_complete(test())

#     def test_connection_lost(self):
#         '''
#         When the transport is closed, any further commands should raise
#         NotConnectedError. (Unless the transport would be auto-reconnecting and
#         have established a new connection.)
#         '''
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             transport, protocol = yield From(connect(self.loop, RedisProtocol))
#             yield From(protocol.set('key', 'value'))

#             # Close transport
#             self.assertEqual(protocol.is_connected, True)
#             transport.close()
#             yield From(asyncio.sleep(.5, loop=self.loop))
#             self.assertEqual(protocol.is_connected, False)

#             # Test get/set
#             with self.assertRaises(NotConnectedError):
#                 yield From(protocol.set('key', 'value'))

#             transport.close()

#         self.loop.run_until_complete(test())

#         # Test connection lost in connection pool.
#         @asyncio.coroutine
#         def test():
#             # Create connection
#             connection = yield From(Pool.create(
#                 host=HOST, port=PORT, poolsize=1, auto_reconnect=False))
#             yield From(connection.set('key', 'value'))

#             # Close transport
#             transport = connection._connections[0].transport
#             transport.close()
#             yield From(asyncio.sleep(.5, loop=self.loop))

#             # Test get/set
#             with self.assertRaises(NoAvailableConnectionsInPoolError) as e:
#                 yield From(connection.set('key', 'value'))
#             self.assertIn(
#                 'No available connections in the pool: size=1,'
#                 ' in_use=0, connected=0', e.exception.args[0])

#             connection.close()

#         self.loop.run_until_complete(test())


class NoGlobalLoopTest(TestCase):
    '''
    If we set the global loop variable to None, everything should still work.
    '''
    def test_no_global_loop(self):
        old_loop = asyncio.get_event_loop()
        try:
            # Remove global loop and create a new one.
            asyncio.set_event_loop(None)
            new_loop = asyncio.new_event_loop()

            # ** Run code on the new loop. **

            # Create connection
            connection = new_loop.run_until_complete(
                Connection.create(host=HOST, port=PORT, loop=new_loop))

            self.assertIsInstance(connection, Connection)
            try:
                # Delete keys
                new_loop.run_until_complete(
                    connection.delete([u'key1', u'key2']))

                # Get/set
                new_loop.run_until_complete(connection.set(u'key1', u'value'))
                result = new_loop.run_until_complete(connection.get(u'key1'))
                self.assertEqual(result, u'value')

                # hmset/hmget (something that uses a MultiBulkReply)
                new_loop.run_until_complete(
                    connection.hmset(u'key2', {u'a': u'b', u'c': u'd'}))
                result = new_loop.run_until_complete(
                    connection.hgetall_asdict(u'key2'))
                self.assertEqual(result, {u'a': u'b', u'c': u'd'})
            finally:
                connection.close()
        finally:
            # Run loop briefly until socket has been closed.
            # (call_soon behind the scenes.)
            run_briefly(new_loop)

            new_loop.close()
            asyncio.set_event_loop(old_loop)


class RedisProtocolWithoutGlobalEventloopTest(RedisProtocolTest):
    '''
    Run all the tests from `RedisProtocolTest` again without a global
    event loop.
    '''
    def setUp(self):
        super(RedisProtocolWithoutGlobalEventloopTest, self).setUp()

        # Remove global loop and create a new one.
        self._old_loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(self._old_loop)
        super(RedisProtocolWithoutGlobalEventloopTest, self).tearDown()


class RedisBytesWithoutGlobalEventloopProtocolTest(RedisBytesProtocolTest):
    '''
    Run all the tests from `RedisBytesProtocolTest`` again without a global
    event loop.
    '''
    def setUp(self):
        super(RedisBytesWithoutGlobalEventloopProtocolTest, self).setUp()

        # Remove global loop and create a new one.
        self._old_loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(self._old_loop)
        super(RedisBytesWithoutGlobalEventloopProtocolTest, self).tearDown()


# def _start_redis_server(loop):
#     print('Running Redis server REDIS_HOST=%r REDIS_PORT=%r...' % (HOST, PORT))

#     redis_srv = loop.run_until_complete(
#         asyncio.create_subprocess_exec(
#             'redis-server',
#             '--port', str(PORT),
#             ('--bind' if PORT else '--unixsocket'), HOST,
#             '--maxclients', '100',
#             '--save', '""',
#             '--loglevel', 'warning',
#             loop=loop,
#             stdout=open(os.devnull),
#             stderr=open(os.devnull)))
#     loop.run_until_complete(asyncio.sleep(.05, loop=loop))
#     return redis_srv


@unittest.skipIf(hiredis is None, 'Hiredis not found.')
class HiRedisProtocolTest(RedisProtocolTest):
    def setUp(self):
        super(HiRedisProtocolTest, self).setUp()
        self.protocol_class = HiRedisProtocol


@unittest.skipIf(hiredis is None, 'Hiredis not found.')
class HiRedisBytesProtocolTest(RedisBytesProtocolTest):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.protocol_class = \
            lambda **kw: HiRedisProtocol(encoder=BytesEncoder(), **kw)


if __name__ == '__main__':
    if START_REDIS_SERVER:
        redis_srv = _start_redis_server(asyncio.get_event_loop())

    try:
        unittest.main()
    finally:
        if START_REDIS_SERVER:
            redis_srv.terminate()
