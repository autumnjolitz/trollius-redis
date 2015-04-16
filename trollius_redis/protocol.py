#!/usr/bin/env python3
import logging
import types
import trollius as asyncio
from trollius import From, Return
from trollius.futures import Future
from trollius.queues import Queue
from trollius.streams import StreamReader
import six
range = six.moves.range

try:
    import hiredis
except ImportError:
    hiredis = None

from collections import deque
from functools import wraps
from inspect import formatargspec, getcallargs
from inspect import getargspec as getfullargspec

from .encoders import BaseEncoder, UTF8Encoder
from .exceptions import (
    ConnectionLostError,
    Error,
    ErrorReply,
    NoRunningScriptError,
    NotConnectedError,
    ScriptKilledError,
    TimeoutError,
    TransactionError,
)
from .log import logger
from .replies import (
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

from .cursors import Cursor, SetCursor, DictCursor, ZCursor

__all__ = (
    'RedisProtocol',
    'HiRedisProtocol',
    'Transaction',
    'Subscription',
    'Script',

    'ZAggregate',
    'ZScoreBoundary',
)

try:
    long
except NameError:
    long = int


def typedef(*input_types, **kwargs):
    try:
        return_type = kwargs['return_type']
    except KeyError:
        if not kwargs:
            return_type = input_types[:-1]
        else:
            raise SyntaxError

    def decorator(func):
        func.return_type = return_type
        func.input_types = input_types
        return func
    return decorator


NoneType = type(None)


class ZScoreBoundary(object):
    """
    Score boundary for a sorted set.
    for queries like zrangebyscore and similar

    :param value: Value for the boundary.
    :type value: float
    :param exclude_boundary: Exclude the boundary.
    :type exclude_boundary: bool
    """
    def __init__(self, value, exclude_boundary=False):
        assert isinstance(value, float) or value in (u'+inf', u'-inf')
        self.value = value
        self.exclude_boundary = exclude_boundary

    def __repr__(self):
        return 'ZScoreBoundary(value=%r, exclude_boundary=%r)' % (
            self.value, self.exclude_boundary)

ZScoreBoundary.MIN_VALUE = ZScoreBoundary(u'-inf')
ZScoreBoundary.MAX_VALUE = ZScoreBoundary(u'+inf')


class ZAggregate(object):  # TODO: use the Python 3.4 enum type.
    """
    Aggregation method for zinterstore and zunionstore.
    """
    #: Sum aggregation.
    SUM = u'SUM'

    #: Min aggregation.
    MIN = u'MIN'

    #: Max aggregation.
    MAX = u'MAX'


class PipelinedCall(object):
    """ Track record for call that is being executed in a protocol. """
    __slots__ = ('cmd', 'is_blocking')

    def __init__(self, cmd, is_blocking):
        self.cmd = cmd
        self.is_blocking = is_blocking


class MultiBulkReply(object):
    """
    Container for a multi bulk reply.
    """
    def __init__(self, protocol, count, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        #: Buffer of incoming, undelivered data, received from the parser.
        self._data_queue = []

        #: Incoming read queries.
        #: Contains (read_count, Future, decode_flag, one_only_flag) tuples.
        self._f_queue = deque()

        self.protocol = protocol
        self.count = int(count)

    def _feed_received(self, item):
        """
        Feed entry for the parser.
        """
        # Push received items on the queue
        self._data_queue.append(item)
        self._flush()

    def _flush(self):
        """
        Answer read queries when we have enough data in our multibulk reply.
        """
        # As long as we have more data in our queue then we require for a read
        # query  -> answer queries.
        while self._f_queue and self._f_queue[0][0] <= len(self._data_queue):
            # Pop query.
            count, f, decode, one_only = self._f_queue.popleft()

            # Slice data buffer.
            data, self._data_queue = \
                self._data_queue[:count], self._data_queue[count:]

            # When the decode flag is given, decode bytes to native types.
            if decode:
                data = [self._decode(d) for d in data]

            # When one_only flag has been given, don't return an array.
            if one_only:
                assert len(data) == 1
                f.set_result(data[0])
            else:
                f.set_result(data)

    def _decode(self, result):
        """ Decode bytes to native Python types. """
        if isinstance(result, (StatusReply, int, long, float, MultiBulkReply)):
            # Note that MultiBulkReplies can be nested.
            # e.g. in the 'scan' operation.
            return result
        elif isinstance(result, bytes):
            return self.protocol.decode_to_native(result)
        elif result is None:
            return result
        else:
            raise AssertionError('Invalid type: %r' % type(result))

    def _read(self, decode=True, count=1, _one=False):
        """ Do read operation on the queue. Return future. """
        f = Future(loop=self.protocol._loop)
        self._f_queue.append((count, f, decode, _one))

        # If there is enough data on the queue, answer future immediately.
        self._flush()

        return f

    def iter_raw(self):
        """
        Iterate over all multi bulk packets. This yields futures that won't
        decode bytes yet.
        """
        for i in range(self.count):
            yield self._read(decode=False, _one=True)

    def __iter__(self):
        """
        Iterate over the reply. This yields coroutines of the decoded packets.
        It decodes bytes automatically using protocol.decode_to_native.
        """
        for i in range(self.count):
            yield self._read(_one=True)

    def __repr__(self):
        return 'MultiBulkReply(protocol=%r, count=%r)' % \
            (self.protocol, self.count)


class _ScanPart(object):
    """ Internal: result chunk of a scan operation. """
    def __init__(self, new_cursor_pos, items):
        self.new_cursor_pos = new_cursor_pos
        self.items = items


class PostProcessors(object):
    """
    At the protocol level, we only know about a few basic classes; they
    include: bool, int, StatusReply, MultiBulkReply and bytes.
    This will return a postprocessor function that turns these into more
    meaningful objects.

    For some methods, we have several post processors. E.g. a list can be
    returned either as a ListReply (which has some special streaming
    functionality), but also as a Python list.
    """
    @classmethod
    def get_all(cls, return_type):
        """
        Return list of (suffix, return_type, post_processor)
        """
        default = cls.get_default(return_type)
        alternate = cls.get_alternate_post_processor(return_type)

        result = [('', return_type, default)]
        if alternate:
            result.append(alternate)
        return result

    @classmethod
    def get_default(cls, return_type):
        """ Give post processor function for return type. """
        return {
            ListReply: cls.multibulk_as_list,
            SetReply: cls.multibulk_as_set,
            DictReply: cls.multibulk_as_dict,

            float: cls.bytes_to_float,
            (float, NoneType): cls.bytes_to_float_or_none,
            NativeType: cls.bytes_to_native,
            (NativeType, NoneType): cls.bytes_to_native_or_none,
            InfoReply: cls.bytes_to_info,
            ClientListReply: cls.bytes_to_clientlist,
            six.text_type: cls.bytes_to_str,
            bool: cls.int_to_bool,
            BlockingPopReply: cls.multibulk_as_blocking_pop_reply,
            ZRangeReply: cls.multibulk_as_zrangereply,

            StatusReply: cls.bytes_to_status_reply,
            (StatusReply, NoneType): cls.bytes_to_status_reply_or_none,
            int: None,
            six.integer_types: None,
            (int, NoneType): None,
            ConfigPairReply: cls.multibulk_as_configpair,
            ListOf(bool): cls.multibulk_as_boolean_list,
            _ScanPart: cls.multibulk_as_scanpart,
            EvalScriptReply: cls.any_to_evalscript,

            NoneType: None,
        }[return_type]

    @classmethod
    def get_alternate_post_processor(cls, return_type):
        """ For list/set/dict. Create additional post processors that return
        python classes rather than ListReply/SetReply/DictReply """
        original_post_processor = cls.get_default(return_type)

        if return_type == ListReply:
            @asyncio.coroutine
            def as_list(protocol, result):
                result = yield From(original_post_processor(protocol, result))
                raise Return((yield From(result.aslist())))
            return '_aslist', list, as_list

        elif return_type == SetReply:
            @asyncio.coroutine
            def as_set(protocol, result):
                result = yield From(original_post_processor(protocol, result))
                raise Return((yield From(result.asset())))
            return '_asset', set, as_set

        elif return_type in (DictReply, ZRangeReply):
            @asyncio.coroutine
            def as_dict(protocol, result):
                result = yield From(original_post_processor(protocol, result))
                raise Return((yield From(result.asdict())))
            return '_asdict', dict, as_dict

    # === Post processor handlers below. ===

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_list(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return ListReply(result)

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_boolean_list(protocol, result):
        # Turn the array of integers into booleans.
        assert isinstance(result, MultiBulkReply)
        values = yield From(ListReply(result).aslist())
        raise Return([bool(v) for v in values])

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_set(protocol, result):
        assert isinstance(result, MultiBulkReply), \
            'Result is not MultiBulkReply, is {0}, {1}'.format(type(result), result)
        return SetReply(result)

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_dict(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return DictReply(result)

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_zrangereply(protocol, result):
        assert isinstance(result, MultiBulkReply)
        return ZRangeReply(result)

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_blocking_pop_reply(protocol, result):
        if result is None:
            raise TimeoutError('Timeout in blocking pop')
        else:
            assert isinstance(result, MultiBulkReply)
            list_name, value = yield From(ListReply(result).aslist())
            raise Return(BlockingPopReply(list_name, value))

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_configpair(protocol, result):
        assert isinstance(result, MultiBulkReply)
        parameter, value = yield From(ListReply(result).aslist())
        raise Return(ConfigPairReply(parameter, value))

    @staticmethod
    @asyncio.coroutine
    def multibulk_as_scanpart(protocol, result):
        """
        Process scanpart result.
        This is a multibulk reply of length two, where the first item is the
        new cursor position and the second item is a nested multi bulk reply
        containing all the elements.
        """
        # Get outer multi bulk reply.
        assert isinstance(result, MultiBulkReply)
        new_cursor_pos, items_bulk = yield From(ListReply(result).aslist())
        assert isinstance(items_bulk, MultiBulkReply)

        # Read all items for scan chunk in memory. This is fine, because it's
        # transmitted in chunks of about 10.
        items = yield From(ListReply(items_bulk).aslist())
        raise Return(_ScanPart(int(new_cursor_pos), items))

    @staticmethod
    @asyncio.coroutine
    def bytes_to_info(protocol, result):
        assert isinstance(result, bytes)
        return InfoReply(result)

    @staticmethod
    @asyncio.coroutine
    def bytes_to_status_reply(protocol, result):
        assert isinstance(result, bytes), \
            "Result is not bytes. Is {0}, {1}".format(type(result), result)
        return StatusReply(result.decode('utf-8'))

    @staticmethod
    @asyncio.coroutine
    def bytes_to_status_reply_or_none(protocol, result):
        assert isinstance(result, (bytes, NoneType))
        if result:
            return StatusReply(result.decode('utf-8'))

    @staticmethod
    @asyncio.coroutine
    def bytes_to_clientlist(protocol, result):
        assert isinstance(result, bytes)
        return ClientListReply(result)

    @staticmethod
    @asyncio.coroutine
    def int_to_bool(protocol, result):
        assert isinstance(result, six.integer_types), \
            "Cannot coerce {0}. Wrong data type.".format(type(result))
        return bool(result)  # Convert int to bool

    @staticmethod
    @asyncio.coroutine
    def bytes_to_native(protocol, result):
        assert isinstance(result, bytes), \
            "Cannot coerce {0}".format(type(result))
        return protocol.decode_to_native(result)

    @staticmethod
    @asyncio.coroutine
    def bytes_to_str(protocol, result):
        assert isinstance(result, bytes), \
            "Cannot coerce {0}".format(type(result))
        return result.decode('ascii')

    @staticmethod
    @asyncio.coroutine
    def bytes_to_native_or_none(protocol, result):
        if result is None:
            return result
        else:
            assert isinstance(result, bytes), \
                "Cannot coerce {0}".format(type(result))
            return protocol.decode_to_native(result)

    @staticmethod
    @asyncio.coroutine
    def bytes_to_float_or_none(protocol, result):
        if result is None:
            return result
        assert isinstance(result, bytes)
        return float(result)

    @staticmethod
    @asyncio.coroutine
    def bytes_to_float(protocol, result):
        assert isinstance(result, bytes)
        return float(result)

    @staticmethod
    @asyncio.coroutine
    def any_to_evalscript(protocol, result):
        # Result can be native, int, MultiBulkReply or even a nested structure
        assert isinstance(result, (int, long, bytes, MultiBulkReply, NoneType))
        return EvalScriptReply(protocol, result)


class ListOf(object):
    """ Annotation helper for protocol methods. """
    def __init__(self, type_):
        self.type = type_

    def __repr__(self):
        return 'ListOf(%r)' % self.type

    def __eq__(self, other):
        return isinstance(other, ListOf) and other.type == self.type

    def __hash__(self):
        return hash((ListOf, self.type))


class NativeType(object):
    """
    Constant which represents the native Python type that's used.
    """
    def __new__(cls):
        raise Exception('NativeType is not meant to be initialized.')


class CommandCreator(object):
    """
    Utility for creating a wrapper around the Redis protocol methods.
    This will also do type checking.

    This wrapper handles (optionally) post processing of the returned data and
    implements some logic where commands behave different in case of a
    transaction or pubsub.

    Warning: We use the annotations of `method` extensively for type checking
             and determining which post processor to choose.
    """
    def __init__(self, method):
        self.method = method

    @property
    def specs(self):
        """ Argspecs """
        return getfullargspec(self.method)

    @property
    def return_type(self):
        """ Return type as defined in the method's annotation. """
        return getattr(self.method, 'return_type', NoneType)

    @property
    def params(self):
        types = getattr(self.method, 'input_types', None)
        if types:
            return {k: v for k, v in zip(
                [x for x in self.specs.args if x != 'self'], types)}
        return {}
        # return {k: v for k, v in self.specs.annotations.items()
        #         if k != 'return'}

    @classmethod
    def get_real_type(cls, protocol, type_):
        """
        Given a protocol instance, and type annotation, return something that
        we can pass to isinstance for the typechecking.
        """
        # If NativeType was given, replace it with the type of the protocol
        # itself.
        if isinstance(type_, tuple):
            return tuple(cls.get_real_type(protocol, t) for t in type_)

        if type_ == NativeType:
            return protocol.native_type
        elif isinstance(type_, ListOf):
            # We don't check the content of the list.
            return (list, types.GeneratorType)
        else:
            return type_

    def _create_input_typechecker(self):
        """ Return function that does typechecking on input data. """
        params = self.params

        if params:
            def typecheck_input(protocol, *a, **kw):
                """
                Given a protocol instance and *a/**kw of this method, raise
                TypeError when the signature doesn't match.
                """
                if protocol.enable_typechecking:
                    for name, value in getcallargs(
                            self.method, None, *a, **kw).items():
                        if name in params:
                            real_type = self.get_real_type(
                                protocol, params[name])
                            if not isinstance(value, real_type):
                                raise TypeError(
                                    'RedisProtocol.%s received %r, expected %r'
                                    % (self.method.__name__,
                                       type(value).__name__, real_type))
        else:
            def typecheck_input(protocol, *a, **kw):
                pass

        return typecheck_input

    def _create_return_typechecker(self, return_type):
        """ Return function that does typechecking on output data. """
        # Exclude 'Transaction'/'Subscription' which are 'str'
        if return_type and not isinstance(return_type, six.text_type):
            def typecheck_return(protocol, result):
                """
                Given protocol and result value. Raise TypeError if
                the result is of the wrong type.
                """
                if protocol.enable_typechecking:
                    expected_type = self.get_real_type(protocol, return_type)
                    try:
                        res = not isinstance(result, expected_type)
                    except Exception:
                        print(expected_type, type(result))
                        raise
                    else:
                        if res:
                            raise TypeError(
                                'Got unexpected return type %r in RedisProtocol.%s'
                                ', expected %r' %
                                (type(result).__name__, self.method.__name__,
                                 expected_type))
        else:
            def typecheck_return(protocol, result):
                pass

        return typecheck_return

    def _get_docstring(self, suffix, return_type):
        # Append the real signature as the first line in the docstring.
        # (This will make the sphinx docs show the real signature instead of
        # (*a, **kw) of the wrapper.)
        # (But don't put the anotations inside the copied signature, that's
        # rather ugly in the docs.)
        signature = formatargspec(* self.specs[:6])

        # Use function annotations to generate param documentation.

        def get_name(type_):
            """ Turn type annotation into doc string. """
            try:
                v = {
                    BlockingPopReply: (
                        ":class:`BlockingPopReply <trollius_redis.replies."
                        "BlockingPopReply>`"),
                    ConfigPairReply: (
                        ":class:`ConfigPairReply <trollius_redis.replies."
                        "ConfigPairReply>`"),
                    DictReply: (
                        ":class:`DictReply <trollius_redis.replies."
                        "DictReply>`"),
                    InfoReply: (
                        ":class:`InfoReply <trollius_redis.replies."
                        "InfoReply>`"),
                    ClientListReply: (
                        ":class:`InfoReply <trollius_redis.replies."
                        "ClientListReply>`"),
                    ListReply: (
                        ":class:`ListReply <trollius_redis.replies.ListReply>`"
                    ),
                    MultiBulkReply: (
                        ":class:`MultiBulkReply <trollius_redis.replies."
                        "MultiBulkReply>`"),
                    NativeType: (
                        "Native Python type, as defined by :attr:`"
                        "~trollius_redis.encoders.BaseEncoder.native_type`"),
                    NoneType: ("None"),
                    SetReply: (
                        ":class:`SetReply <trollius_redis.replies.SetReply>`"),
                    StatusReply: (
                        ":class:`StatusReply <trollius_redis."
                        "replies.StatusReply>`"),
                    ZRangeReply: (
                        ":class:`ZRangeReply <trollius_redis."
                        "replies.ZRangeReply>`"),
                    ZScoreBoundary: (
                        ":class:`ZScoreBoundary <trollius_redis."
                        "replies.ZScoreBoundary>`"),
                    EvalScriptReply: (
                        ":class:`EvalScriptReply <trollius_redis."
                        "replies.EvalScriptReply>`"),
                    Cursor: (
                        ":class:`Cursor <trollius_redis.cursors.Cursor>`"),
                    SetCursor: (
                        ":class:`SetCursor <trollius_redis."
                        "cursors.SetCursor>`"),
                    DictCursor: (
                        ":class:`DictCursor <trollius_redis."
                        "cursors.DictCursor>`"),
                    ZCursor: (
                        ":class:`ZCursor <trollius_redis.cursors.ZCursor>`"),
                    _ScanPart: ":class:`_ScanPart",
                    int: 'int',
                    bool: 'bool',
                    dict: 'dict',
                    float: 'float',
                    six.text_type: 'str',
                    six.binary_type: 'bytes',

                    list: 'list',
                    set: 'set',
                    dict: 'dict',

                    # XXX: Because of circulare references, we cannot use the
                    # real types here.
                    'Transaction': ":class:`trollius_redis.Transaction`",
                    'Subscription': ":class:`trollius_redis.Subscription`",
                    'Script': ":class:`~trollius_redis.Script`",
                }
                if long is not int:
                    v[long] = 'long'
                return v[type_]
            except KeyError:
                if isinstance(type_, ListOf):
                    return "List or iterable of %s" % get_name(type_.type)

                elif isinstance(type_, tuple):
                    return ' or '.join(get_name(t) for t in type_)
                else:
                    raise Exception('Unknown annotation %r' % type_)
                    #return "``%s``" % type_.__name__

        def get_param(k, v):
            return ':param %s: %s\n' % (k, get_name(v))

        params_str = [get_param(k, v) for k, v in self.params.items()]
        returns = \
            ':returns: (Future of) %s\n' % get_name(return_type) \
            if return_type else ''

        return '%s%s\n%s\n\n%s%s' % (
            self.method.__name__ + suffix, signature,
            self.method.__doc__,
            ''.join(params_str),
            returns
        )

    def get_methods(self):
        """
        Return all the methods to be used in the RedisProtocol class.
        """
        return [('', self._get_wrapped_method(None, '', self.return_type))]

    def _get_wrapped_method(self, post_process, suffix, return_type):
        """
        Return the wrapped method for use in the `RedisProtocol` class.
        """
        typecheck_input = self._create_input_typechecker()
        typecheck_return = self._create_return_typechecker(return_type)
        method = self.method

        # Wrap it into a check which allows this command to be run either
        # directly on the protocol, outside of transactions or from the
        # transaction object.
        @wraps(method)
        def wrapper(protocol_self, *a, **kw):
            # When calling from a transaction
            if protocol_self.in_transaction:
                # The first arg should be the transaction # object.
                if not a or a[0] != protocol_self._transaction:
                    raise Error(
                        'Cannot run command inside transaction (use the '
                        'Transaction object instead)')

                # In case of a transaction, we receive a Future from the
                # command.
                else:
                    typecheck_input(protocol_self, *a[1:], **kw)
                    future = yield From(method(protocol_self, *a[1:], **kw))
                    future2 = Future(loop=protocol_self._loop)

                    # Typecheck the future when the result is available.

                    @asyncio.coroutine
                    def done(result):
                        if post_process:
                            result = yield From(
                                post_process(protocol_self, result))
                        typecheck_return(protocol_self, result)
                        future2.set_result(result)

                    future.add_done_callback(
                        lambda f: asyncio.async(
                            done(f.result()), loop=protocol_self._loop))

                    raise Return(future2)

            # When calling from a pubsub context
            elif protocol_self.in_pubsub:
                if not a or a[0] != protocol_self._subscription:
                    raise Error(
                        'Cannot run command inside pubsub subscription.')

                else:
                    typecheck_input(protocol_self, *a[1:], **kw)
                    result = yield From(method(protocol_self, *a[1:], **kw))
                    if post_process:
                        result = yield From(
                            post_process(protocol_self, result))
                    typecheck_return(protocol_self, result)
                    raise Return(result)

            else:
                typecheck_input(protocol_self, *a, **kw)
                result = yield From(method(protocol_self, *a, **kw))
                if post_process:
                    result = yield From(post_process(protocol_self, result))
                typecheck_return(protocol_self, result)
                raise Return(result)

        wrapper.__doc__ = self._get_docstring(suffix, return_type)
        return wrapper


class QueryCommandCreator(CommandCreator):
    """
    Like `CommandCreator`, but for methods registered with `_query_command`.
    This are the methods that cause commands to be send to the server.

    Most of the commands get a reply from the server that needs to be post
    processed to get the right Python type. We inspect here the
    'returns'-annotation to determine the correct post processor.
    """
    def get_methods(self):
        # (Some commands, e.g. those that return a ListReply can generate
        # multiple protocol methods.  One that does return the ListReply, but
        # also one with the 'aslist' suffix that returns a Python list.)
        all_post_processors = PostProcessors.get_all(self.return_type)
        result = []

        for suffix, return_type, post_processor in all_post_processors:
            result.append(
                (suffix, self._get_wrapped_method(
                    post_processor, suffix, return_type)))

        return result

_SMALL_INTS = list(six.text_type(i).encode('ascii') for i in range(1000))


# List of all command methods.
_all_commands = []


class _command(object):
    """ Mark method as command (to be passed through CommandCreator for the
    creation of a protocol method) """
    creator = CommandCreator

    def __init__(self, method):
        self.method = method


class _query_command(_command):
    """
    Mark method as query command: This will pass through QueryCommandCreator.

    NOTE: be sure to choose the correct 'returns'-annotation.
    This will automatially determine the correct post processor function in
    class:`PostProcessors`.
    """
    creator = QueryCommandCreator

    def __init__(self, method):
        super(_query_command, self).__init__(method)


class _RedisProtocolMeta(type):
    """
    Metaclass for `RedisProtocol` which applies the _command decorator.
    """
    def __new__(cls, name, bases, attrs):
        for attr_name, value in dict(attrs).items():
            if isinstance(value, _command):
                creator = value.creator(value.method)
                for suffix, method in creator.get_methods():
                    attrs[attr_name + suffix] = method

                    # Register command.
                    _all_commands.append(attr_name + suffix)

        return type.__new__(cls, name, bases, attrs)


class RedisProtocol(six.with_metaclass(_RedisProtocolMeta, asyncio.Protocol)):
    """
    The Redis Protocol implementation.

    ::

        self.loop = trollius.get_event_loop()
        transport, protocol = yield From(loop.create_connection(
            RedisProtocol, 'localhost', 6379))

    :param password: Redis database password
    :type password: Native Python type as defined by the ``encoder`` parameter
    :param encoder: Encoder to use for encoding to or decoding from redis bytes
                    to a native type.
                    (Defaults to :class:`~trollius_redis.encoders.UTF8Encoder`)
    :type encoder: :class:`~trollius_redis.encoders.BaseEncoder` instance.
    :param db: Redis database
    :type db: int
    :param enable_typechecking: When ``True``, check argument types for all
                                redis commands. Normally you want to have this
                                enabled.
    :type enable_typechecking: bool
    """

    def __init__(self, password=None, db=0, encoder=None,
                 connection_lost_callback=None, enable_typechecking=True,
                 loop=None):
        if encoder is None:
            encoder = UTF8Encoder()

        assert isinstance(db, int)
        assert isinstance(encoder, BaseEncoder)
        assert encoder.native_type, 'Encoder.native_type not defined'
        assert not password or isinstance(password, encoder.native_type)

        self.password = password
        self.db = db
        self._connection_lost_callback = connection_lost_callback
        self._loop = loop or asyncio.get_event_loop()

        # Take encode / decode settings from encoder
        self.encode_from_native = encoder.encode_from_native
        self.decode_to_native = encoder.decode_to_native
        self.native_type = encoder.native_type
        self.enable_typechecking = enable_typechecking

        self.transport = None
        self._queue = deque()  # Input parser queues
        self._messages_queue = None  # Pubsub queue
        # True as long as the underlying transport is connected.
        self._is_connected = False

        # Pubsub state
        self._in_pubsub = False
        self._subscription = None
        self._pubsub_channels = set()  # Set of channels
        self._pubsub_patterns = set()  # Set of patterns

        # Transaction related stuff.
        self._in_transaction = False
        self._transaction = None
        self._transaction_response_queue = None  # Transaction answer queue

        self._line_received_handlers = {
            b'+': self._handle_status_reply,
            b'-': self._handle_error_reply,
            b'$': self._handle_bulk_reply,
            b'*': self._handle_multi_bulk_reply,
            b':': self._handle_int_reply,
        }

    def connection_made(self, transport):
        self.transport = transport
        self._is_connected = True
        logger.log(logging.INFO, 'Redis connection made')

        # Pipelined calls
        self._pipelined_calls = set()  # Set of all the pipelined calls.

        # Start parsing reader stream.
        self._reader = StreamReader(loop=self._loop)
        self._reader.set_transport(transport)
        self._reader_f = asyncio.async(
            self._reader_coroutine(), loop=self._loop)

        @asyncio.coroutine
        def initialize():
            # If a password or database was been given, first connect to
            # that one.
            if self.password:
                yield From(self.auth(self.password))

            if self.db:
                yield From(self.select(self.db))

            #  If we are in pubsub mode, send channel subscriptions again.
            if self._in_pubsub:
                if self._pubsub_channels:
                    # TODO: unittest this
                    yield From(self._subscribe(
                        self._subscription, list(self._pubsub_channels)))

                if self._pubsub_patterns:
                    yield From(self._psubscribe(
                        self._subscription, list(self._pubsub_patterns)))

        asyncio.async(initialize(), loop=self._loop)

    def data_received(self, data):
        """ Process data received from Redis server.  """
        self._reader.feed_data(data)

    @typedef(int, return_type=bytes)
    def _encode_int(self, value):
        """ Encodes an integer to bytes. (always ascii) """
        if 0 < value < 1000:  # For small values, take pre-encoded string.
            return _SMALL_INTS[value]
        else:
            return six.text_type(value).encode('ascii')

    @typedef(float, return_type=bytes)
    def _encode_float(self, value):
        """ Encodes a float to bytes. (always ascii) """
        return six.text_type(value).encode('ascii')

    @typedef(ZScoreBoundary, return_type=six.text_type)
    def _encode_zscore_boundary(self, value):
        """ Encodes a zscore boundary. (always ascii) """
        if isinstance(value.value, six.text_type):
            return six.text_type(value.value).encode('ascii')  # +inf and -inf
        elif value.exclude_boundary:
            return six.text_type("(%f" % value.value).encode('ascii')
        else:
            return six.text_type("%f" % value.value).encode('ascii')

    def eof_received(self):
        logger.log(logging.INFO, 'EOF received in RedisProtocol')
        self._reader.feed_eof()

    def connection_lost(self, exc):
        if exc is None:
            self._reader.feed_eof()
        else:
            logger.info("Connection lost with exec: %s" % exc)
            self._reader.set_exception(exc)

        if self._reader_f:
            self._reader_f.cancel()

        self._is_connected = False
        self.transport = None
        self._reader = None
        self._reader_f = None

        # Raise exception on all waiting futures.
        while self._queue:
            f = self._queue.popleft()
            if not f.cancelled():
                f.set_exception(ConnectionLostError(exc))

        logger.log(logging.INFO, 'Redis connection lost')

        # Call connection_lost callback
        if self._connection_lost_callback:
            self._connection_lost_callback()

    # Request state

    @property
    def in_blocking_call(self):
        """ True when waiting for answer to blocking command. """
        return any(c.is_blocking for c in self._pipelined_calls)

    @property
    def in_pubsub(self):
        """ True when the protocol is in pubsub mode. """
        return self._in_pubsub

    @property
    def in_transaction(self):
        """ True when we're inside a transaction. """
        return self._in_transaction

    @property
    def in_use(self):
        """ True when this protocol is in use. """
        return self.in_blocking_call or self.in_pubsub or self.in_transaction

    @property
    def is_connected(self):
        """ True when the underlying transport is connected. """
        return self._is_connected

    # Handle replies

    @asyncio.coroutine
    def _reader_coroutine(self):
        """
        Coroutine which reads input from the stream reader and processes it.
        """
        while True:
            try:
                yield From(self._handle_item(self._push_answer))
            except ConnectionLostError:
                return

    @asyncio.coroutine
    def _handle_item(self, cb):
        c = yield From(self._reader.readexactly(1))
        if c:
            yield From(self._line_received_handlers[c](cb))
        else:
            raise ConnectionLostError(None)

    @asyncio.coroutine
    def _handle_status_reply(self, cb):
        line = (yield From(self._reader.readline())).rstrip(b'\r\n')
        cb(line)

    @asyncio.coroutine
    def _handle_int_reply(self, cb):
        line = (yield From(self._reader.readline())).rstrip(b'\r\n')
        cb(int(line))

    @asyncio.coroutine
    def _handle_error_reply(self, cb):
        line = (yield From(self._reader.readline())).rstrip(b'\r\n')
        cb(ErrorReply(line.decode('ascii')))

    @asyncio.coroutine
    def _handle_bulk_reply(self, cb):
        length = int((yield From(self._reader.readline())).rstrip(b'\r\n'))
        if length == -1:
            # None bulk reply
            cb(None)
        else:
            # Read data
            data = yield From(self._reader.readexactly(length))
            cb(data)

            # Ignore trailing newline.
            remaining = yield From(self._reader.readline())
            assert remaining.rstrip(b'\r\n') == b''

    @asyncio.coroutine
    def _handle_multi_bulk_reply(self, cb):
                # NOTE: the reason for passing the callback `cb` in here is
                #       mainly because we want to return the result object
                #       especially in this case before the input is read
                #       completely. This allows a streaming API.
        count = int((yield From(self._reader.readline())).rstrip(b'\r\n'))

        # Handle multi-bulk none.
        # (Used when a transaction exec fails.)
        if count == -1:
            cb(None)
            return

        reply = MultiBulkReply(self, count, loop=self._loop)

        # Return the empty queue immediately as an answer.
        if self._in_pubsub:
            asyncio.async(
                self._handle_pubsub_multibulk_reply(reply), loop=self._loop)
        else:
            cb(reply)

        # Wait for all multi bulk reply content.
        for i in range(count):
            yield From(self._handle_item(reply._feed_received))

    @asyncio.coroutine
    def _handle_pubsub_multibulk_reply(self, multibulk_reply):
        # Read first item of the multi bulk reply raw.
        type = yield From(multibulk_reply._read(decode=False, _one=True))
        assert type in (b'message', b'subscribe', b'unsubscribe',
                        b'pmessage', b'psubscribe', b'punsubscribe')

        if type == b'message':
            channel, value = yield From(multibulk_reply._read(count=2))
            yield From(
                self._subscription._messages_queue.put(
                    PubSubReply(channel, value)))

        elif type == b'pmessage':
            pattern, channel, value = yield From(
                multibulk_reply._read(count=3))
            yield From(self._subscription._messages_queue.put(
                PubSubReply(channel, value, pattern=pattern)))

        # We can safely ignore 'subscribe'/'unsubscribe' replies at this point,
        # they don't contain anything really useful.

    # Redis operations.

    def _send_command(self, args):
        """
        Send Redis request command.
        `args` should be a list of bytes to be written to the transport.
        """
        # Create write buffer.
        data = []

        # NOTE: First, I tried to optimize by also flushing this buffer in
        # between the looping through the args. However, I removed that as the
        # advantage was really small. Even when some commands like `hmset`
        # could accept a generator instead of a list/dict, we would need to
        # read out the whole generator in memory in order to write the number
        # of arguments first.

        # Serialize and write header (number of arguments.)
        data += [b'*', self._encode_int(len(args)), b'\r\n']

        # Write arguments.
        for arg in args:
            data += [b'$', self._encode_int(len(arg)), b'\r\n', arg, b'\r\n']

        # Flush the last part
        self.transport.write(b''.join(data))

    @asyncio.coroutine
    def _get_answer(self, answer_f, _bypass=False, call=None):
        """
        Return an answer to the pipelined query.
        (Or when we are in a transaction, return a future for the answer.)
        """
        # Wait for the answer to come in
        result = yield From(answer_f)

        if self._in_transaction and not _bypass:
            # When the connection is inside a transaction, the query will
            # be queued.
            if result != b'QUEUED':
                raise Error(
                    'Expected to receive QUEUED for query in transaction,'
                    ' received %r.' % result)

            # Return a future which will contain the result when it arrives.
            f = Future(loop=self._loop)
            self._transaction_response_queue.append((f, call))
            raise Return(f)
        else:
            if call:
                self._pipelined_calls.remove(call)
            raise Return(result)

    def _push_answer(self, answer):
        """
        Answer future at the queue.
        """
        f = self._queue.popleft()

        if isinstance(answer, Exception):
            f.set_exception(answer)
        elif f.cancelled():
            # Received an answer from Redis, for a query which `Future` got
            # already cancelled. Don't call set_result, that would raise an
            # `InvalidStateError` otherwise.
            pass
        else:
            f.set_result(answer)

    @asyncio.coroutine
    def _query(self, *args, **kwargs):
        """
        Wrapper around both _send_command and _get_answer.

        Coroutine that sends the query to the server, and returns the reply.
        (Where the reply is a simple Redis type: these are `int`,
        `StatusReply`, `bytes` or `MultiBulkReply`) When we are in a
        transaction, this coroutine will return a `Future` of the actual
        result.
        """
        _bypass = kwargs.get('_bypass', False)
        set_blocking = kwargs.get('set_blocking', False)
        if not self._is_connected:
            raise NotConnectedError

        call = PipelinedCall(args[0], set_blocking)
        self._pipelined_calls.add(call)

        # Add a new future to our answer queue.
        answer_f = Future(loop=self._loop)
        self._queue.append(answer_f)

        # Send command
        self._send_command(args)

        # Receive answer.
        result = yield From(
            self._get_answer(answer_f, _bypass=_bypass, call=call))
        raise Return(result)

    # Internal

    @_query_command
    @typedef(NativeType, return_type=StatusReply)
    def auth(self, password):
        """ Authenticate to the server """
        self.password = password
        return self._query(b'auth', self.encode_from_native(password))

    @_query_command
    @typedef(int, return_type=StatusReply)
    def select(self, db):
        """ Change the selected database for the current connection """
        self.db = db
        return self._query(b'select', self._encode_int(db))

    # Strings

    @_query_command
    @typedef(NativeType, NativeType, (int, NoneType), (int, NoneType), bool,
             bool, return_type=(StatusReply, NoneType))
    def set(self, key, value,
            expire=None, pexpire=None,
            only_if_not_exists=False, only_if_exists=False):
        """
        Set the string value of a key

        ::

            yield from protocol.set('key', 'value')
            result = yield from protocol.get('key')
            assert result == 'value'

        To set a value and its expiration, only if key not exists, do:

        ::

            yield from protocol.set(
                'key', 'value', expire=1, only_if_not_exists=True)

        This will send: ``SET key value EX 1 NX`` at the network.
        To set value and its expiration in milliseconds, but only if
            key already exists:

        ::

            yield from protocol.set(
                'key', 'value', pexpire=1000, only_if_exists=True)
        """
        params = [
            b'set',
            self.encode_from_native(key),
            self.encode_from_native(value)
        ]
        if expire is not None:
            params.extend((b'ex', self._encode_int(expire)))
        if pexpire is not None:
            params.extend((b'px', self._encode_int(pexpire)))
        if only_if_not_exists and only_if_exists:
            raise ValueError(
                "only_if_not_exists and only_if_exists cannot be true "
                "simultaniously")
        if only_if_not_exists:
            params.append(b'nx')
        if only_if_exists:
            params.append(b'xx')

        return self._query(*params)

    @_query_command
    @typedef(NativeType, int, NativeType, return_type=StatusReply)
    def setex(self, key, seconds, value):
        """ Set the string value of a key with expire """
        return self._query(
            b'setex', self.encode_from_native(key),
            self._encode_int(seconds), self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, NativeType, return_type=bool)
    def setnx(self, key, value):
        """ Set the string value of a key if it does not exist.
        Returns True if value is successfully set """
        return self._query(
            b'setnx', self.encode_from_native(key),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, return_type=(NativeType, NoneType))
    def get(self, key):
        """ Get the value of a key """
        return self._query(b'get', self.encode_from_native(key))

    @_query_command
    @typedef(ListOf(NativeType), return_type=ListReply)
    def mget(self, keys):
        """ Returns the values of all specified keys. """
        return self._query(b'mget', *map(self.encode_from_native, keys))

    @_query_command
    @typedef(NativeType, return_type=int)
    def strlen(self, key):
        """ Returns the length of the string value stored at key. An error is
        returned when key holds a non-string value.  """
        return self._query(b'strlen', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def append(self, key, value):
        """ Append a value to a key """
        return self._query(
            b'append', self.encode_from_native(key),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, NativeType, return_type=(NativeType, NoneType))
    def getset(self, key, value):
        """ Set the string value of a key and return its old value """
        return self._query(
            b'getset', self.encode_from_native(key),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, return_type=int)
    def incr(self, key):
        """ Increment the integer value of a key by one """
        return self._query(b'incr', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def incrby(self, key, increment):
        """ Increment the integer value of a key by the given amount """
        return self._query(
            b'incrby', self.encode_from_native(key),
            self._encode_int(increment))

    @_query_command
    @typedef(NativeType, return_type=int)
    def decr(self, key):
        """ Decrement the integer value of a key by one """
        return self._query(b'decr', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def decrby(self, key, increment):
        """ Decrement the integer value of a key by the given number """
        return self._query(
            b'decrby', self.encode_from_native(key),
            self._encode_int(increment))

    @_query_command
    @typedef(return_type=NativeType)
    def randomkey(self):
        """ Return a random key from the keyspace """
        return self._query(b'randomkey')

    @_query_command
    @typedef(NativeType, return_type=bool)
    def exists(self, key):
        """ Determine if a key exists """
        return self._query(b'exists', self.encode_from_native(key))

    @_query_command
    @typedef(ListOf(NativeType), return_type=int)
    def delete(self, keys):
        """ Delete a key """
        return self._query(b'del', *map(self.encode_from_native, keys))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def move(self, key, database):
        """ Move a key to another database """
        # TODO: unittest
        return self._query(
            b'move', self.encode_from_native(key), self._encode_int(database))

    @_query_command
    @typedef(NativeType, NativeType, return_type=StatusReply)
    def rename(self, key, newkey):
        """ Rename a key """
        return self._query(
            b'rename', self.encode_from_native(key),
            self.encode_from_native(newkey))

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def renamenx(self, key, newkey):
        """ Rename a key, only if the new key does not exist
        (Returns 1 if the key was successfully renamed.) """
        return self._query(
            b'renamenx', self.encode_from_native(key),
            self.encode_from_native(newkey))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def bitop_and(self, destkey, srckeys):
        """ Perform a bitwise AND operation between multiple keys. """
        return self._bitop(b'and', destkey, srckeys)

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def bitop_or(self, destkey, srckeys):
        """ Perform a bitwise OR operation between multiple keys. """
        return self._bitop(b'or', destkey, srckeys)

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def bitop_xor(self, destkey, srckeys):
        """ Perform a bitwise XOR operation between multiple keys. """
        return self._bitop(b'xor', destkey, srckeys)

    def _bitop(self, op, destkey, srckeys):
        return self._query(
            b'bitop', op, self.encode_from_native(destkey),
            *map(self.encode_from_native, srckeys))

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def bitop_not(self, destkey, key):
        """ Perform a bitwise NOT operation between multiple keys. """
        return self._query(
            b'bitop', b'not', self.encode_from_native(destkey),
            self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, int, int, return_type=int)
    def bitcount(self, key, start=0, end=-1):
        """ Count the number of set bits (population counting) in a string. """
        return self._query(
            b'bitcount', self.encode_from_native(key),
            self._encode_int(start), self._encode_int(end))

    @_query_command
    @typedef(NativeType, int, return_type=bool)
    def getbit(self, key, offset):
        """ Returns the bit value at offset in the string value stored at key
        """
        return self._query(
            b'getbit', self.encode_from_native(key),
            self._encode_int(offset))

    @_query_command
    @typedef(NativeType, int, bool, return_type=bool)
    def setbit(self, key, offset, value):
        """ Sets or clears the bit at offset in the string value stored at key
        """
        return self._query(
            b'setbit', self.encode_from_native(key), self._encode_int(offset),
            self._encode_int(int(value)))

    # Keys

    @_query_command
    @typedef(NativeType, return_type=ListReply)
    def keys(self, pattern):
        """
        Find all keys matching the given pattern.

        .. note:: Also take a look at
        :func:`~trollius_redis.RedisProtocol.scan`.
        """
        return self._query(b'keys', self.encode_from_native(pattern))

#    @_query_command
#    def dump(self, key):
#        """ Return a serialized version of the value stored at the specified
#        key. """
#        # Dump does not work yet. It shouldn't be decoded using utf-8'
#        raise NotImplementedError('Not supported.')

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def expire(self, key, seconds):
        """ Set a key's time to live in seconds """
        return self._query(
            b'expire', self.encode_from_native(key), self._encode_int(seconds))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def pexpire(self, key, milliseconds):
        """ Set a key's time to live in milliseconds """
        return self._query(
            b'pexpire', self.encode_from_native(key),
            self._encode_int(milliseconds))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def expireat(self, key, timestamp):
        """ Set the expiration for a key as a UNIX timestamp """
        return self._query(
            b'expireat', self.encode_from_native(key),
            self._encode_int(timestamp))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def pexpireat(self, key, milliseconds_timestamp):
        """ Set the expiration for a key as a UNIX timestamp specified in
        milliseconds """
        return self._query(
            b'pexpireat', self.encode_from_native(key),
            self._encode_int(milliseconds_timestamp))

    @_query_command
    @typedef(NativeType, return_type=int)
    def persist(self, key):
        """ Remove the expiration from a key """
        return self._query(b'persist', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=int)
    def ttl(self, key):
        """ Get the time to live for a key """
        return self._query(b'ttl', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=int)
    def pttl(self, key):
        """ Get the time to live for a key in milliseconds """
        return self._query(b'pttl', self.encode_from_native(key))

    # Set operations

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def sadd(self, key, members):
        """ Add one or more members to a set """
        return self._query(
            b'sadd', self.encode_from_native(key),
            *map(self.encode_from_native, members))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def srem(self, key, members):
        """ Remove one or more members from a set """
        return self._query(
            b'srem', self.encode_from_native(key),
            *map(self.encode_from_native, members))

    @_query_command
    @typedef(NativeType, return_type=NativeType)
    def spop(self, key):
        """ Removes and returns a random element
        from the set value stored at key. """
        return self._query(b'spop', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, int, return_type=SetReply)
    def srandmember(self, key, count=1):
        """ Get one or multiple random members from a set
        (Returns a list of members, even when count==1) """
        return self._query(
            b'srandmember', self.encode_from_native(key),
            self._encode_int(count))

    @_query_command
    @typedef(NativeType, NativeType, return_type=bool)
    def sismember(self, key, value):
        """ Determine if a given value is a member of a set """
        return self._query(
            b'sismember', self.encode_from_native(key),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, return_type=int)
    def scard(self, key):
        """ Get the number of members in a set """
        return self._query(b'scard', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=SetReply)
    def smembers(self, key):
        """ Get all the members in a set """
        return self._query(b'smembers', self.encode_from_native(key))

    @_query_command
    @typedef(ListOf(NativeType), return_type=SetReply)
    def sinter(self, keys):
        """ Intersect multiple sets """
        return self._query(b'sinter', *map(self.encode_from_native, keys))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def sinterstore(self, destination, keys):
        """ Intersect multiple sets and store the resulting set in a key """
        return self._query(
            b'sinterstore', self.encode_from_native(destination),
            *map(self.encode_from_native, keys))

    @_query_command
    @typedef(ListOf(NativeType), return_type=SetReply)
    def sdiff(self, keys):
        """ Subtract multiple sets """
        return self._query(b'sdiff', *map(self.encode_from_native, keys))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def sdiffstore(self, destination, keys):
        """ Subtract multiple sets and store the resulting set in a key """
        return self._query(
            b'sdiffstore', self.encode_from_native(destination),
            *map(self.encode_from_native, keys))

    @_query_command
    @typedef(ListOf(NativeType), return_type=SetReply)
    def sunion(self, keys):
        """ Add multiple sets """
        return self._query(b'sunion', *map(self.encode_from_native, keys))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def sunionstore(self, destination, keys):
        """ Add multiple sets and store the resulting set in a key """
        return self._query(
            b'sunionstore', self.encode_from_native(destination),
            *map(self.encode_from_native, keys))

    @_query_command
    @typedef(NativeType, NativeType, NativeType, return_type=int)
    def smove(self, source, destination, value):
        """ Move a member from one set to another """
        return self._query(
            b'smove', self.encode_from_native(source),
            self.encode_from_native(destination),
            self.encode_from_native(value))

    # List operations

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def lpush(self, key, values):
        """ Prepend one or multiple values to a list """
        return self._query(
            b'lpush', self.encode_from_native(key),
            *map(self.encode_from_native, values))

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def lpushx(self, key, value):
        """ Prepend a value to a list, only if the list exists """
        return self._query(
            b'lpushx', self.encode_from_native(key),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def rpush(self, key, values):
        """ Append one or multiple values to a list """
        return self._query(
            b'rpush', self.encode_from_native(key),
            *map(self.encode_from_native, values))

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def rpushx(self, key, value):
        """ Append a value to a list, only if the list exists """
        return self._query(
            b'rpushx', self.encode_from_native(key),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, return_type=int)
    def llen(self, key):
        """ Returns the length of the list stored at key. """
        return self._query(b'llen', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, int, return_type=int)
    def lrem(self, key, count=0, value=''):
        """ Remove elements from a list """
        return self._query(
            b'lrem', self.encode_from_native(key),
            self._encode_int(count), self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, int, int, return_type=ListReply)
    def lrange(self, key, start=0, stop=-1):
        """ Get a range of elements from a list. """
        return self._query(
            b'lrange', self.encode_from_native(key),
            self._encode_int(start), self._encode_int(stop))

    @_query_command
    @typedef(NativeType, int, int, return_type=StatusReply)
    def ltrim(self, key, start=0, stop=-1):
        """ Trim a list to the specified range """
        return self._query(
            b'ltrim', self.encode_from_native(key),
            self._encode_int(start), self._encode_int(stop))

    @_query_command
    @typedef(NativeType, return_type=(NativeType, NoneType))
    def lpop(self, key):
        """ Remove and get the first element in a list """
        return self._query(b'lpop', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=(NativeType, NoneType))
    def rpop(self, key):
        """ Remove and get the last element in a list """
        return self._query(b'rpop', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, NativeType, return_type=(NativeType, NoneType))
    def rpoplpush(self, source, destination):
        """ Remove the last element in a list, append it to another list and
        return it """
        return self._query(
            b'rpoplpush', self.encode_from_native(source),
            self.encode_from_native(destination))

    @_query_command
    @typedef(NativeType, int, return_type=(NativeType, NoneType))
    def lindex(self, key, index):
        """ Get an element from a list by its index """
        return self._query(
            b'lindex', self.encode_from_native(key), self._encode_int(index))

    @_query_command
    @typedef(ListOf(NativeType), int, return_type=BlockingPopReply)
    def blpop(self, keys, timeout=0):
        """ Remove and get the first element in a list, or block until one is
        available.
        This will raise :class:`~trollius_redis.exceptions.TimeoutError` when
        the timeout was exceeded and Redis returns `None`. """
        return self._blocking_pop(b'blpop', keys, timeout=timeout)

    @_query_command
    @typedef(ListOf(NativeType), int, return_type=BlockingPopReply)
    def brpop(self, keys, timeout=0):
        """ Remove and get the last element in a list, or block until one is
        available.
        This will raise :class:`~trollius_redis.exceptions.TimeoutError` when
        the timeout was exceeded and Redis returns `None`. """
        return self._blocking_pop(b'brpop', keys, timeout=timeout)

    def _blocking_pop(self, command, keys, timeout=0):
        return self._query(
            command, *(
                [self.encode_from_native(k) for k in keys] +
                [self._encode_int(timeout)]), set_blocking=True)

    @_command
    @asyncio.coroutine
    @typedef(NativeType, NativeType, int, return_type=NativeType)
    def brpoplpush(self, source, destination, timeout=0):
        """ Pop a value from a list, push it to another list and return it;
        or block until one is available """
        result = yield From(self._query(
            b'brpoplpush', self.encode_from_native(source),
            self.encode_from_native(destination), self._encode_int(timeout),
            set_blocking=True))

        if result is None:
            raise TimeoutError('Timeout in brpoplpush')
        else:
            assert isinstance(result, bytes)
            raise Return(self.decode_to_native(result))

    @_query_command
    @typedef(NativeType, int, NativeType, return_type=StatusReply)
    def lset(self, key, index, value):
        """ Set the value of an element in a list by its index. """
        return self._query(
            b'lset', self.encode_from_native(key), self._encode_int(index),
            self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, NativeType, NativeType, return_type=int)
    def linsert(self, key, pivot, value, before=False):
        """ Insert an element before or after another element in a list """
        return self._query(
            b'linsert', self.encode_from_native(key),
            (b'BEFORE' if before else b'AFTER'),
            self.encode_from_native(pivot), self.encode_from_native(value))

    # Sorted Sets

    @_query_command
    @typedef(NativeType, dict, return_type=int)
    def zadd(self, key, values):
        """
        Add one or more members to a sorted set, or update its score if it
        already exists

        ::

            yield protocol.zadd('myzset', { 'key': 4, 'key2': 5 })
        """
        data = []
        for k, score in values.items():
            assert isinstance(k, self.native_type), \
                "Key in dictionary is {0}, not {1}".format(
                    type(k), self.native_type)
            assert isinstance(score, six.integer_types + (float,)), \
                "Value in dictionary is not numeric, it is {0}".format(
                    type(score))

            data.append(self._encode_float(score))
            data.append(self.encode_from_native(k))

        return self._query(b'zadd', self.encode_from_native(key), *data)

    @_query_command
    @typedef(NativeType, int, int, return_type=ZRangeReply)
    def zrange(self, key, start=0, stop=-1):
        """
        Return a range of members in a sorted set, by index.

        You can do the following to receive the slice of the sorted set as a
        python dict (mapping the keys to their scores):

        ::

            result = yield protocol.zrange('myzset', start=10, stop=20)
            my_dict = yield result.asdict()

        or the following to retrieve it as a list of keys:

        ::

            result = yield protocol.zrange('myzset', start=10, stop=20)
            my_dict = yield result.aslist()
        """
        return self._query(
            b'zrange', self.encode_from_native(key), self._encode_int(start),
            self._encode_int(stop), b'withscores')

    @_query_command
    @typedef(NativeType, int, int, return_type=ZRangeReply)
    def zrevrange(self, key, start=0, stop=-1):
        """
        Return a range of members in a reversed sorted set, by index.

        You can do the following to receive the slice of the sorted set as a
        python dict (mapping the keys to their scores):

        ::

            my_dict = yield protocol.zrevrange_asdict(
                'myzset', start=10, stop=20)

        or the following to retrieve it as a list of keys:

        ::

            zrange_reply = yield protocol.zrevrange(
                'myzset', start=10, stop=20)
            my_dict = yield zrange_reply.aslist()

        """
        return self._query(
            b'zrevrange', self.encode_from_native(key),
            self._encode_int(start), self._encode_int(stop), b'withscores')

    @_query_command
    @typedef(NativeType, ZScoreBoundary, ZScoreBoundary, return_type=ZRangeReply)
    def zrangebyscore(
            self, key,
            min=ZScoreBoundary.MIN_VALUE,
            max=ZScoreBoundary.MAX_VALUE):
        """ Return a range of members in a sorted set, by score """
        return self._query(
            b'zrangebyscore', self.encode_from_native(key),
            self._encode_zscore_boundary(min),
            self._encode_zscore_boundary(max), b'withscores')

    @_query_command
    @typedef(NativeType, ZScoreBoundary, ZScoreBoundary, return_type=ZRangeReply)
    def zrevrangebyscore(
            self, key,
            max=ZScoreBoundary.MAX_VALUE,
            min=ZScoreBoundary.MIN_VALUE):
        """ Return a range of members in a sorted set, by score, with scores
        ordered from high to low """
        return self._query(
            b'zrevrangebyscore', self.encode_from_native(key),
            self._encode_zscore_boundary(max),
            self._encode_zscore_boundary(min), b'withscores')

    @_query_command
    @typedef(NativeType, ZScoreBoundary, ZScoreBoundary, return_type=int)
    def zremrangebyscore(
            self, key,
            min=ZScoreBoundary.MIN_VALUE,
            max=ZScoreBoundary.MAX_VALUE):
        """ Remove all members in a sorted set within the given scores """
        return self._query(
            b'zremrangebyscore', self.encode_from_native(key),
            self._encode_zscore_boundary(min),
            self._encode_zscore_boundary(max))

    @_query_command
    @typedef(NativeType, int, int, return_type=int)
    def zremrangebyrank(self, key, min=0, max=-1):
        """ Remove all members in a sorted set within the given indexes """
        return self._query(
            b'zremrangebyrank', self.encode_from_native(key),
            self._encode_int(min), self._encode_int(max))

    @_query_command
    @typedef(NativeType, ZScoreBoundary, ZScoreBoundary, return_type=int)
    def zcount(self, key, min, max):
        """ Count the members in a sorted set with scores within the given
        values """
        return self._query(
            b'zcount', self.encode_from_native(key),
            self._encode_zscore_boundary(min),
            self._encode_zscore_boundary(max))

    @_query_command
    @typedef(NativeType, NativeType, return_type=(float, NoneType))
    def zscore(self, key, member):
        """ Get the score associated with the given member in a sorted set """
        return self._query(
            b'zscore', self.encode_from_native(key),
            self.encode_from_native(member))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), (NoneType,ListOf(float)), return_type=int)
    def zunionstore(self, destination, keys, weights=None,
                    aggregate=ZAggregate.SUM):
        """ Add multiple sorted sets and store the resulting sorted set in a
        new key """
        return self._zstore(
            b'zunionstore', destination, keys, weights, aggregate)

    @_query_command
    @typedef(NativeType, ListOf(NativeType), (NoneType,ListOf(float)), return_type=int)
    def zinterstore(self, destination, keys, weights=None,
                    aggregate=ZAggregate.SUM):
        """ Intersect multiple sorted sets and store the resulting sorted set
        in a new key """
        return self._zstore(
            b'zinterstore', destination, keys, weights, aggregate)

    def _zstore(self, command, destination, keys, weights, aggregate):
        """ Common part for zunionstore and zinterstore. """
        numkeys = len(keys)
        if weights is None:
            weights = [1] * numkeys

        return self._query(
            *([command, self.encode_from_native(
                destination), self._encode_int(numkeys)] +
              list(map(self.encode_from_native, keys)) +
              [b'weights'] +
              list(map(self._encode_float, weights)) +
              [b'aggregate'] +
              [{
                  ZAggregate.SUM: b'SUM',
                  ZAggregate.MIN: b'MIN',
                  ZAggregate.MAX: b'MAX'}[aggregate]
               ]))

    @_query_command
    @typedef(NativeType, return_type=int)
    def zcard(self, key):
        """ Get the number of members in a sorted set """
        return self._query(b'zcard', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, NativeType, return_type=(int, NoneType))
    def zrank(self, key, member):
        """ Determine the index of a member in a sorted set """
        return self._query(
            b'zrank', self.encode_from_native(key),
            self.encode_from_native(member))

    @_query_command
    @typedef(NativeType, NativeType, return_type=(int, NoneType))
    def zrevrank(self, key, member):
        """ Determine the index of a member in a sorted set, with scores
        ordered from high to low """
        return self._query(
            b'zrevrank', self.encode_from_native(key),
            self.encode_from_native(member))

    @_query_command
    @typedef(NativeType, float, NativeType, return_type=float)
    def zincrby(self, key, increment, member):
        """ Increment the score of a member in a sorted set """
        return self._query(
            b'zincrby', self.encode_from_native(key),
            self._encode_float(increment), self.encode_from_native(member))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def zrem(self, key, members):
        """ Remove one or more members from a sorted set """
        return self._query(
            b'zrem', self.encode_from_native(key),
            *map(self.encode_from_native, members))

    # Hashes

    @_query_command
    @typedef(NativeType, NativeType, NativeType, return_type=int)
    def hset(self, key, field, value):
        """ Set the string value of a hash field """
        return self._query(
            b'hset', self.encode_from_native(key),
            self.encode_from_native(field), self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, dict, return_type=StatusReply)
    def hmset(self, key, values):
        """ Set multiple hash fields to multiple values """
        data = []
        for k, v in values.items():
            assert isinstance(k, self.native_type), \
                "{0} is not {1}".format(type(k), self.native_type)
            assert isinstance(v, self.native_type), \
                "{0} is not {1}".format(type(v), self.native_type)

            data.append(self.encode_from_native(k))
            data.append(self.encode_from_native(v))

        return self._query(b'hmset', self.encode_from_native(key), *data)

    @_query_command
    @typedef(NativeType, NativeType, NativeType, return_type=int)
    def hsetnx(self, key, field, value):
        """ Set the value of a hash field, only if the field does not exist """
        return self._query(
            b'hsetnx', self.encode_from_native(key),
            self.encode_from_native(field), self.encode_from_native(value))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=int)
    def hdel(self, key, fields):
        """ Delete one or more hash fields """
        return self._query(
            b'hdel', self.encode_from_native(key),
            *map(self.encode_from_native, fields))

    @_query_command
    @typedef(NativeType, NativeType, return_type=(NativeType, NoneType))
    def hget(self, key, field):
        """ Get the value of a hash field """
        return self._query(
            b'hget', self.encode_from_native(key),
            self.encode_from_native(field))

    @_query_command
    @typedef(NativeType, NativeType, return_type=bool)
    def hexists(self, key, field):
        """ Returns if field is an existing field in the hash stored at key."""
        return self._query(
            b'hexists', self.encode_from_native(key),
            self.encode_from_native(field))

    @_query_command
    @typedef(NativeType, return_type=SetReply)
    def hkeys(self, key):
        """ Get all the keys in a hash. (Returns a set) """
        return self._query(b'hkeys', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=ListReply)
    def hvals(self, key):
        """ Get all the values in a hash. (Returns a list) """
        return self._query(b'hvals', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=int)
    def hlen(self, key):
        """Returns the number of fields contained in the hash stored at key."""
        return self._query(b'hlen', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, return_type=DictReply)
    def hgetall(self, key):
        """ Get the value of a hash field """
        return self._query(b'hgetall', self.encode_from_native(key))

    @_query_command
    @typedef(NativeType, ListOf(NativeType), return_type=ListReply)
    def hmget(self, key, fields):
        """ Get the values of all the given hash fields """
        return self._query(
            b'hmget', self.encode_from_native(key),
            *map(self.encode_from_native, fields))

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def hincrby(self, key, field, increment):
        """ Increment the integer value of a hash field by the given number
        Returns: the value at field after the increment operation. """
        assert isinstance(increment, int)
        return self._query(
            b'hincrby', self.encode_from_native(key),
            self.encode_from_native(field), self._encode_int(increment))

    @_query_command
    @typedef(NativeType, NativeType, (int,float), return_type=float)
    def hincrbyfloat(self, key, field, increment):
        """ Increment the float value of a hash field by the given amount
        Returns: the value at field after the increment operation. """
        return self._query(
            b'hincrbyfloat', self.encode_from_native(key),
            self.encode_from_native(field), self._encode_float(increment))

    # Pubsub
    # (subscribe, unsubscribe, etc... should be called through the
    # Subscription class.)

    @_command
    @typedef(return_type=u'Subscription')
    def start_subscribe(self, *a):
        """
        Start a pubsub listener.

        ::

            # Create subscription
            subscription = yield from protocol.start_subscribe()
            yield from subscription.subscribe(['key'])
            yield from subscription.psubscribe(['pattern*'])

            while True:
                result = yield from subscription.next_published()
                print(result)

        :returns: :class:`~trollius_redis.Subscription`
        """
        # (Make coroutine. @asyncio.coroutine breaks documentation. It uses
        # @functools.wraps to make a generator for this function. But _command
        # will no longer be able to read the signature.)
        if False:
            yield

        if self.in_use:
            raise Error(
                'Cannot start pubsub listener when a protocol is in use.')

        subscription = Subscription(self)

        self._in_pubsub = True
        self._subscription = subscription
        raise Return(subscription)

    @_command
    @typedef(ListOf(NativeType), return_type=NoneType)
    def _subscribe(self, channels):
        """ Listen for messages published to the given channels """
        self._pubsub_channels |= set(channels)
        return self._pubsub_method('subscribe', channels)

    @_command
    @typedef(ListOf(NativeType), return_type=NoneType)
    def _unsubscribe(self, channels):
        """ Stop listening for messages posted to the given channels """
        self._pubsub_channels -= set(channels)
        return self._pubsub_method('unsubscribe', channels)

    @_command
    @typedef(ListOf(NativeType), return_type=NoneType)
    def _psubscribe(self, patterns):
        """Listen for messages published to channels matching the given
        patterns"""
        self._pubsub_patterns |= set(patterns)
        return self._pubsub_method('psubscribe', patterns)

    @_command
    @typedef(ListOf(NativeType), return_type=NoneType)
    def _punsubscribe(self, patterns):
        """ Stop listening for messages posted to channels matching the
        given patterns """
        self._pubsub_patterns -= set(patterns)
        return self._pubsub_method('punsubscribe', patterns)

    @asyncio.coroutine
    def _pubsub_method(self, method, params):
        if not self._in_pubsub:
            raise Error(
                'Cannot call pubsub methods without calling start_subscribe')

        # Send
        self._send_command(
            [method.encode('ascii')] + list(
                map(self.encode_from_native, params)))

        # Note that we can't use `self._query` here. The reason is that one
        # subscribe/unsubscribe command returns a separate answer for every
        # parameter. It doesn't fit in the same model of all the other queries
        # where one query puts a Future on the queue that is replied with the
        # incoming answer.
        # Redis returns something like [ 'subscribe', 'channel_name', 1] for
        # each parameter, but we can safely ignore those replies that.

    @_query_command
    @typedef(NativeType, NativeType, return_type=int)
    def publish(self, channel, message):
        """ Post a message to a channel
        (Returns the number of clients that received this message.) """
        return self._query(
            b'publish', self.encode_from_native(channel),
            self.encode_from_native(message))

    @_query_command
    @typedef((NativeType, NoneType), return_type=ListReply)
    def pubsub_channels(self, pattern=None):
        """
        Lists the currently active channels. An active channel is a Pub/Sub
        channel with one ore more subscribers (not including clients subscribed
        to patterns).
        """
        return self._query(
            b'pubsub', b'channels',
            (self.encode_from_native(pattern) if pattern else b'*'))

    @_query_command
    @typedef(ListOf(NativeType), return_type=DictReply)
    def pubsub_numsub(self, channels):
        """Returns the number of subscribers (not counting clients subscribed
        to patterns) for the specified channels.  """
        return self._query(
            b'pubsub', b'numsub',
            *[self.encode_from_native(c) for c in channels])

    @_query_command
    @typedef(return_type=int)
    def pubsub_numpat(self):
        """ Returns the number of subscriptions to patterns (that are performed
        using the PSUBSCRIBE command). Note that this is not just the count of
        clients subscribed to patterns but the total number of patterns all the
        clients are subscribed to. """
        return self._query(b'pubsub', b'numpat')

    # Server

    @_query_command
    @typedef(return_type=StatusReply)
    def ping(self):
        """ Ping the server (Returns PONG) """
        return self._query(b'ping')

    @_query_command
    @typedef(NativeType, return_type=NativeType)
    def echo(self, string):
        """ Echo the given string """
        return self._query(b'echo', self.encode_from_native(string))

    @_query_command
    @typedef(return_type=StatusReply)
    def save(self):
        """ Synchronously save the dataset to disk """
        return self._query(b'save')

    @_query_command
    @typedef(return_type=StatusReply)
    def bgsave(self):
        """ Asynchronously save the dataset to disk """
        return self._query(b'bgsave')

    @_query_command
    @typedef(return_type=StatusReply)
    def bgrewriteaof(self):
        """ Asynchronously rewrite the append-only file """
        return self._query(b'bgrewriteaof')

    @_query_command
    @typedef(return_type=int)
    def lastsave(self):
        """ Get the UNIX time stamp of the last successful save to disk """
        return self._query(b'lastsave')

    @_query_command
    @typedef(return_type=int)
    def dbsize(self):
        """ Return the number of keys in the currently-selected database. """
        return self._query(b'dbsize')

    @_query_command
    @typedef(return_type=StatusReply)
    def flushall(self):
        """ Remove all keys from all databases """
        return self._query(b'flushall')

    @_query_command
    @typedef(return_type=StatusReply)
    def flushdb(self):
        """ Delete all the keys of the currently selected DB. This
        command never fails. """
        return self._query(b'flushdb')

#    @_query_command
#    def object(self, subcommand, args):
#        """ Inspect the internals of Redis objects """
#        raise NotImplementedError

    @_query_command
    @typedef(NativeType, return_type=StatusReply)
    def type(self, key):
        """ Determine the type stored at key """
        return self._query(b'type', self.encode_from_native(key))

    @_query_command
    @typedef(six.text_type, six.text_type, return_type=StatusReply)
    def config_set(self, parameter, value):
        """ Set a configuration parameter to the given value """
        return self._query(
            b'config', b'set', self.encode_from_native(parameter),
            self.encode_from_native(value))

    @_query_command
    @typedef(six.text_type, return_type=ConfigPairReply)
    def config_get(self, parameter):
        """ Get the value of a configuration parameter """
        return self._query(
            b'config', b'get', self.encode_from_native(parameter))

    @_query_command
    @typedef(return_type=StatusReply)
    def config_rewrite(self):
        """ Rewrite the configuration file with the in memory configuration """
        return self._query(b'config', b'rewrite')

    @_query_command
    @typedef(return_type=StatusReply)
    def config_resetstat(self):
        """ Reset the stats returned by INFO """
        return self._query(b'config', b'resetstat')

    @_query_command
    @typedef((NativeType, NoneType), return_type=InfoReply)
    def info(self, section=None):
        """ Get information and statistics about the server """
        if section is None:
            return self._query(b'info')
        else:
            return self._query(b'info', self.encode_from_native(section))

    @_query_command
    @typedef(return_type=StatusReply)
    def shutdown(self, save=False):
        """ Synchronously save the dataset to disk and then shut down the
        server """
        return self._query(b'shutdown', (b'save' if save else b'nosave'))

    @_query_command
    @typedef(return_type=NativeType)
    def client_getname(self):
        """ Get the current connection name """
        return self._query(b'client', b'getname')

    @_query_command
    @typedef(return_type=StatusReply)
    def client_setname(self, name):
        """ Set the current connection name """
        return self._query(
            b'client', b'setname', self.encode_from_native(name))

    @_query_command
    @typedef(return_type=ClientListReply)
    def client_list(self):
        """ Get the list of client connections """
        return self._query(b'client', b'list')

    @_query_command
    @typedef(six.text_type, return_type=StatusReply)
    def client_kill(self, address):
        """
        Kill the connection of a client
        `address` should be an "ip:port" string.
        """
        return self._query(b'client', b'kill', address.encode('utf-8'))

    # LUA scripting

    @_command
    @typedef(six.text_type, return_type=u'Script')
    def register_script(self, script):
        """
        Register a LUA script.

        ::

            script = yield from protocol.register_script(lua_code)
            result = yield from script.run(keys=[...], args=[...])
        """
        # The register_script APi was made compatible with the redis.py
        # library: https://github.com/andymccurdy/redis-py
        sha = yield From(self.script_load(script))
        raise Return(Script(sha, script, lambda: self.evalsha))

    @_query_command
    @typedef(ListOf(six.text_type), return_type=ListOf(bool))
    def script_exists(self, shas):
        """ Check existence of scripts in the script cache. """
        return self._query(
            b'script', b'exists', *[sha.encode('ascii') for sha in shas])

    @_query_command
    @typedef(return_type=StatusReply)
    def script_flush(self):
        """ Remove all the scripts from the script cache. """
        return self._query(b'script', b'flush')

    @_query_command
    @typedef(return_type=StatusReply)
    def script_kill(self):
        """
        Kill the script currently in execution.  This raises
        :class:`~trollius_redis.exceptions.NoRunningScriptError` when there are
        no scripts running.
        """
        try:
            result = yield From(self._query(b'script', b'kill'))
        except ErrorReply as e:
            if 'NOTBUSY' in e.args[0]:
                raise NoRunningScriptError
            else:
                raise
        else:
            raise Return(result)

    @_query_command
    @typedef(six.text_type, (ListOf(NativeType), NoneType), (ListOf(NativeType), NoneType), return_type=EvalScriptReply)
    def evalsha(self, sha, keys=None, args=None):
        """
        Evaluates a script cached on the server side by its SHA1 digest.
        Scripts are cached on the server side using the SCRIPT LOAD command.

        The return type/value depends on the script.

        This will raise a :class:`~trollius_redis.exceptions.ScriptKilledError`
        exception if the script was killed.
        """
        if not keys:
            keys = []
        if not args:
            args = []

        try:
            result = yield From(self._query(
                b'evalsha', sha.encode('ascii'),
                self._encode_int(len(keys)),
                *map(self.encode_from_native, keys + args)))

            raise Return(result)
        except ErrorReply:
            raise ScriptKilledError

    @_query_command
    @typedef(six.text_type, return_type=six.text_type)
    def script_load(self, script):
        """ Load script, returns sha1 """
        return self._query(b'script', b'load', script.encode('ascii'))

    # Scanning

    @_command
    @typedef(NativeType, return_type=Cursor)
    def scan(self, match='*'):
        """
        Walk through the keys space. You can either fetch the items one by one
        or in bulk.

        ::

            cursor = yield from protocol.scan(match='*')
            while True:
                item = yield from cursor.fetchone()
                if item is None:
                    break
                else:
                    print(item)

        ::

            cursor = yield from protocol.scan(match='*')
            items = yield from cursor.fetchall()

        Also see: :func:`~trollius_redis.RedisProtocol.sscan`,
        :func:`~trollius_redis.RedisProtocol.hscan` and
        :func:`~trollius_redis.RedisProtocol.zscan`

        Redis reference: http://redis.io/commands/scan
        """
        if False:
            yield

        def scanfunc(cursor):
            return self._scan(cursor, match)

        raise Return(Cursor(name='scan(match=%r)' % match, scanfunc=scanfunc))

    @_query_command
    @typedef(int, NativeType, return_type=_ScanPart)
    def _scan(self, cursor, match):
        return self._query(
            b'scan', self._encode_int(cursor),
            b'match', self.encode_from_native(match))

    @_command
    @typedef(NativeType, NativeType, return_type=SetCursor)
    def sscan(self, key, match=u'*'):
        """
        Incrementally iterate set elements

        Also see: :func:`~trollius_redis.RedisProtocol.scan`
        """
        if False:
            yield
        name = u'sscan(key=%r match=%r)' % (key, match)

        @typedef(NativeType, return_type=Cursor)
        def scan(cursor):
            return self._do_scan(b'sscan', key, cursor, match)

        raise Return(SetCursor(name=name, scanfunc=scan))

    @_command
    @typedef(NativeType, NativeType, return_type=DictCursor)
    def hscan(self, key, match=u'*'):
        """
        Incrementally iterate hash fields and associated values
        Also see: :func:`~trollius_redis.RedisProtocol.scan`
        """
        if False:
            yield
        name = u'hscan(key=%r match=%r)' % (key, match)

        @typedef(NativeType, return_type=Cursor)
        def scan(cursor):
            return self._do_scan(b'hscan', key, cursor, match)

        raise Return(DictCursor(name=name, scanfunc=scan))

    @_command
    @typedef(NativeType, NativeType, return_type=DictCursor)
    def zscan(self, key, match=u'*'):
        """
        Incrementally iterate sorted sets elements and associated scores
        Also see: :func:`~trollius_redis.RedisProtocol.scan`
        """
        if False:
            yield
        name = u'zscan(key=%r match=%r)' % (key, match)

        @typedef(NativeType, return_type=Cursor)
        def scan(cursor):
            return self._do_scan(b'zscan', key, cursor, match)

        raise Return(ZCursor(name=name, scanfunc=scan))

    @_query_command
    @typedef(six.binary_type, NativeType, int, NativeType, return_type=_ScanPart)
    def _do_scan(self, verb, key, cursor, match):
        return self._query(verb, self.encode_from_native(key),
                           self._encode_int(cursor),
                           b'match', self.encode_from_native(match))

    # Transaction

    @_command
    @asyncio.coroutine
    @typedef(ListOf(NativeType), return_type=NoneType)
    def watch(self, keys):
        """
        Watch keys (pulled in from asyncio-redis
            ac9467ef381523da1aeee293fac71e443b3c018f)

        ::

            # Watch keys for concurrent updates
            yield From(protocol.watch([u'key', u'other_key']))

            value = yield From(protocol.get(u'key'))
            another_value = yield From(protocol.get(u'another_key'))

            transaction = yield From(protocol.multi())

            f1 = yield From(transaction.set(u'key', another_value))
            f2 = yield From(transaction.set(u'another_key', value))

            # Commit transaction
            yield From(transaction.exec())

            # Retrieve results
            yield From(f1)
            yield From(f2)

        """
        result = yield From(self._query(
            b'watch', *map(self.encode_from_native, keys)))
        assert result == b'OK'

    @_command
    @asyncio.coroutine
    @typedef((ListOf(NativeType), NoneType), return_type=u'Transaction')
    def multi(self, watch=None):
        """
        Start of transaction.

        ::

            transaction = yield from protocol.multi()

            # Run commands in transaction
            f1 = yield from transaction.set(u'key', u'value')
            f2 = yield from transaction.set(u'another_key', u'another_value')

            # Commit transaction
            yield from transaction.execute()

            # Retrieve results (you can also use asyncio.tasks.gather)
            result1 = yield from f1
            result2 = yield from f2

        :returns: A :class:`trollius_redis.Transaction` instance.
        """
        if (self._in_transaction):
            raise Error('Multi calls can not be nested.')

        # Call watch
        if watch is not None:
            yield From(self.watch(watch))

        # Call multi
        result = yield From(self._query(b'multi'))
        assert result == b'OK'

        self._in_transaction = True
        self._transaction_response_queue = deque()

        # Create transaction object.
        t = Transaction(self)
        self._transaction = t
        raise Return(t)

    @asyncio.coroutine
    def _exec(self):
        """
        Execute all commands issued after MULTI
        """
        if not self._in_transaction:
            raise Error('Not in transaction')

        futures_and_postprocessors = self._transaction_response_queue
        self._transaction_response_queue = None

        # Get transaction answers.
        multi_bulk_reply = yield From(self._query(b'exec', _bypass=True))

        if multi_bulk_reply is None:
            # We get None when a transaction failed.
            self._transaction_response_queue = deque()
            self._in_transaction = False
            self._transaction = None
            raise TransactionError('Transaction failed.')
        else:
            assert isinstance(multi_bulk_reply, MultiBulkReply)

        for f in multi_bulk_reply.iter_raw():
            answer = yield From(f)
            f2, call = futures_and_postprocessors.popleft()

            if isinstance(answer, Exception):
                f2.set_exception(answer)
            else:
                if call:
                    self._pipelined_calls.remove(call)

                f2.set_result(answer)

        self._transaction_response_queue = deque()
        self._in_transaction = False
        self._transaction = None

    @asyncio.coroutine
    def _discard(self):
        """
        Discard all commands issued after MULTI
        """
        if not self._in_transaction:
            raise Error('Not in transaction')

        self._transaction_response_queue = deque()
        self._in_transaction = False
        self._transaction = None
        result = yield From(self._query(b'discard'))
        assert result == b'OK'

    @asyncio.coroutine
    def _unwatch(self):
        """
        Forget about all watched keys
        """
        if not self._in_transaction:
            raise Error('Not in transaction')

        result = yield From(self._query(b'unwatch'))
        assert result == b'OK'


class Script(object):
    """ Lua script. """
    def __init__(self, sha, code, get_evalsha_func):
        self.sha = sha
        self.code = code
        self.get_evalsha_func = get_evalsha_func

    def run(self, keys=[], args=[]):
        """
        Returns a coroutine that executes the script.

        ::

            script_reply = yield from script.run(keys=[], args=[])

            # If the LUA script returns something, retrieve the return value
            result = yield from script_reply.return_value()

        This will raise a :class:`~trollius_redis.exceptions.ScriptKilledError`
        exception if the script was killed.
        """
        return self.get_evalsha_func()(self.sha, keys, args)


class Transaction(object):
    """
    Transaction context. This is a proxy to a :class:`.RedisProtocol` instance.
    Every redis command called on this object will run inside the transaction.
    The transaction can be finished by calling either ``discard`` or ``exec``.

    More info: http://redis.io/topics/transactions
    """
    def __init__(self, protocol):
        self._protocol = protocol

    def __getattr__(self, name):
        """
        Proxy to a protocol.
        """
        # Only proxy commands.
        if name not in _all_commands:
            raise AttributeError(name)

        method = getattr(self._protocol, name)

        # Wrap the method into something that passes the transaction object as
        # first argument.
        @wraps(method)
        def wrapper(*a, **kw):
            if self._protocol._transaction != self:
                raise Error('Transaction already finished or invalid.')

            return method(self, *a, **kw)
        return wrapper

    def discard(self):
        """
        Discard all commands issued after MULTI
        """
        return self._protocol._discard()

    def execute(self):
        """
        Execute transaction.

        This can raise a :class:`~trollius_redis.exceptions.TransactionError`
        when the transaction fails.
        """
        return self._protocol._exec()

    def unwatch(self):  # XXX: test
        """
        Forget about all watched keys
        """
        return self._protocol._unwatch()


class Subscription(object):
    """
    Pubsub subscription
    """
    def __init__(self, protocol):
        self.protocol = protocol
        self._messages_queue = Queue(loop=protocol._loop)  # Pubsub queue

    @wraps(RedisProtocol._subscribe)
    def subscribe(self, channels):
        return self.protocol._subscribe(self, channels)

    @wraps(RedisProtocol._unsubscribe)
    def unsubscribe(self, channels):
        return self.protocol._unsubscribe(self, channels)

    @wraps(RedisProtocol._psubscribe)
    def psubscribe(self, patterns):
        return self.protocol._psubscribe(self, patterns)

    @wraps(RedisProtocol._punsubscribe)
    def punsubscribe(self, patterns):
        return self.protocol._punsubscribe(self, patterns)

    @asyncio.coroutine
    def next_published(self):
        """
        Coroutine which waits for next pubsub message to be received and
        returns it.

        :returns: instance of :class:`PubSubReply
        <trollius_redis.replies.PubSubReply>`
        """
        r = yield From(self._messages_queue.get())
        raise Return(r)


class HiRedisProtocol(six.with_metaclass(_RedisProtocolMeta, RedisProtocol)):
    """
    Protocol implementation that uses the `hiredis` library for parsing the
    incoming data. This will be faster in many cases, but not necessarily
    always.

    It does not (yet) support streaming of multibulk replies, which means that
    you won't see the first item of a multi bulk reply, before the whole
    response has been parsed.
    """

    def __init__(self, password=None, db=0, encoder=None,
                 connection_lost_callback=None, enable_typechecking=True,
                 loop=None):
        super(HiRedisProtocol, self).__init__(
            password=password,
            db=db,
            encoder=encoder,
            connection_lost_callback=connection_lost_callback,
            enable_typechecking=enable_typechecking,
            loop=loop)
        self._hiredis = None
        assert hiredis, \
            "`hiredis` libary not available. Please don't use HiRedisProtocol."

    def connection_made(self, transport):
        super(HiRedisProtocol, self).connection_made(transport)
        self._hiredis = hiredis.Reader()

    def data_received(self, data):
        # Move received data to hiredis parser
        self._hiredis.feed(data)

        while True:
            item = self._hiredis.gets()

            if item is not False:
                self._process_hiredis_item(item, self._push_answer)
            else:
                break

    def _process_hiredis_item(self, item, cb):
        if isinstance(item, six.integer_types):
            if not isinstance(item, int):
                item = int(item)
            cb(item)
        elif isinstance(item, bytes):
            cb(item)
        elif isinstance(item, list):
            reply = MultiBulkReply(self, len(item), loop=self._loop)

            for i in item:
                self._process_hiredis_item(i, reply._feed_received)

            cb(reply)
        elif isinstance(item, hiredis.ReplyError):
            cb(ErrorReply(item.args[0]))
        elif isinstance(item, NoneType):
            cb(item)

    @asyncio.coroutine
    def _reader_coroutine(self):
        # We don't need this one.
        raise Return(None)
