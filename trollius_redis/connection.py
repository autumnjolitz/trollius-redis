from .log import logger
from .protocol import RedisProtocol, _all_commands
import trollius as asyncio
from trollius import From, Return
import logging
from six.moves.urllib.parse import (parse_qs, urlparse)


__all__ = ('Connection', )


class Connection(object):
    """
    Wrapper around the protocol and transport which takes care of establishing
    the connection and reconnecting it.


    ::

        connection = yield from Connection.create(host='localhost', port=6379)
        result = yield from connection.set('key', 'value')
    """
    @classmethod
    @asyncio.coroutine
    def create(cls, host='localhost', port=6379, password=None, db=0,
               encoder=None, auto_reconnect=True, loop=None,
               enable_typechecking=True,
               protocol_class=RedisProtocol):
        """
        :param host: Address, either host or unix domain socket path
        :type host: str
        :param port: TCP port. If port is 0 then host assumed to be unix
                     socket path
        :type port: int
        :param password: Redis database password
        :type password: bytes
        :param db: Redis database
        :type db: int
        :param encoder: Encoder to use for encoding to or decoding
                        from redis bytes to a native type.
        :type encoder: :class:`~trollius_redis.encoders.BaseEncoder` instance.
        :param auto_reconnect: Enable auto reconnect
        :type auto_reconnect: bool
        :param loop: (optional) asyncio event loop.
        :type protocol_class: :class:`~trollius_redis.RedisProtocol`
        :param protocol_class: (optional) redis protocol implementation
        """
        assert port >= 0, "Unexpected port value: %r" % (port, )
        connection = cls()
        connection.host = host
        connection.port = port
        connection._loop = loop or asyncio.get_event_loop()
        connection._retry_interval = .5
        connection._closed = False
        connection._closing = False

        connection._auto_reconnect = auto_reconnect

        # Create protocol instance
        def connection_lost():
            if connection._auto_reconnect and not connection._closing:
                asyncio.async(connection._reconnect(), loop=connection._loop)

        # Create protocol instance
        connection.protocol = protocol_class(
            password=password, db=db, encoder=encoder,
            connection_lost_callback=connection_lost, loop=connection._loop,
            enable_typechecking=enable_typechecking)

        # Connect
        yield From(connection._reconnect())

        raise Return(connection)

    @staticmethod
    @asyncio.coroutine
    def from_uri(uri, enable_type_checking=True):
        kwargs = {}
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme.lower().encode('utf8') == b'unix':
            kwargs['host'] = parsed_uri.path
            params = parse_qs(parsed_uri.query)
            if 'db' in params:
                kwargs['db'] = int(params['db'][0], 10)
            if '@' in parsed_uri.netloc:
                password = parsed_uri.netloc[:parsed_uri.netloc.index('@')]
                if password:
                    kwargs['password'] = password
            kwargs['port'] = 0
        elif parsed_uri.scheme.lower().encode('utf8').endswith(b'redis'):
            if parsed_uri.scheme.lower().encode('utf8').endswith('ss'):
                raise NotImplementedError("SSL not supported (yet)")
            db = parsed_uri.path
            if len(db) > 1:
                try:
                    db = int(db[1:], 10)
                except ValueError:
                    raise ValueError("Illegal db specification")
                else:
                    kwargs['db'] = db
            if '@' in parsed_uri.netloc:
                password, host_port = parsed_uri.netloc.split('@', 1)
            else:
                password = None
                host_port = parsed_uri.netloc
            if ':' in host_port:
                host, port = host_port.split(':')
                kwargs['host'] = host
                kwargs['port'] = int(port, 10)
            else:
                kwargs['host'] = host_port
                kwargs['port'] = 6379
            if password:
                if password.startswith('[:'):
                    password = password[2:]
                if password.endswith(']'):
                    password = password[:-1]
                kwargs['password'] = password
        for key in kwargs:
            if isinstance(kwargs[key], unicode):
                kwargs[key] = kwargs[key].encode('utf8')
        kwargs['enable_type_checking'] = enable_type_checking
        conn = yield From(Connection.create(**kwargs))
        raise Return(conn)

    @property
    def transport(self):
        """ The transport instance that the protocol is currently using. """
        return self.protocol.transport

    def _get_retry_interval(self):
        """ Time to wait for a reconnect in seconds. """
        return self._retry_interval

    def _reset_retry_interval(self):
        """ Set the initial retry interval. """
        self._retry_interval = .5

    def _increase_retry_interval(self):
        """ When a connection failed. Increase the interval."""
        self._retry_interval = min(60, 1.5 * self._retry_interval)

    def _reconnect(self):
        """
        Set up Redis connection.
        """
        while True:
            try:
                logger.log(logging.INFO, 'Connecting to redis')
                if self.port:
                    yield From(self._loop.create_connection(
                        lambda: self.protocol, self.host, self.port))
                else:
                    yield From(self._loop.create_unix_connection(
                        lambda: self.protocol, self.host))
                self._reset_retry_interval()
                return
            except OSError:
                # Sleep and try again
                self._increase_retry_interval()
                interval = self._get_retry_interval()
                logger.log(
                    logging.INFO,
                    'Connecting to redis failed. Retrying in %i seconds' %
                    interval)
                yield From(asyncio.sleep(interval, loop=self._loop))

    def __getattr__(self, name):
        # Only proxy commands.
        if name not in _all_commands:
            raise AttributeError

        return getattr(self.protocol, name)

    def __repr__(self):
        return 'Connection(host=%r, port=%r)' % (self.host, self.port)

    def close(self):
        """
        Close the connection transport.
        """
        self._closing = True

        if self.protocol.transport:
            self.protocol.transport.close()
