#!/usr/bin/env python
"""
Simple example that sets a key, and retrieves it again.
"""
from __future__ import print_function
import trollius as asyncio
from trollius import From
from trollius_redis import RedisProtocol

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    def run():
        # Create connection
        transport, protocol = yield From(
            loop.create_connection(RedisProtocol, u'localhost', 6379))

        # Set a key
        yield From(protocol.set(u'key', 'value'))

        # Retrieve a key
        result = yield From(protocol.get(u'key'))

        # Print result
        print (u'Succeeded', result == 'value')

        transport.close()

    loop.run_until_complete(run())
