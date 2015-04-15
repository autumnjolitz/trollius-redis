#!/usr/bin/env python
"""
Example of how the connection should reconnect to the server.
It's a loop that publishes 'message' in 'our-channel'.
"""
from __future__ import print_function
import trollius as asyncio
from trollius import From
import logging
import trollius_redis

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        connection = yield From(
            trollius_redis.Connection.create(host=u'localhost', port=6379))

        try:
            while True:
                yield From(asyncio.sleep(.5))

                try:
                    # Try to send message
                    yield From(connection.publish(u'our-channel', u'message'))
                except Exception as e:
                    print(u'error', repr(e))
        finally:
            connection.close()

    loop.run_until_complete(run())
