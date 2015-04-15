#!/usr/bin/env python
from __future__ import print_function
import trollius as asyncio
from trollius import From
import trollius_redis
import logging


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        # Create a new redis connection (this will also auto reconnect)
        connection = yield From(
            trollius_redis.Connection.create(u'localhost', 6379))

        try:
            while True:
                # Get input (always use executor for blocking calls)
                text = yield From(
                    loop.run_in_executor(None, raw_input, u'Enter message: '))

                # Publish value
                try:
                    yield From(connection.publish(u'our-channel', text))
                    print(u'Published.')
                except trollius_redis.Error as e:
                    print(u'Published failed', repr(e))

        finally:
            connection.close()

    loop.run_until_complete(run())
