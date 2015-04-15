#!/usr/bin/env python
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
        # Create a new redis connection (this will also auto reconnect)
        connection = yield From(
            trollius_redis.Connection.create(u'localhost', 6379))

        try:
            # Subscribe to a channel.
            subscriber = yield From(connection.start_subscribe())
            yield From(subscriber.subscribe([u'our-channel']))

            # Print published values in a while/true loop.
            while True:
                reply = yield From(subscriber.next_published())
                print(
                    u'Received: ', repr(reply.value), 'on channel',
                    reply.channel)

        finally:
            connection.close()

    loop.run_until_complete(run())
