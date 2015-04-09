#!/usr/bin/env python
import trollius as asyncio
from trollius import From
import logging
import asyncio_redis

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        # Create a new redis connection (this will also auto reconnect)
        connection = yield From(
            asyncio_redis.Connection.create('localhost', 6379))

        try:
            # Subscribe to a channel.
            subscriber = yield From(connection.start_subscribe())
            yield From(subscriber.subscribe(['our-channel']))

            # Print published values in a while/true loop.
            while True:
                reply = yield From(subscriber.next_published())
                print(
                    'Received: ', repr(reply.value), 'on channel',
                    reply.channel)

        finally:
            connection.close()

    loop.run_until_complete(run())
