#!/usr/bin/env python
"""
Benchmark how long it takes to set 10,000 keys in the database.
"""
from __future__ import print_function
import trollius as asyncio
from trollius import From
import logging
import trollius_redis
import time
from six.moves import range

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.WARNING)

    def run():
        connection = yield From(trollius_redis.Pool.create(
            host=u'localhost', port=6379, poolsize=50))

        try:
            # === Benchmark 1 ==
            print(
                u'1. How much time does it take to set 10,000 values '
                u'in Redis? (without pipelining)')
            print(u'Starting...')
            start = time.time()

            # Do 10,000 set requests
            for i in range(10 * 1000):
                # By using yield from here, we wait for the answer.
                yield From(connection.set(u'key', u'value'))

            print(u'Done. Duration=', time.time() - start)
            print()

            # === Benchmark 2 (should be at least 3x as fast) ==

            print(u'2. How much time does it take if we use asyncio.gather, '
                  u'and pipeline requests?')
            print(u'Starting...')
            start = time.time()

            # Do 10,000 set requests
            futures = [asyncio.Task(connection.set(u'key', u'value')) for x
                       in range(10*1000)]
            yield From(asyncio.gather(*futures))

            print(u'Done. Duration=', time.time() - start)

        finally:
            connection.close()

    loop.run_until_complete(run())
