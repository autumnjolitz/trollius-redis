#!/usr/bin/env python
"""
Example of how an 'smembers' call gets streamed when it's a big reply, covering
multiple IP packets.
"""
from __future__ import print_function
import trollius
from trollius import From
import logging
import trollius_redis
import six
range = six.moves.range

if __name__ == '__main__':
    loop = trollius.get_event_loop()

    # Enable logging
    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(logging.INFO)

    def run():
        connection = yield From(trollius_redis.Connection.create(
            host=u'localhost', port=6379))

        # Create a set that contains a million items
        print(u'Creating big set contains a million items (Can'
              u' take about half a minute)')

        yield From(connection.delete([u'my-big-set']))

        # We will suffix all the items with a very long key, just to be sure
        # that this needs many IP packets, in order to send or receive this.
        long_string = u'abcdefghij' * 1000  # len=10k

        for prefix in range(10):
            print(u'Callidng redis sadd:', prefix, u'/10')
            yield From(
                connection.sadd(
                    u'my-big-set',
                    (u'%s-%s-%s' % (prefix, i, long_string)
                    for i in range(10 * 1000))))
        print(u'Done\n')

        # Now stream the values from the database:
        print(u'Streaming values, calling smembers')

        # The following smembers call will block until the first IP packet
        # containing the head of the multi bulk reply comes in. This will
        # contain the size of the multi bulk reply and that's enough
        # information to create a SetReply instance. Probably the first packet
        # will also contain the first X members, so we don't have to wait for
        # these anymore.
        set_reply = yield From(connection.smembers(u'my-big-set'))
        print(u'Got: ', set_reply)

        # Stream the items, this will probably wait for the next IP packets to
        # come in.
        count = 0
        for f in set_reply:
            m = yield From(f)
            count += 1
            if count % 1000 == 0:
                print(u'Received %i items' % count)

    loop.run_until_complete(run())
