import zlib
import zmq
import time
import sys
import simplejson
import logging

def main():
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # Connect to the first publicly available relay.
    subscriber.connect('tcp://relay-us-central-1.eve-emdr.com:8050')
    # Disable filtering.
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    received = 0
    while True:
        market_json = zlib.decompress(subscriber.recv())
        market_data = simplejson.loads(market_json)
        columns = market_data['columns']
        for rowset in market_data['rowsets']:
            received += len(rowset['rows'])

        sys.stdout.write("\rreceived: %d" % received)
        sys.stdout.flush()

if __name__ == '__main__':
    main()

