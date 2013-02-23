import zlib
import zmq
import time
import sys
import simplejson
import logging
from dateutil import parser

def main():
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # Connect to the first publicly available relay.
    subscriber.connect('tcp://relay-us-central-1.eve-emdr.com:8050')
    # Disable filtering.
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    received = 0
    recent = None
    while True:
        market_json = zlib.decompress(subscriber.recv())
        market_data = simplejson.loads(market_json)
        columns = market_data['columns']
        for rowset in market_data['rowsets']:
            received += len(rowset['rows'])
            generated_at = parser.parse(rowset['generatedAt'])
            if not recent or generated_at > recent: recent = generated_at


        sys.stdout.write("\rreceived: %d, most recent generated at %s" % (received, recent))
        sys.stdout.flush()

if __name__ == '__main__':
    main()

