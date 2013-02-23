import zlib
import zmq
import time
import sys
import simplejson
import logging
from operator import attrgetter
from datetime import datetime, timedelta
from dateutil import parser
from itertools import * 
from model import *
from sqlalchemy import func

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

def init_data(columns, row, generated_at, type_id, region_id):
    data = dict(zip(columns, row))
    data['generatedAt'] = generated_at
    data['typeID'] = type_id
    data['regionID'] = region_id
    return data

def main():
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # Connect to the first publicly available relay.
    subscriber.connect('tcp://relay-us-central-1.eve-emdr.com:8050')
    # Disable filtering.
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    count = {'rows':0, 'orders':0, 'histories':0, 'items':0, 'received':0, 'recent':None}
    while True:
        market_json = zlib.decompress(subscriber.recv())
        market_data = simplejson.loads(market_json)
        columns = market_data['columns']
        for rowset in market_data['rowsets']:
            generated_at = parser.parse(rowset['generatedAt'])
            if not count['recent'] or generated_at > count['recent']: count['recent'] = generated_at
            type_id = rowset['typeID']
            region_id = rowset['regionID']
            count['received'] += len(rowset['rows'])
            if region_id != 10000002: break

            if market_data['resultType'] == 'history':
                most_recent = None
                for row in rowset['rows']:
                    data = init_data(columns, row, generated_at, type_id, region_id)

                    data['date'] = parser.parse(data['date'])
                    history = session.query(History).get((type_id, region_id, data['date']))
                    if history:
                        if history.quantity != data['quantity']:
                            history.quantity = data['quantity']
                            history.generated_at = generated_at
                        break
                    else:
                        history = History(data)
                        session.add(history)
                        count['histories'] += 1

            elif market_data['resultType'] == 'orders':
                by_station = {}
                for row in rowset['rows']:
                    data = init_data(columns, row, generated_at, type_id, region_id)

                    data['issueDate'] = parser.parse(data['issueDate'])
                    order = session.query(Order).get(data['orderID'])
                    if order:
                        if order.vol_remaining != data['volRemaining']:
                            order.vol_entered = data['volRemaining']
                            order.generated_at = generated_at
                    else:
                        order = Order(data)
                        session.add(order)
                        count['orders'] += 1
                    if order.station_id not in by_station:
                        by_station[order.station_id] = [order]
                    else:
                        by_station[order.station_id].append(order)

                for station_id, orders in by_station.iteritems():
                    item = session.query(Item).get((type_id, station_id))
                    if not item:
                        item = Item({
                                'id': type_id,
                                'station_id': station_id,
                                'region_id': region_id,
                                'solar_system_id': orders[0].solar_system_id
                                })
                        session.add(item)
                        count['items']+=1
                    elif generated_at < item.generated_at:
                        break;
                                 
                    bid = attrgetter('bid')
                    sort = sorted(orders, key=bid)
                    
                    bids = {k:list(g) for k,g in groupby(sort, key=bid)}
                    item.max_bid = max(o.price for o in bids[True]) if True in bids else None
                    item.min_sell = min(o.price for o in bids[False]) if False in bids else None

                    item.quantity = session.query(func.avg(History.quantity)).filter_by(type_id=type_id, region_id=region_id).filter(History.date > datetime.now() - timedelta(days=7)).one()
                    item.generated_at = generated_at

        try:
            session.commit()
        except Exception as ex:
            session.rollback()
            logger.exception(ex)
        
        count['rows'] = count['orders'] + count['histories']
        sys.stdout.write("\r%(rows)d rows stored out of %(received)d received: %(orders)d orders, %(histories)d history, %(items)d items. Most recent generated at %(recent)s" % (count))
        sys.stdout.flush()

if __name__ == '__main__':
    main()
