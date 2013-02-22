import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Float, DateTime, Boolean, BigInteger
from sqlalchemy.orm import sessionmaker

engine = create_engine("postgresql://samuel@/eve")
Base = declarative_base(bind=engine)
session = sessionmaker(bind=engine)()

def convert(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

class Order(Base):
    __tablename__ = 'orders'
    order_id = Column(BigInteger, primary_key=True)
    type_id = Column(Integer)
    region_id = Column(Integer)
    price = Column(Float)
    vol_remaining = Column(Integer)
    range = Column(Integer)
    vol_entered = Column(Integer)
    min_volume = Column(Integer)
    bid = Column(Boolean)
    issue_date = Column(DateTime(timezone=True))
    duration = Column(Integer)
    station_id = Column(Integer)
    solar_system_id = Column(Integer)
    generated_at = Column(DateTime(timezone=True))

    def __init__(self, data):
        converted = {convert(key): val for key, val in data.iteritems()}
        self.__dict__.update(converted)
    
    def __repr__(self):
        return '<Order(%d, %d, %d, %f, %s)>' % (self.order_id, self.type_id, self.region_id, self.price, self.issue_date)

class History(Base):
    __tablename__ = 'history'
    type_id = Column(Integer, primary_key=True)
    region_id = Column(Integer, primary_key=True)
    date = Column(DateTime(timezone=True), primary_key=True)
    orders= Column(Integer)
    low = Column(Float)
    high = Column(Float)
    average = Column(Float)
    quantity = Column(BigInteger)
    generated_at = Column(DateTime(timezone=True))

    def __init__(self, data):
        converted = {convert(key): val for key, val in data.iteritems()}
        self.__dict__.update(converted)

    def __repr__(self):
        return '<History(%d, %d, %s, %d, %f, %f)>' % (typeID, regionID, date, quantity, low, high)

class Item(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    station_id = Column(Integer, primary_key=True)
    region_id = Column(Integer)
    solar_system_id = Column(Integer)
    generated_at = Column(DateTime)
    max_bid = Column(Float)
    min_sell = Column(Float)
    quantity = Column(BigInteger)

    def __init__(self, data):
        converted = {convert(key): val for key, val in data.iteritems()}
        self.__dict__.update(converted)

    def __repr__(self):
        return '<History(%d, %d, %s, %d, %f, %f)>' % (self.type_id, self.region_id, self.date, self.quantity, self.low, self.high)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
