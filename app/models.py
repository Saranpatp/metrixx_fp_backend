from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class MarketData(Base):
    __tablename__ = 'market_data'
    
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    datetime = Column(DateTime)
    last_price = Column(Float)
    trades = Column(Integer)
    total_volume = Column(Integer)
    total_buying_volume = Column(Integer)
    total_selling_volume = Column(Integer)
    bar_delta = Column(Integer)
    delta_percent = Column(Float)
    delta_for_price = Column(Integer)
    ask_volume_for_price = Column(Integer)
    bid_volume_for_price = Column(Integer)
    total_volume_for_price = Column(Integer)
    max_ask_volume = Column(Integer)
    price_with_max_ask_volume = Column(String)
    max_bid_volume = Column(Integer)
    price_with_max_bid_volume = Column(String)
    max_combined_volume = Column(Integer)
    price_with_max_combined_volume = Column(String)
    max_positive_delta = Column(Integer)
    max_negative_delta = Column(Integer)
    max_seen_delta = Column(Integer)
    min_seen_delta = Column(Integer)
    cumulative_delta = Column(Integer)
