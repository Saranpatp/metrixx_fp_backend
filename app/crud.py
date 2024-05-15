from sqlalchemy.orm import Session
from . import models

from datetime import datetime

def get_market_data(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.MarketData).offset(skip).limit(limit).all()

def get_market_data_by_symbol(db: Session, symbol: str):
    return db.query(models.MarketData).filter(models.MarketData.symbol == symbol).all()

# TODO create a function to get prev min data with parameter (symbol and start_time and end_time)

def get_market_data_by_symbol_time(db: Session, symbol: str, start_time: datetime, end_time: datetime):
    return db.query(models.MarketData).filter(
        models.MarketData.symbol == symbol,
        models.MarketData.datetime >= start_time,
        models.MarketData.datetime <= end_time
    ).all()