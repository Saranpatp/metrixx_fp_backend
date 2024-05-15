from sqlalchemy.orm import Session
from . import models

def get_market_data(db: Session, skip: int = 0, limit: int = 10):
    return db.query(models.MarketData).offset(skip).limit(limit).all()

def get_market_data_by_symbol(db: Session, symbol: str):
    return db.query(models.MarketData).filter(models.MarketData.symbol == symbol).all()
