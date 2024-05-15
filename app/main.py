from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from . import models, crud, schemas
from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/market_data/", response_model=List[schemas.MarketData])
def read_market_data(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    market_data = crud.get_market_data(db, skip=skip, limit=limit)
    return market_data

@app.get("/market_data/{symbol}", response_model=List[schemas.MarketData])
def read_market_data_by_symbol(symbol: str, db: Session = Depends(get_db)):
    market_data = crud.get_market_data_by_symbol(db, symbol=symbol)
    if not market_data:
        raise HTTPException(status_code=404, detail="Market data not found")
    return market_data


