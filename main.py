import time
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta

from app import models, crud, schemas
from app.database import SessionLocal, engine
import pandas as pd

# Initialize the database
models.Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def fetch_and_print_latest_data():
    with next(get_db()) as db:
        curr_time = datetime.now()
        market_data = crud.get_market_data_by_symbol_time(db, symbol='MES 06-24', start_time= curr_time - timedelta(minutes=1), end_time=curr_time)
        data = [result.__dict__ for result in market_data]
    
    # Remove the SQLAlchemy instance state from each dictionary
    for item in data:
        item.pop('_sa_instance_state', None)
    
    # Convert to a DataFrame
    df = pd.DataFrame(data)
    print(df.head())

def main():
    while True:
        fetch_and_print_latest_data()
        time.sleep(60)

if __name__ == "__main__":
    main()
