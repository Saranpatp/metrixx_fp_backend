import time
from sqlalchemy.orm import Session
from typing import List

from app import models, crud, schemas
from app.database import SessionLocal, engine

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
        market_data = crud.get_market_data(db, skip=0, limit=10)
        for data in market_data:
            print(data.id)

def main():
    while True:
        fetch_and_print_latest_data()
        time.sleep(15)

if __name__ == "__main__":
    main()
