import time
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta

from app import models, crud, schemas
from app.database import SessionLocal, engine
import pandas as pd
import numpy as np
import csv

# Initialize the database
models.Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def fetch_and_print_data(start_time = datetime.min, end_time = datetime.now(), symbol = 'MES 06-24'):
    with next(get_db()) as db:
        curr_time = datetime.now()
        market_data = crud.get_market_data_by_symbol_time(db, symbol=symbol, start_time= start_time, end_time = end_time)
        data = [result.__dict__ for result in market_data]
    
    # Remove the SQLAlchemy instance state from each dictionary
    for item in data:
        item.pop('_sa_instance_state', None)
    
    # Convert to a DataFrame
    df = pd.DataFrame(data)
    return df

def foot_print_transformation_time_frame(df, time_frame_min=1):
    df = df[['symbol','datetime','last_price','trades', 'total_volume', 'total_buying_volume','total_selling_volume']]
    df.columns = ['Symbol', 'Time', 'Price', 'Trades', 'TotalVolume', 'Bid', 'Ask']
    df['Time'] = df['Time'].apply(lambda t: datetime(t.year, t.month, t.day, t.hour,time_frame_min*(t.minute // time_frame_min)))

    #Create footprint chart data
    footprint_data = df.groupby(['Time', 'Price']).agg(
        Bid=('Bid', 'sum'),
        Ask=('Ask', 'sum'),
        Volume=('TotalVolume', 'sum') 
    ).reset_index()
    #sort value
    footprint_data = footprint_data.sort_values(by=['Time', 'Price'])
    return(footprint_data)

def bid_ask_spread(df, time_frame_min = 1):
    footprint = foot_print_transformation_time_frame(df, time_frame_min=time_frame_min)
    footprint['PriceXBid'] = footprint['Price'] * footprint['Bid']
    footprint['PriceXAsk'] = footprint['Price'] * footprint['Ask']
    
    # Finding Price
    weighted_average = footprint.groupby('Time').agg(PriceXBid=('PriceXBid', sum), PriceXAsk=('PriceXAsk', sum), Bid=('Bid',sum), Ask=('Ask',sum))
    weighted_average['Weighted_Bid_Price'] = np.where(weighted_average['Bid'] != 0, weighted_average['PriceXBid']/weighted_average['Bid'], np.nan)
    weighted_average['Weighted_Ask_Price'] = np.where(weighted_average['Ask'] != 0, weighted_average['PriceXAsk']/weighted_average['Ask'], np.nan)
    max_BidPrice = footprint[footprint['Bid'] != 0].groupby('Time')['Price'].max().reset_index(name='Max_Bid')
    min_AskPrice = footprint[footprint['Ask'] != 0].groupby('Time')['Price'].min().reset_index(name='Min_Ask')

    # Merge the results on 'Time'
    spread = pd.merge(weighted_average, min_AskPrice, on='Time', how='left')
    spread = pd.merge(spread, max_BidPrice, on='Time')
    spread['Bid_Ask_Spread'] = spread['Min_Ask']-spread['Max_Bid']
    spread['Weighted_Bid_Ask_Spread'] = spread['Weighted_Ask_Price']-spread['Weighted_Bid_Price']
    spread = spread[['Time','Bid_Ask_Spread','Weighted_Bid_Ask_Spread']]
    return spread


def turn_over_ratio(df, time_frame_min = 1):
    df['Time'] = df['datetime'].apply(lambda t: datetime(t.year, t.month, t.day, t.hour,time_frame_min*(t.minute // time_frame_min)))
    turnover_df = df.groupby(['Time']).agg(
        Trades=('trades', 'sum'),  # Sum the 'Bid' column
        TotalVolume=('total_volume', 'sum')   # Sum the 'Ask' column
    ).reset_index() 
    turnover_df['Turnover_Ratio'] = turnover_df['Trades']/turnover_df['TotalVolume']
    turnover_df = turnover_df[['Time','Turnover_Ratio']]
    return turnover_df

def get_liquidity_index(start_time = datetime.min, end_time = datetime.now(), symbol = 'MES 06-24', time_frame_min = 1):
    df = fetch_and_print_data(start_time, end_time, symbol= symbol)
    BidAsk = bid_ask_spread(df, time_frame_min=time_frame_min)
    turnover = turn_over_ratio(df, time_frame_min=time_frame_min)
    result = pd.merge(BidAsk, turnover, on='Time', how='left')
    return result


result = get_liquidity_index(time_frame_min=30)
print(result.head(20))


