import time
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta

from app import models, crud, schemas, notification
from app.database import SessionLocal, engine
import pandas as pd
import numpy as np
import csv

# Initialize the database
models.Base.metadata.create_all(bind=engine)
tickers = ['MES 06-24', 'ES 06-24']

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
    return df

def fetch_and_print_last_min_data(symbol = 'MES 06-24'):
    with next(get_db()) as db:
        curr_time = datetime.now().replace(second=0) - timedelta(seconds=1)
        market_data = crud.get_market_data_by_symbol_time(db, symbol=symbol, start_time= curr_time - timedelta(minutes=1), end_time=curr_time)
        data = [result.__dict__ for result in market_data]
    
    # Remove the SQLAlchemy instance state from each dictionary
    for item in data:
        item.pop('_sa_instance_state', None)
    
    # Convert to a DataFrame
    df = pd.DataFrame(data)
    print(df.head())
    return df

def foot_print_transformation(df):
    df = df[['symbol','datetime','last_price','trades', 'total_volume', 'total_buying_volume','total_selling_volume']]
    df.columns = ['Symbol', 'Time', 'Price', 'Trades', 'TotalVolume', 'Bid', 'Ask']
    df['Time'] = pd.to_datetime(df['Time']).dt.floor('Min')

    #Create footprint chart data
    footprint_data = df.groupby(['Time', 'Price']).agg(
        Bid=('Bid', 'sum'),
        Ask=('Ask', 'sum'),
        Volume=('TotalVolume', 'sum') 
    ).reset_index() 

    #sort value
    footprint_data = footprint_data.sort_values(by=['Time', 'Price'])
    return(footprint_data)

def volume_cluster(footprint, tick_size = 0.25, cluster_param = 5):
    result = pd.DataFrame()
    price_by_time = footprint.sort_values(by=['Volume'],ascending=False)
    time = footprint.loc[0,'Time']
    price_step = price_by_time.shape[0]
    price_by_time = price_by_time[0:cluster_param]
    if price_step >= cluster_param:
        max = price_by_time['Price'].max()
        min = price_by_time['Price'].min()
        if (max - min)/tick_size == cluster_param-1:
            result = pd.concat([result, pd.DataFrame([['Volume Cluster', time, max]],columns=['Event', 'Time', 'Price'])])
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    
    return result

def imbalance(footprint, tick_size = 0.25, stacked_param = 3, imbalance_param = 3):
    time = footprint.loc[0,'Time']
    #compute imbalance and preparing shifted data
    testdf = footprint.sort_values(by=['Price'],ascending=False)
    temp = pd.DataFrame(np.arange (testdf['Price'].min(),testdf['Price'].max()+tick_size,tick_size)).rename(columns={0: "Price"})
    temp = temp.sort_values(by=['Price'],ascending=False)
    temp["Price_Up"] = temp['Price']+tick_size
    temp = temp.merge(testdf[{'Price','Bid'}], how='left')
    temp = temp.merge(testdf[{'Price','Ask'}], how='left', left_on='Price_Up', right_on='Price')
    temp = temp.rename(columns={'Price_x': "Price"})
    temp = temp[['Price','Bid','Ask']]
    temp['imbalance'] = abs(temp['Bid'] - temp['Ask']) / np.minimum(temp['Bid'], temp['Ask'])
    # to litigate divided by zero
    temp.fillna(0, inplace=True)
    temp = temp.replace([np.inf, -np.inf], 0)
    # create a list of our conditions
    conditions = [
       (temp['imbalance'] >= imbalance_param) & (temp['Bid'] > temp['Ask']),
       (temp['imbalance'] >= imbalance_param) & (temp['Bid'] < temp['Ask'])
       ]
    # create a list of the values we want to assign for each condition
    values = ['Selling Imbalance', 'Buying Imbalance']
    # create a new column and use np.select to assign values to it using our lists as arguments
    temp['Imbalance_Flag'] = np.select(conditions, values)
    data = temp[['Price', 'Imbalance_Flag']]
    #start to find stack imbalance
    result = pd.DataFrame()
    count = 0
    stack_price = 0
    current_flag = 'n'
    for price, flag in data.itertuples(index=False):
        if flag in ('Selling Imbalance', 'Buying Imbalance') and flag == current_flag:
            count +=1
            current_flag = flag
        else:
            count = 1
            current_flag = flag
            stack_price = price
        if count == stacked_param:
            result = pd.concat([result, pd.DataFrame([[flag, time, stack_price]],columns=['Event', 'Time', 'Price'])])
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    
    return result

def multiple_high_volume_node(footprint, n_node = 2, last_price = 0, multiple_count = 1):
    time = footprint.loc[0,'Time']
    result = pd.DataFrame()
    price_by_time = footprint.sort_values(by=['Volume'],ascending=False)
    price = price_by_time['Price'].iloc[0]
    if price == last_price:
        multiple_count += 1
        if multiple_count == n_node:
            result = pd.concat([result, pd.DataFrame([['Multiple High Volume Node', time, price]],columns=['Event', 'Time', 'Price'])])
    else:
        multiple_count = 1    
    
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    
    return price, multiple_count, result

def zero_print(footprint):
    result = pd.DataFrame()
    time = footprint.loc[0,'Time']
    df = footprint.sort_values(by=['Price'],ascending=False)
    if df['Bid'].iloc[-1] == 0:
        result = pd.concat([result, pd.DataFrame([['Bid Zero Print', time, df['Price'].iloc[-1]]],columns=['Event', 'Time', 'Price'])])
    if df['Ask'].iloc[0] == 0:
        result = pd.concat([result, pd.DataFrame([['Ask Zero Print', time, df['Price'].iloc[0]]],columns=['Event', 'Time', 'Price'])])
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    return result
    
def main():
    last_price_dict = {ticker: 0 for ticker in tickers}
    multiple_count_dict = {ticker: 1 for ticker in tickers}
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M")
    while True:
        for ticker in tickers:
            #fetch_and_print_latest_data()
            try:
                df = fetch_and_print_last_min_data(ticker)
            except Exception as e:
                print(e)
            if df.empty:
                continue
            footprint = foot_print_transformation(df)
            cluster_result = volume_cluster(footprint)
            cluster_result.insert(loc=0, column='Symbol', value=ticker)
            if cluster_result['Price'][0] != 0:
                notification.sent_msg(f"{cluster_result['Symbol'][0]}: {cluster_result['Event'][0]} at {cluster_result['Price'][0]} USD at {cluster_result['Time'].dt.strftime('%Y-%m-%d %H:%M')[0]}")
                cluster_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
            
            imbalance_result = imbalance(footprint)
            imbalance_result.insert(loc=0, column='Symbol', value=ticker)
            if imbalance_result['Price'][0] != 0:
                notification.sent_msg(f"{imbalance_result['Symbol'][0]}: {imbalance_result['Event'][0]} at {imbalance_result['Price'][0]} USD at {imbalance_result['Time'].dt.strftime('%Y-%m-%d %H:%M')[0]}")
                imbalance_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')

            price, multiple_count_new, multiple_result = multiple_high_volume_node(footprint, n_node=2, last_price=last_price_dict[ticker], multiple_count=multiple_count_dict[ticker])
            multiple_result.insert(loc=0, column='Symbol', value=ticker)
            if multiple_result['Price'][0] != 0:
                notification.sent_msg(f"{multiple_result['Symbol'][0]}: {multiple_result['Event'][0]} at {multiple_result['Price'][0]} USD at {multiple_result['Time'].dt.strftime('%Y-%m-%d %H:%M')[0]}")
                multiple_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
            last_price_dict[ticker] = price
            multiple_count_dict[ticker] = multiple_count_new
            
            zero_print_result = zero_print(footprint)
            zero_print_result.insert(loc=0, column='Symbol', value=ticker)
            if zero_print_result['Price'].iloc[0] != 0:
                notification.sent_msg(f"{zero_print_result['Symbol'].iloc[0]}: {zero_print_result['Event'].iloc[0]} zero print at {zero_print_result['Price'].iloc[0]} USD at {zero_print_result['Time'].dt.strftime('%Y-%m-%d %H:%M').iloc[0]}")
                zero_print_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')

        time.sleep(60)

if __name__ == "__main__":
    main()
