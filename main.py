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
# Add more tickers to the infinite loop here
tickers = ['MES 06-24', 'ES 06-24']

# get database connection
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# fetch latest data in the past 60 seconds (from now-60s to now) 
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

# fetch data of the full previous minute data (eg. if now is 12:15:30, return data from to 12:14:00 - 12:14:59 )
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

# Function to mimic footprint chart data
def foot_print_transformation(df):
    # select and format data
    df = df[['symbol','datetime','last_price','trades', 'total_volume', 'total_buying_volume','total_selling_volume']]
    df.columns = ['Symbol', 'Time', 'Price', 'Trades', 'TotalVolume', 'Bid', 'Ask']
    df['Time'] = pd.to_datetime(df['Time']).dt.floor('Min')

    #Create footprint chart data (compute bid and ask at prices in a time period)
    footprint_data = df.groupby(['Time', 'Price']).agg(
        Bid=('Bid', 'sum'),
        Ask=('Ask', 'sum'),
        Volume=('TotalVolume', 'sum') 
    ).reset_index() 

    #sort value
    footprint_data = footprint_data.sort_values(by=['Time', 'Price'])
    return(footprint_data)

# Function to detect volume cluster
def volume_cluster(footprint, tick_size = 0.25, cluster_param = 5):
    result = pd.DataFrame()
    #sort volume descending to see what prices have highest/lowest volume
    price_by_time = footprint.sort_values(by=['Volume'],ascending=False)
    #get time (there is only one time in a footprint data)
    time = footprint.loc[0,'Time']
    #count unique price at a time
    price_step = price_by_time.shape[0]
    #filter the most volume price up to number of cluster parameter
    price_by_time = price_by_time[0:cluster_param]
    #check if there is price step more than the cluster parameter. If not, there is no way for volume cluster
    if price_step >= cluster_param:
        max = price_by_time['Price'].max()
        min = price_by_time['Price'].min()
        #there is volume cluster if and only if number of tick between max and min; (max-min)/tick_price = cluster_param - 1
        if (max - min)/tick_size == cluster_param-1:
            result = pd.concat([result, pd.DataFrame([['Volume Cluster', time, max]],columns=['Event', 'Time', 'Price'])])
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    
    return result

# Function to detect selling and buying imbalance
def imbalance(footprint, tick_size = 0.25, stacked_param = 3, imbalance_param = 3):
    time = footprint.loc[0,'Time']
    #compute imbalance and preparing shifted data (we compute differnce between bid at price x and ask at price x+1_tick)
    testdf = footprint.sort_values(by=['Price'],ascending=False)
    temp = pd.DataFrame(np.arange (testdf['Price'].min(),testdf['Price'].max()+tick_size,tick_size)).rename(columns={0: "Price"})
    temp = temp.sort_values(by=['Price'],ascending=False)
    temp["Price_Up"] = temp['Price']+tick_size
    temp = temp.merge(testdf[['Price','Bid']], how='left')
    temp = temp.merge(testdf[['Price','Ask']], how='left', left_on='Price_Up', right_on='Price')
    temp = temp.rename(columns={'Price_x': "Price"})
    temp = temp[['Price','Bid','Ask']]
    temp['imbalance'] = abs(temp['Bid'] - temp['Ask']) / np.minimum(temp['Bid'], temp['Ask'])
    # to handle n/a and undefined error
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
    #start to find stack imbalance and set counting parameter
    result = pd.DataFrame()
    count = 0
    stack_price = 0
    current_flag = 'n'
    for price, flag in data.itertuples(index=False):
        # in case previous and current flag are the same, count + 1
        if flag in ('Selling Imbalance', 'Buying Imbalance') and flag == current_flag:
            count +=1
            current_flag = flag
        # if not, reset the count
        else:
            count = 1
            current_flag = flag
            stack_price = price
        # Signal the event if count = imbalance parameter
        if count == stacked_param:
            result = pd.concat([result, pd.DataFrame([[flag, time, stack_price]],columns=['Event', 'Time', 'Price'])])
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    
    return result

# Function to detect multiple high volume node
def multiple_high_volume_node(footprint, n_node = 2, last_price = 0, multiple_count = 1):
    time = footprint.loc[0,'Time']
    result = pd.DataFrame()
    #get the highest volume price
    price_by_time = footprint.sort_values(by=['Volume'],ascending=False)
    price = price_by_time['Price'].iloc[0]
    # check if price = last price. If so, multiple_count + 1
    if price == last_price:
        multiple_count += 1
        # when multiple_count reaches predefinec n_node, signal an event.
        if multiple_count == n_node:
            result = pd.concat([result, pd.DataFrame([['Multiple High Volume Node', time, price]],columns=['Event', 'Time', 'Price'])])
    # if price != last price, reset multiple_count
    else:
        multiple_count = 1    
    
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    #return price and multiple_count to be input for the next loop
    return price, multiple_count, result

# Function to detect zero print
def zero_print(footprint):
    result = pd.DataFrame()
    time = footprint.loc[0,'Time']
    #sort value by price to mimic a foot print candles
    df = footprint.sort_values(by=['Price'],ascending=False)
    # there is Bid Zero Print if Bid = 0 at the lowest price
    if df['Bid'].iloc[-1] == 0:
        result = pd.concat([result, pd.DataFrame([['Bid Zero Print', time, df['Price'].iloc[-1]]],columns=['Event', 'Time', 'Price'])])
    # there is Ask Zero Print if Ask = 0 at the highest price
    if df['Ask'].iloc[0] == 0:
        result = pd.concat([result, pd.DataFrame([['Ask Zero Print', time, df['Price'].iloc[0]]],columns=['Event', 'Time', 'Price'])])
    # return price = 0 if there is no Zero Print
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    return result

# Function to detect failed auction
def failed_auction(footprint):
    result = pd.DataFrame()
    time = footprint.loc[0,'Time']
    # sort value by price to mimic a foot print candles
    df = footprint.sort_values(by=['Price'],ascending=False)
    # there is failed auction - Bid high if bid is not zero at the highest price
    if df['Bid'].iloc[0] != 0:
        result = pd.concat([result, pd.DataFrame([['Failed Auction - Bid High', time, df['Price'].iloc[0]]],columns=['Event', 'Time', 'Price'])])
    # there is failed auction - Ask low if Ask is not zero at the lowest price
    if df['Ask'].iloc[-1] != 0:
        result = pd.concat([result, pd.DataFrame([['Failed Auction - Ask Low', time, df['Price'].iloc[-1]]],columns=['Event', 'Time', 'Price'])])
    # return price = 0 if there is no failed auction
    if result.empty:
        result = pd.concat([result, pd.DataFrame([['NaN', time, 0]],columns=['Event', 'Time', 'Price'])])
    return result

# this is the main fuction that call all functions to query data, detect foot print events, and send notifications 
def main():
    # create a dictionary for last_price and multiple_count to be used as input for multiple_high_volume detection
    last_price_dict = {ticker: 0 for ticker in tickers}
    multiple_count_dict = {ticker: 1 for ticker in tickers}
    # start time for log file
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M")
    # infinite loop to query data and event detection
    while True:
        # Loop for all tickers at the top
        for ticker in tickers:
            # Fetch last minute data, if error skip to the next loop
            try:
                df = fetch_and_print_last_min_data(ticker)
            except Exception as e:
                print(e)
            if df.empty:
                continue
            # transform to foottprint data
            footprint = foot_print_transformation(df)
            # Detect volume cluster, if it is found sent notification and save to log file
            cluster_result = volume_cluster(footprint)
            cluster_result.insert(loc=0, column='Symbol', value=ticker)
            if cluster_result['Price'][0] != 0:
                notification.sent_msg(f"{cluster_result['Symbol'][0]}: {cluster_result['Event'][0]} at {cluster_result['Price'][0]} USD at {cluster_result['Time'].dt.strftime('%Y-%m-%d %H:%M')[0]}")
                cluster_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
            # Detect imbalance, if it is found sent notification and save to log file
            imbalance_result = imbalance(footprint)
            imbalance_result.insert(loc=0, column='Symbol', value=ticker)
            if imbalance_result['Price'][0] != 0:
                notification.sent_msg(f"{imbalance_result['Symbol'][0]}: {imbalance_result['Event'][0]} at {imbalance_result['Price'][0]} USD at {imbalance_result['Time'].dt.strftime('%Y-%m-%d %H:%M')[0]}")
                imbalance_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
            # Detect multiple high volume node, if it is found sent notification and save to log file
            price, multiple_count_new, multiple_result = multiple_high_volume_node(footprint, n_node=2, last_price=last_price_dict[ticker], multiple_count=multiple_count_dict[ticker])
            multiple_result.insert(loc=0, column='Symbol', value=ticker)
            if multiple_result['Price'][0] != 0:
                notification.sent_msg(f"{multiple_result['Symbol'][0]}: {multiple_result['Event'][0]} at {multiple_result['Price'][0]} USD at {multiple_result['Time'].dt.strftime('%Y-%m-%d %H:%M')[0]}")
                multiple_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
            last_price_dict[ticker] = price
            multiple_count_dict[ticker] = multiple_count_new
            # Detect zero print, if it is found sent notification and save to log file
            zero_print_result = zero_print(footprint)
            zero_print_result.insert(loc=0, column='Symbol', value=ticker)
            if zero_print_result['Price'].iloc[0] != 0:
                notification.sent_msg(f"{zero_print_result['Symbol'].iloc[0]}: {zero_print_result['Event'].iloc[0]} at {zero_print_result['Price'].iloc[0]} USD at {zero_print_result['Time'].dt.strftime('%Y-%m-%d %H:%M').iloc[0]}")
                zero_print_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
            # Detect failed auction, if it is found sent notification and save to log file
            failed_auction_result = failed_auction(footprint)
            failed_auction_result.insert(loc=0, column='Symbol', value=ticker)
            if failed_auction_result['Price'].iloc[0] != 0:
                notification.sent_msg(f"{failed_auction_result['Symbol'].iloc[0]}: {failed_auction_result['Event'].iloc[0]} at {failed_auction_result['Price'].iloc[0]} USD at {failed_auction_result['Time'].dt.strftime('%Y-%m-%d %H:%M').iloc[0]}")
                failed_auction_result.to_csv(f'log_{start_time}.csv', sep='\t', header=None, mode='a')
        
        # once loop all the tickers in a minute, stop for 60 secs to do another loop. This is to make sure that we get data from different time period
        time.sleep(60)

if __name__ == "__main__":
    main()
