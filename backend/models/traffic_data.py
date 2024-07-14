import pandas as pd
from sqlalchemy import create_engine

def create_sqlalchemy_engine():
    engine = create_engine('mysql+mysqlconnector://root@localhost/traffic_data')
    return engine

def get_historical_traffic_data(realtime=False):
    engine = create_sqlalchemy_engine()
    query = "SELECT * FROM historical_traffic_data"
    df = pd.read_sql(query, con=engine)
    engine.dispose()
    
    if realtime:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.sort_values('timestamp', inplace=True)
        return df.iloc[-1:]  # Return the last record as real-time data
    return df

def get_weather_data():
    engine = create_sqlalchemy_engine()
    query = "SELECT * FROM weather"
    df = pd.read_sql(query, con=engine)
    engine.dispose()
    return df
