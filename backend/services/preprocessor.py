from shapely.wkb import loads as wkb_loads
from geopy.distance import distance
import json
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler

def preprocess_historical_data(df):
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['latitudes'], df['longitudes'] = zip(*df['coordinates'].apply(extract_coordinates))

    max_points = max(df['latitudes'].apply(len).max(), df['longitudes'].apply(len).max())
    latitude_cols = [f'latitude_{i}' for i in range(max_points)]
    longitude_cols = [f'longitude_{i}' for i in range(max_points)]

    df = pd.concat([df, pd.DataFrame(df['latitudes'].tolist(), columns=latitude_cols, index=df.index)], axis=1)
    df = pd.concat([df, pd.DataFrame(df['longitudes'].tolist(), columns=longitude_cols, index=df.index)], axis=1)

    df.drop(columns=['coordinates', 'latitudes', 'longitudes'], inplace=True)
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['hour_of_day'] = df['timestamp'].dt.hour
    df['unix_timestamp'] = df['timestamp'].astype('int64') // 10**9
    df['congestion_level'] = df.apply(lambda row: calculate_congestion_level(row['current_speed'], row['free_flow_speed']), axis=1)

    numerical_features = ['current_speed', 'free_flow_speed', 'current_travel_time', 'free_flow_travel_time']
    scaler = StandardScaler()
    df[numerical_features] = scaler.fit_transform(df[numerical_features])

    return df[['timestamp', 'latitude_0', 'longitude_0', 'day_of_week', 'hour_of_day', 'unix_timestamp', 'congestion_level']]

def extract_coordinates(coord_json):
    coordinates = json.loads(coord_json)['coordinate']
    latitudes = [point['latitude'] for point in coordinates]
    longitudes = [point['longitude'] for point in coordinates]
    return latitudes, longitudes

def preprocess_weather_data(weather_df):
    weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])
    weather_df['day_of_week'] = weather_df['datetime'].dt.dayofweek
    weather_df['hour'] = weather_df['datetime'].dt.hour

    numerical_cols = ['temperature', 'precipitation', 'wind_speed', 'visibility', 'humidity', 'pressure']
    for column in numerical_cols:
        if weather_df[column].isnull().any():
            mode_value = weather_df[column].mode()[0]
            weather_df[column].fillna(mode_value, inplace=True)

    weather_df[numerical_cols] = (weather_df[numerical_cols] - weather_df[numerical_cols].mean()) / weather_df[numerical_cols].std()

    return weather_df

def preprocess_osm_data(df):
    df['road_length'] = df.apply(lambda row: calculate_road_length(row['way_geometry']), axis=1)
    df = pd.get_dummies(df, columns=['highway'], prefix='highway')
    
    scaler = MinMaxScaler()
    df[['latitude', 'longitude', 'length', 'road_length']] = scaler.fit_transform(df[['latitude', 'longitude', 'length', 'road_length']])
    
    return df

def calculate_road_length(way_geometry):
    geom = wkb_loads(way_geometry)
    length = 0
    for i in range(len(geom.coords) - 1):
        length += distance(geom.coords[i], geom.coords[i+1]).meters
    return length

def merge_data(traffic_df, weather_df, osm_df):
    merged_df = pd.merge_asof(traffic_df.sort_values('timestamp'), weather_df.sort_values('datetime'), left_on='timestamp', right_on='datetime', direction='nearest')
    merged_df = pd.merge(merged_df, osm_df, left_on=['latitude_0', 'longitude_0'], right_on=['latitude', 'longitude'], how='left')
    return merged_df
