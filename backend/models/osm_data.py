import pandas as pd
from sqlalchemy import create_engine

def create_sqlalchemy_engine():
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/postgres')
    return engine

def get_osm_data():
    engine = create_sqlalchemy_engine()
    query = "SELECT road_id, latitude, longitude, highway, way_geometry, length FROM osm_road_data"
    df = pd.read_sql(query, con=engine)
    engine.dispose()
    return df
