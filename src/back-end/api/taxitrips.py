import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import psycopg2
import json

client = Socrata("data.cityofchicago.org", None, timeout = 120)

query = """SELECT trip_id \
    ,trip_start_timestamp \
    ,trip_end_timestamp \
    ,trip_seconds  \
    ,trip_miles \
    ,pickup_census_tract \
    ,dropoff_census_tract \
    ,pickup_community_area \
    ,dropoff_community_area \
    ,pickup_centroid_latitude \
    ,pickup_centroid_longitude \
    ,dropoff_centroid_latitude \
    ,dropoff_centroid_longitude \
    WHERE trip_start_timestamp >='2020-01-01'
    limit 20000 """

results = client.get("m6dm-c72p", query=query)

results_df = pd.DataFrame.from_records(results)

def convert_complex_types(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

results_df = convert_complex_types(results_df)

def load_data_to_postgresql(df):
    engine = create_engine('postgresql+psycopg2://hs:password1@localhost/postgres')
    df.to_sql('taxitrips', con=engine, if_exists='replace', index=False)

conn = psycopg2.connect(
    host="localhost",  
    database="postgres",  
    user="hs", 
    password="password1" 
)

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS taxitrips (
    trip_id TEXT,
    trip_start_timestamp TIMESTAMPTZ,
    trip_end_timestamp TIMESTAMPTZ,
    trip_seconds NUMERIC,
    trip_miles NUMERIC,
    pickup_community_area NUMERIC,
    dropoff_community_area NUMERIC,
    pickup_centroid_latitude NUMERIC,
    pickup_centroid_longitude NUMERIC,
    dropoff_centroid_latitude NUMERIC,
    dropoff_centroid_longitude NUMERIC
)
''')
conn.commit()

engine = create_engine('postgresql+psycopg2://hs:password1@localhost:5432/chicago')

load_data_to_postgresql(results_df)

conn.close()