import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import psycopg2
import json

# Initialize Socrata client
client = Socrata("data.cityofchicago.org", None)

# Query data
query = """
SELECT 
    trip_id,
    trip_start_timestamp,
    trip_end_timestamp,
    pickup_community_area,
    dropoff_community_area,
    pickup_centroid_latitude,
    pickup_centroid_longitude,
    dropoff_centroid_latitude,
    dropoff_centroid_longitude
    WHERE trip_start_timestamp >='2020-01-01'
LIMIT 20000
"""

results = client.get("m6dm-c72p", query=query)

# Convert results to DataFrame
results_df = pd.DataFrame.from_records(results)


for col in ['pickup_community_area','dropoff_community_area','pickup_centroid_latitude','pickup_centroid_longitude','dropoff_centroid_latitude','dropoff_centroid_longitude']:
    results_df[col] = pd.to_numeric(results_df[col], errors='coerce')

for col in ['trip_start_timestamp','trip_end_timestamp']:
    results_df[col] = pd.to_datetime(results_df[col], errors='coerce')

# Convert complex types to JSON strings
results_df = results_df.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# Define PostgreSQL connection details
db_config = {
    'dbname': 'postgres',
    'user': 'hs',
    'password': 'password1',
    'host': 'localhost'
}

# Create table if it doesn't exist
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS taxitrips (
    trip_id TEXT,
    trip_start_timestamp TIMESTAMPTZ,
    trip_end_timestamp TIMESTAMPTZ,
    pickup_community_area NUMERIC,
    dropoff_community_area NUMERIC,
    pickup_centroid_latitude NUMERIC,
    pickup_centroid_longitude NUMERIC,
    dropoff_centroid_latitude NUMERIC,
    dropoff_centroid_longitude NUMERIC
)
''')
conn.commit()

# Create engine and load data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}")
results_df.to_sql('taxitrips', con=engine, if_exists='replace', index=False)

# Close the connection
conn.close()