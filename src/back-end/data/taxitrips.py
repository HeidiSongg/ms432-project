import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import psycopg2
import json
import requests
import os
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.getenv('API_KEY')

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
    WHERE trip_start_timestamp >='2020-03-01'
    AND pickup_community_area IS NOT NULL 
    AND dropoff_community_area IS NOT NULL
LIMIT 1000
"""

results = client.get("m6dm-c72p", query=query)

# Convert results to DataFrame
results_df = pd.DataFrame.from_records(results)

# Convert columns to appropriate types
for col in ['pickup_community_area','dropoff_community_area','pickup_centroid_latitude','pickup_centroid_longitude','dropoff_centroid_latitude','dropoff_centroid_longitude']:
    results_df[col] = pd.to_numeric(results_df[col], errors='coerce')

for col in ['trip_start_timestamp','trip_end_timestamp']:
    results_df[col] = pd.to_datetime(results_df[col], errors='coerce')

# Convert complex types to JSON strings
results_df = results_df.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# Function to get ZIP code from latitude and longitude using Google Geocoding API
def get_zip_code(lat, lon, api_key):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['status'] == 'OK':
            for component in data['results'][0]['address_components']:
                if 'postal_code' in component['types']:
                    return component['long_name']
    return None

results_df['pickup_zip_code'] = results_df.apply(
    lambda row: get_zip_code(row['pickup_centroid_latitude'], row['pickup_centroid_longitude'], API_KEY) if pd.notnull(row['pickup_centroid_latitude']) and pd.notnull(row['pickup_centroid_longitude']) else None,
    axis=1
)

results_df['dropoff_zip_code'] = results_df.apply(
    lambda row: get_zip_code(row['dropoff_centroid_latitude'], row['dropoff_centroid_longitude'], API_KEY) if pd.notnull(row['dropoff_centroid_latitude']) and pd.notnull(row['dropoff_centroid_longitude']) else None,
    axis=1
)

# Define PostgreSQL connection details
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST')
}

# Create table if it doesn't exist
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS taxitrip (
    trip_id TEXT,
    trip_start_timestamp TIMESTAMPTZ,
    trip_end_timestamp TIMESTAMPTZ,
    pickup_community_area NUMERIC,
    dropoff_community_area NUMERIC,
    pickup_centroid_latitude NUMERIC,
    pickup_centroid_longitude NUMERIC,
    dropoff_centroid_latitude NUMERIC,
    dropoff_centroid_longitude NUMERIC,
    pickup_zip_code TEXT,
    dropoff_zip_code TEXT           
)
''')
conn.commit()

# Create engine and load data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}")
results_df.to_sql('taxitrip', con=engine, if_exists='replace', index=False)

# Close the connection
conn.close()
