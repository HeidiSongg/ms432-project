import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import psycopg2
import json

# Initialize Socrata client
client = Socrata("data.cityofchicago.org", None)

# Query data
query = """
SELECT id,
    permit_,
    permit_status,
    permit_type,
    application_start_date ,
    issue_date ,
    street_number,
    street_direction,
    street_name ,
    work_type,
    work_description ,
    total_fee,
    community_area,
    census_tract,
    xcoordinate,
    ycoordinate,
    latitude,
    longitude
LIMIT 20000
"""

results = client.get("ydr8-5enu", query=query)

# Convert results to DataFrame
results_df = pd.DataFrame.from_records(results)


for col in ['total_fee','community_area','census_tract','xcoordinate','ycoordinate','latitude','longitude']:
    results_df[col] = pd.to_numeric(results_df[col], errors='coerce')

for col in ['application_start_date','issue_date']:
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
CREATE TABLE IF NOT EXISTS buildings (
    id TEXT,
    permit_ TEXT,
    permit_status TEXT,
    permit_type TEXT,
    application_start_date TIMESTAMPTZ,
    issue_date TIMESTAMPTZ,
    street_number NUMERIC,
    street_direction TEXT,
    street_name TEXT,
    work_type TEXT,
    work_description TEXT,
    total_fee NUMERIC,
    community_area NUMERIC,
    census_tract NUMERIC,
    xcoordinate NUMERIC,
    ycoordinate NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC
)
''')
conn.commit()

# Create engine and load data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}")
results_df.to_sql('buildings', con=engine, if_exists='replace', index=False)

# Close the connection
conn.close()