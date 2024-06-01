import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Socrata client
client = Socrata("data.cityofchicago.org", None)

# Query data
query = """
SELECT the_geom,
       perimeter,
       area,
       comarea,
       comarea_id,
       area_numbe,
       community,
       area_num_1,
       shape_area,
       shape_len
LIMIT 20000
"""

results = client.get("igwz-8jzy", query=query)

# Convert results to DataFrame
results_df = pd.DataFrame.from_records(results)

#Convert columns to numeric, handling errors
for col in ['perimeter', 'area', 'comarea' ,'comarea_id', 'shape_area', 'shape_len']:
    results_df[col] = pd.to_numeric(results_df[col], errors='coerce')

# Convert complex types to JSON strings
results_df = results_df.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

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
CREATE TABLE IF NOT EXISTS commarea (
       the_geom  TEXT,
       perimeter NUMERIC,
       area      NUMERIC,
       comarea   NUMERIC,
       comarea_id NUMERIC,
       area_numbe TEXT,
       community  TEXT,
       area_num_1 TEXT,
       shape_area NUMERIC,
       shape_len NUMERIC
)
''')
conn.commit()

# Create engine and load data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}")
results_df.to_sql('commarea', con=engine, if_exists='replace', index=False)

# Close the connection
conn.close()