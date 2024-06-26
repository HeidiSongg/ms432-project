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
SELECT 
       geography_type,
       community_area_or_zip,
       community_area_name,
       ccvi_score,
       ccvi_category,
       rank_socioeconomic_status,
       location 
LIMIT 20000
"""

results = client.get("xhc6-88s9", query=query)

# Convert results to DataFrame
results_df = pd.DataFrame.from_records(results)

#Convert columns to numeric, handling errors
for col in ['ccvi_score', 'rank_socioeconomic_status']:
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
CREATE TABLE IF NOT EXISTS ccvi (
    geography_type TEXT,
    community_area_or_zip TEXT,
    community_area_name TEXT,
    ccvi_score NUMERIC,
    ccvi_category TEXT,
    rank_socioeconomic_status NUMERIC,
    location TEXT
)
''')
conn.commit()

# Create engine and load data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}")
results_df.to_sql('ccvi', con=engine, if_exists='replace', index=False)

# Close the connection
conn.close()