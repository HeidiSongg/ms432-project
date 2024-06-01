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
       zip_code,
       week_number,
       week_start,
       week_end,
       cases_weekly
LIMIT 20000
"""

results = client.get("yhhz-zm2v", query=query)

# Convert results to DataFrame
results_df = pd.DataFrame.from_records(results)

#Convert columns to numeric, handling errors
for col in ['week_number', 'cases_weekly']:
    results_df[col] = pd.to_numeric(results_df[col], errors='coerce')

for col in ['week_start','week_end']:
    results_df[col] = pd.to_datetime(results_df[col], errors='coerce')

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
CREATE TABLE IF NOT EXISTS covid (
    zip_code TEXT,
    week_number NUMERIC,
    week_start TIMESTAMPTZ,
    week_end TIMESTAMPTZ,
    cases_weekly NUMERIC
)
''')
conn.commit()

# Create engine and load data to PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['dbname']}")
results_df.to_sql('covid', con=engine, if_exists='replace', index=False)

# Close the connection
conn.close()