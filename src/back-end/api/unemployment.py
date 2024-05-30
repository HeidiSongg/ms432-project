#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata
from sqlalchemy import create_engine
import psycopg2
import json

client = Socrata("data.cityofchicago.org", None)

query = """SELECT community_area \
    ,community_area_name \
    ,below_poverty_level \
    ,per_capita_income \
    ,unemployment  \
    limit 20000 """

results = client.get("iqnk-2tcu", query=query)

results_df = pd.DataFrame.from_records(results)

def convert_complex_types(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

results_df = convert_complex_types(results_df)

def load_data_to_postgresql(df):
    engine = create_engine('postgresql+psycopg2://hs:password1@localhost/postgres')
    df.to_sql('unemployment', con=engine, if_exists='replace', index=False)

conn = psycopg2.connect(
    host="localhost",  
    database="postgres",  
    user="hs", 
    password="password1" 
)

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS unemployment (
    community_area TEXT,
    community_area_name TEXT,
    below_poverty_level NUMERIC,
    per_capita_income NUMERIC,
    unemployment NUMERIC
)
''')
conn.commit()

engine = create_engine('postgresql+psycopg2://hs:password1@localhost:5432/chicago')

load_data_to_postgresql(results_df)

conn.close()
