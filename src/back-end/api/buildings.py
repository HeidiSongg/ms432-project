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

query = """SELECT id \
    ,permit_ \
    ,permit_status \
    ,permit_milestone \
    ,permit_type \
    ,review_type \
    ,application_start_date \
    ,issue_date \
    ,processing_time \
    ,street_number \
    ,street_direction \
    ,street_name \
    ,work_type \
    ,work_description \
    ,building_fee_paid \
    ,zoning_fee_paid \
    ,other_fee_paid \
    ,subtotal_paid \
    ,building_fee_unpaid \
    ,zoning_fee_unpaid \
    ,other_fee_unpaid \
    ,subtotal_unpaid \
    ,building_fee_waived \
    ,building_fee_subtotal \
    ,zoning_fee_subtotal \
    ,other_fee_subtotal \
    ,zoning_fee_waived \
    ,other_fee_waived \
    ,subtotal_waived \
    ,total_fee \
    ,contact_1_type \
    ,contact_1_name \
    ,contact_1_city \
    ,contact_1_state \
    ,contact_1_zipcode \
    ,reported_cost \
    ,pin1 \
    ,community_area \
    ,census_tract \
    ,ward \
    ,xcoordinate \
    ,ycoordinate \
    ,latitude \
    ,longitude \
    limit 20000 """

results = client.get("ydr8-5enu", query=query)

results_df = pd.DataFrame.from_records(results)

def convert_complex_types(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

results_df = convert_complex_types(results_df)

def load_data_to_postgresql(df):
    engine = create_engine('postgresql+psycopg2://hs:password1@localhost/postgres')
    df.to_sql('buildings', con=engine, if_exists='replace', index=False)

conn = psycopg2.connect(
    host="localhost",  
    database="postgres",  
    user="hs", 
    password="password1" 
)

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS buildings (
    id TEXT,
    permit_ TEXT,
    permit_status TEXT,
    permit_milestone TEXT,
    permit_type TEXT,
    review_type TEXT,
    application_start_date TIMESTAMPTZ,
    issue_date TIMESTAMPTZ,
    processing_time NUMERIC,
    street_number NUMERIC,
    street_direction TEXT,
    street_name TEXT,
    work_type TEXT,
    work_description TEXT,
    building_fee_paid NUMERIC,
    zoning_fee_paid NUMERIC,
    other_fee_paid NUMERIC,
    subtotal_paid NUMERIC,
    building_fee_unpaid NUMERIC,
    zoning_fee_unpaid NUMERIC,
    other_fee_unpaid NUMERIC,
    subtotal_unpaid NUMERIC,
    building_fee_waived NUMERIC,
    building_fee_subtotal NUMERIC,
    zoning_fee_subtotal NUMERIC,
    other_fee_subtotal NUMERIC,
    zoning_fee_waived NUMERIC,
    other_fee_waived NUMERIC,
    subtotal_waived NUMERIC,
    total_fee NUMERIC,
    contact_1_type TEXT,
    contact_1_name TEXT,
    contact_1_city TEXT,
    contact_1_state TEXT,
    contact_1_zipcode TEXT,
    reported_cost NUMERIC,
    pin1 TEXT,
    community_area NUMERIC,
    census_tract NUMERIC,
    ward NUMERIC,
    xcoordinate NUMERIC,
    ycoordinate NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC
)
''')
conn.commit()

engine = create_engine('postgresql+psycopg2://hs:password1@localhost:5432/chicago')

load_data_to_postgresql(results_df)

conn.close()
