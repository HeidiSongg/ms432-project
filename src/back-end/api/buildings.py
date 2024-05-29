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

results = client.get("ydr8-5enu", limit=10)

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
    contact_2_type TEXT,
    contact_2_name TEXT,
    contact_2_city TEXT,
    contact_2_state TEXT,
    contact_2_zipcode TEXT,
    contact_3_type TEXT,
    contact_3_name TEXT,
    contact_3_city TEXT,
    contact_3_state TEXT,
    contact_3_zipcode TEXT,
    contact_4_type TEXT,
    contact_4_name TEXT,
    contact_4_city TEXT,
    contact_4_state TEXT,
    contact_4_zipcode TEXT,
    contact_5_type TEXT,
    contact_5_name TEXT,
    contact_5_city TEXT,
    contact_5_state TEXT,
    contact_5_zipcode TEXT,
    contact_6_type TEXT,
    contact_6_name TEXT,
    contact_6_city TEXT,
    contact_6_state TEXT,
    contact_6_zipcode TEXT,
    contact_7_type TEXT,
    contact_7_name TEXT,
    contact_7_city TEXT,
    contact_7_state TEXT,
    contact_7_zipcode TEXT,
    contact_8_type TEXT,
    contact_8_name TEXT,
    contact_8_city TEXT,
    contact_8_state TEXT,
    contact_8_zipcode TEXT,
    contact_9_type TEXT,
    contact_9_name TEXT,
    contact_9_city TEXT,
    contact_9_state TEXT,
    contact_9_zipcode TEXT,
    contact_10_type TEXT,
    contact_10_name TEXT,
    contact_10_city TEXT,
    contact_10_state TEXT,
    contact_10_zipcode TEXT,
    contact_11_type TEXT,
    contact_11_name TEXT,
    contact_11_city TEXT,
    contact_11_state TEXT,
    contact_11_zipcode TEXT,
    contact_12_type TEXT,
    contact_12_name TEXT,
    contact_12_city TEXT,
    contact_12_state TEXT,
    contact_12_zipcode TEXT,
    contact_13_type TEXT,
    contact_13_name TEXT,
    contact_13_city TEXT,
    contact_13_state TEXT,
    contact_13_zipcode TEXT,
    contact_14_type TEXT,
    contact_14_name TEXT,
    contact_14_city TEXT,
    contact_14_state TEXT,
    contact_14_zipcode TEXT,
    contact_15_type TEXT,
    contact_15_name TEXT,
    contact_15_city TEXT,
    contact_15_state TEXT,
    contact_15_zipcode TEXT,
    reported_cost NUMERIC,
    pin_list TEXT,
    pin1 TEXT,
    pin2 TEXT,
    pin3 TEXT,
    pin4 TEXT,
    pin5 TEXT,
    pin6 TEXT,
    pin7 TEXT,
    pin8 TEXT,
    pin9 TEXT,
    pin10 TEXT,
    community_area NUMERIC,
    census_tract NUMERIC,
    ward NUMERIC,
    xcoordinate NUMERIC,
    ycoordinate NUMERIC,
    latitude NUMERIC,
    longitude NUMERIC,
    location POINT
)
''')
conn.commit()

engine = create_engine('postgresql+psycopg2://hs:password1@localhost:5432/chicago')

load_data_to_postgresql(results_df)