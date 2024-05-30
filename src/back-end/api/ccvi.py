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

query = """SELECT geography_type \
    ,community_area_or_zip \
    ,community_area_name \
    ,ccvi_score \
    ,ccvi_category  \
    ,rank_socioeconomic_status \
    ,rank_household_composition \
    ,rank_adults_no_pcp \
    ,rank_cumulative_mobility_ratio\
    ,rank_frontline_essential_workers \
    ,rank_age_65_plus \
    ,rank_comorbid_conditions \
    ,rank_covid_19_incidence_rate \
    ,rank_covid_19_hospital_admission_rate \
    ,rank_covid_19_crude_mortality_rate \
    ,location \
    limit 20000 """

results = client.get("xhc6-88s9", query=query)

results_df = pd.DataFrame.from_records(results)

def convert_complex_types(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df

results_df = convert_complex_types(results_df)

def load_data_to_postgresql(df):
    engine = create_engine('postgresql+psycopg2://hs:password1@localhost/postgres')
    df.to_sql('ccvi', con=engine, if_exists='replace', index=False)

conn = psycopg2.connect(
    host="localhost",  
    database="postgres",  
    user="hs", 
    password="password1" 
)

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS ccvi (
    geography_type TEXT,
    community_area_or_zip NUMERIC,
    community_area_name TEXT,
    ccvi_score NUMERIC,
    ccvi_category TEXT,
    rank_socioeconomic_status NUMERIC,
    rank_household_composition NUMERIC,
    rank_adults_no_pcp NUMERIC,
    rank_cumulative_mobility_ratio NUMERIC,
    rank_frontline_essential_workers NUMERIC,
    rank_age_65_plus NUMERIC,
    rank_comorbid_conditions NUMERIC,
    rank_covid_19_incidence_rate NUMERIC, 
    rank_covid_19_hospital_admission_rate NUMERIC,
    rank_covid_19_crude_mortality_rate NUMERIC,
    location JSONB
)
''')
conn.commit()

engine = create_engine('postgresql+psycopg2://hs:password1@localhost:5432/chicago')

load_data_to_postgresql(results_df)

conn.close()