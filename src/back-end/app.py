from flask import Flask, jsonify
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)


# PostgreSQL connection configuration
conn = psycopg2.connect(
    dbname= os.getenv('DB_NAME'),
    user= os.getenv('DB_USER'),
    password= os.getenv('DB_PASSWORD'),
    host= os.getenv('DB_HOST'),
    port='5432'
)
cur = conn.cursor()

# Endpoint to execute SQL query and fetch data
@app.route('/api/reqone')
def get_req_one():
    try:
        # SQL query
        query = sql.SQL("""
SELECT
week_number,
community_area,
zipcode,
SUM(trip_cnt) as trip_cnt,
SUM(covid_cases) as covid_cases
FROM
(SELECT
	EXTRACT(WEEK FROM trip_start_timestamp)+1 AS week_number,
	pickup_community_area AS community_area,
	pickup_zip_code		  AS zipcode,
	COUNT(DISTINCT trip_id) AS trip_cnt,
	SUM(cases_weekly) AS covid_cases
FROM
	taxitrip AS tx
JOIN covid c 
ON tx.pickup_zip_code = c.zip_code and tx.trip_start_timestamp >= c.week_start and tx.trip_end_timestamp <= c.week_end 
GROUP BY
	week_number,
	trip_start_timestamp,
	pickup_zip_code,
    pickup_community_area
 
UNION ALL
 
SELECT
	EXTRACT(WEEK FROM trip_start_timestamp)+1 AS week_number,
	dropoff_community_area AS community_area,
	dropoff_zip_code		  AS zipcode,
	COUNT(DISTINCT trip_id) AS trip_cnt,
	SUM(cases_weekly) AS covid_cases
FROM
	taxitrip AS tx
JOIN covid c 
ON tx.dropoff_zip_code = c.zip_code and tx.trip_start_timestamp >= c.week_start and tx.trip_end_timestamp <= c.week_end 
GROUP BY
	week_number,
	trip_start_timestamp,
	dropoff_zip_code,
    dropoff_community_area
)	
GROUP BY
week_number,
community_area,
zipcode
ORDER BY
week_number,
community_area
        """)

        # Execute the query
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'week_number': row[0],
                'community_area': row[1],
                'zipcode': row[2],
                'tripcount': row[3],
                'covid_cases': row[4]
            })

        # Send the fetched data as JSON response
        return jsonify(data)

    except psycopg2.Error as e:
        print('Error executing SQL query:', e)
        return jsonify({'error': 'Internal server error'}), 500
    

@app.route('/api/reqtwo')
def get_req_two():
    try:
        # SQL query
        query = sql.SQL("""
SELECT
dropoff_community_area,
dropoff_zip_code,
SUM(trip_cnt) AS trip_cnt
FROM
(
SELECT
dropoff_community_area,
dropoff_zip_code,
COUNT(DISTINCT trip_id) AS trip_cnt
FROM taxitrip
WHERE pickup_centroid_latitude >= 41.97
AND pickup_centroid_latitude <= 41.98
AND pickup_centroid_longitude <= -87.90
AND pickup_centroid_longitude >= -87.91
GROUP BY 
dropoff_community_area,
dropoff_zip_code
UNION ALL
SELECT
dropoff_community_area,
dropoff_zip_code,
COUNT(DISTINCT trip_id) AS trip_cnt
FROM taxitrip
WHERE pickup_centroid_latitude >= 41.77
AND pickup_centroid_latitude <= 41.80
AND pickup_centroid_longitude <= -87.75
AND pickup_centroid_longitude >= -87.77
GROUP BY 
dropoff_community_area,
dropoff_zip_code
)
GROUP BY 
dropoff_community_area,
dropoff_zip_code
        """)

        # Execute the query
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'dropoff_community_area': row[0],
                'dropoff_zip_code': row[1],
                'trip_cnt': row[2]
            })

        # Send the fetched data as JSON response
        return jsonify(data)

    except psycopg2.Error as e:
        print('Error executing SQL query:', e)
        return jsonify({'error': 'Internal server error'}), 500
    
@app.route('/api/reqthree')
def get_req_three():
    try:
        # SQL query
        query = sql.SQL("""
SELECT
EXTRACT(WEEK FROM trip_start_timestamp)+1 AS week_number,
community_area,
zipcode,
SUM(trip_cnt) trip_count
FROM
(
SELECT
	trip_start_timestamp,
	pickup_community_area AS community_area,
	pickup_zip_code		  AS zipcode,
	COUNT(DISTINCT trip_id) AS trip_cnt
FROM
	taxitrip AS tx
JOIN ccvi AS ca 
ON tx.pickup_community_area = ca.community_area_or_zip and ca.geography_type ='CA'
OR CAST(tx.pickup_zip_code AS numeric)  = ca.community_area_or_zip and ca.geography_type = 'ZIP'
WHERE
	ca.ccvi_category = 'HIGH'
GROUP BY
	trip_start_timestamp,
	pickup_zip_code,
    pickup_community_area
UNION ALL
SELECT
	trip_start_timestamp,
	dropoff_community_area AS community_area,
	dropoff_zip_code	   AS zipcode,
	COUNT(DISTINCT trip_id) AS trip_cnt
FROM
	taxitrip AS tx
JOIN ccvi AS ca 
ON tx.dropoff_community_area = ca.community_area_or_zip and ca.geography_type ='CA'
OR CAST(tx.dropoff_zip_code AS numeric)  = ca.community_area_or_zip and ca.geography_type = 'ZIP'
WHERE
	ca.ccvi_category = 'HIGH'
GROUP BY
	trip_start_timestamp,
	dropoff_community_area,
	dropoff_zip_code
)
GROUP BY 
week_number,
community_area,
zipcode
        """)

        # Execute the query
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'week_number': row[0],
                'community_area': row[1],
                'zipcode': row[2],
                'trip_count': row[3]
            })

        # Send the fetched data as JSON response
        return jsonify(data)

    except psycopg2.Error as e:
        print('Error executing SQL query:', e)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/reqfour')
def get_req_four():
    try:
        # SQL query
        query = sql.SQL("""
SELECT
EXTRACT(WEEK FROM trip_start_timestamp)+1 AS week_number,
pickup_zip_code,
dropoff_zip_code,
count(distinct trip_id) as trip_cnt
FROM taxitrip 
group by 
week_number,
pickup_zip_code,
dropoff_zip_code
        """)

        # Execute the query
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'week_number': row[0],
                'pickup_zip_code': row[1],
                'dropoff_zip_code': row[2],
                'trip_count': row[3]
            })

        # Send the fetched data as JSON response
        return jsonify(data)

    except psycopg2.Error as e:
        print('Error executing SQL query:', e)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/reqfive')
def get_req_five():
    try:
        # SQL query
        query = sql.SQL("""
with topfive as(
(
    SELECT
        community_area,
        community_area_name,
        unemployment,
        below_poverty_level
    FROM
        unemployment
    ORDER BY
        unemployment DESC
    LIMIT 5
)
UNION 
(
    SELECT
        community_area,
        community_area_name,
        unemployment,
        below_poverty_level
    FROM
        unemployment
    ORDER BY
        below_poverty_level DESC
    LIMIT 5
))
SELECT 
        b.community_area,
        community_area_name,
        unemployment,
        below_poverty_level,
		SUM(b.total_fee) as total_fee
FROM buildings b 
JOIN topfive t on b.community_area = t.community_area
GROUP BY 
        b.community_area,
        community_area_name,
        unemployment,
        below_poverty_level

        """)

        # Execute the query
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'community_area': row[0],
                'community_area_name': row[1],
                'unemployment': row[2],
                'below_poverty_level': row[3],
                'total_fee': row[4]
            })

        # Send the fetched data as JSON response
        return jsonify(data)

    except psycopg2.Error as e:
        print('Error executing SQL query:', e)
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/reqsix')
def get_req_six():
    try:
        # SQL query
        query = sql.SQL("""
WITH permit_cnt AS (
SELECT
b.community_area,
u.community_area_name,
COUNT(DISTINCT permit_) AS permit_count
FROM buildings b
JOIN unemployment u on b.community_area = u.community_area
WHERE permit_type = 'PERMIT - NEW CONSTRUCTION'
AND u.per_capita_income < 30000
GROUP BY
b.community_area,
u.community_area_name
ORDER BY permit_count
)
SELECT
community_area,
community_area_name,
permit_count
FROM permit_cnt
WHERE permit_count = (SELECT MIN(permit_count) FROM permit_cnt)
        """)

        # Execute the query
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Convert rows to list of dictionaries
        data = []
        for row in rows:
            data.append({
                'community_area': row[0],
                'community_area_name': row[1],
                'permit_count': row[2]
            })

        # Send the fetched data as JSON response
        return jsonify(data)

    except psycopg2.Error as e:
        print('Error executing SQL query:', e)
        return jsonify({'error': 'Internal server error'}), 500

# Start the server
if __name__ == '__main__':
    app.run(debug=True)
