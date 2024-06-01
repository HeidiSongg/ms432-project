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
def get_chart_data():
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
    


# Start the server
if __name__ == '__main__':
    app.run(port=3001)
