from flask import Flask, request, jsonify
import numpy as np
import json
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql

app = Flask(__name__)

#Store User Info:
data_collection = {}

#Connect to Database:
def connect_db():
    conn = psycopg2.connect(
        dbname='inflation',
        user='jhu',
        password='jhu123',
        host='postgres',
        port = '5432'
    )
    cur = conn.cursor()
    return conn, cur
    
#Prompts / Home Route
@app.route('/')
def home():
    return """
    Welcome to Group 6's Inflation Information API. Here are the options you can use:
    1. /api/aggregate_data: Aggregate most recent data. (Run first!)
    2. /api/get_metros: Returns a list of searchable metros. 
    3. /api/get_means: Returns means for city identified in input. See example. 
    4. /api/get_correlations: Returns correlations on means for two of the following metrics: 'zillow_rent_index_value', 'avg_household_earnings', 'unleaded_gas_cpi', 'electricity_cpi', 'utility_gas_cpi'
    5. /api/get_earnings_data : Takes a city_name: {city} and returns all earnings data available for that city. 
    """

#Routes
@app.route('/api/get_metros', methods=['GET'])
def get_metros():
    conn, cursor = connect_db()  # Unpack the tuple
    global geo 
    
    if not conn or not cursor:
        return jsonify({'error': 'Failed to connect to the database'}), 500

    try:
        query = """
        SELECT z.zillow_geo_name,
               CASE WHEN z.zillow_geo_name = 'Urban Honolulu, HI' THEN 'HON' 
                    ELSE LEFT(REPLACE(REGEXP_REPLACE(UPPER(z.zillow_geo_name), '[^A-Z]', 
                    ''), ' ', ''), 3)
               END AS code
        FROM zillow_geo z
        JOIN geo_map m ON z.zillow_geo_id = m.zillow_geo_id
        """
        cursor.execute(query)
        data = cursor.fetchall()
        conn.close()

        if not data:
            return jsonify({'message': 'No data found'}), 404

        geo = {d[1]: d[0] for d in data}

        return jsonify(geo)
    except Exception as e:
        print(f"Exception occurred: {e}")  # Print the exception for debugging
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/aggregate_data', methods=['POST'])
def aggregate_data():
    conn, cur = connect_db()
    global agg
   
    sql = (
        """
    	   SELECT bc.year, bc.month
       , zg.zillow_geo_name as metro_name
       , z.zillow_rent_index_value
	   , c.avg_household_earnings
       , avg( case when bs.series_name like '%Gasoline, unleaded regular%' then bc.cpi_value end ) as unleaded_gas_cpi
	   , avg( case when bs.series_name like '%Electricity%' then bc.cpi_value end) as electricity_cpi
	   , avg( case when bs.series_name ilike '%Utility (piped) gas%' then bc.cpi_value end ) as utility_gas_cpi
       from bls_cpi bc
       join bls_series bs on bc.series_id = bs.series_id
       join geo_map g on g.bls_geo_id = bs.bls_geo_id
       join zillow_geo zg on g.zillow_geo_id = zg.zillow_geo_id
       join zillow_rent_index z 
           on g.zillow_geo_id = z.zillow_geo_id
           and bc.year = z.year 
           and bc.month = z.month
        join census_earnings c 
            on g.census_geo_id = c.census_geo_id
            and bc.year = z.year
		group by bc.year, bc.month
		   , zg.zillow_geo_name, z.zillow_rent_index_value, c.avg_household_earnings
        """ )
    cur.execute(sql)

    data = cur.fetchall()
    agg = pd.DataFrame(data, columns=['year', 'month', 'metro_name', 'zillow_rent_index_value', 'avg_household_earnings', 'unleaded_gas_cpi', 'electricity_cpi', 'utility_gas_cpi'])

    message = { 'message' : 'data successfully aggregated' }
    return jsonify(message)
    
@app.route('/api/get_means', methods=['GET'])
def get_means():
    global geo 
    global agg

    years = list(agg['year'])
    
    # route accepts json object containing wiki topic
    data = request.json
    metro_code = data['metro']
    year = data['year']
    
    if (len(agg) == 0):
        results = { 'Message' : 'Data not loaded' }
    elif (geo.get(metro_code) is None): 
         results = { 'Message' : 'Metro not found' }
    elif (years.count(year) == 0):
         results = { 'Message' : 'Year not found' }
    else:
        metro = geo.get(metro_code)
        subset = agg [ (agg['metro_name'] == metro) 
            & (agg['year'] == year) ]
        results = { 
            'Metro' : metro,
            'Year' : year, 
            'Zillow Rent Index' : np.round(np.mean( subset['zillow_rent_index_value'] ), 2), 
             'Avg Household Earnings' : np.round(np.mean( subset['avg_household_earnings'] ), 0), 
             'Unleaded Gasoline CPI' : np.round(np.mean( subset['unleaded_gas_cpi'] ), 2),
            'Electricity CPI' : np.round(np.mean( subset['electricity_cpi'] ), 2),
            'Utility Gas CPI' : np.round(np.mean( subset['utility_gas_cpi'] ), 2)
        }
    return jsonify(results)

@app.route('/api/get_correlations', methods=['GET'])
def get_correlations():

    global agg
    
    # route accepts json object containing wiki topic
    data = request.json
    metric1 = data['metric1']
    metric2 = data['metric2']
    cols = list(agg.columns)
    
    if (len(agg) == 0):
        results = { 'Message' : 'Data not loaded' }
    elif ( cols.count(metric1) == 0 ):
        results = { 'Message' : 'Metric 1 not found' }
    elif ( cols.count(metric2) == 0 ):
        results = { 'Message' : 'Metric 2 not found' }
    else:
        
        results = { 
            'Metrics' : metric1 + ' ~ ' + metric2,
            'Correlation' : agg[metric1].corr(agg[metric2])
        }
    
    return jsonify(results)

@app.route('/api/get_earnings_data', methods=['POST'])
def get_earnings():
    data = request.json
    city_name = data.get('city_name')
    
    if not city_name:
        return jsonify({'error': 'City name not provided'}), 400

    conn, cursor = connect_db()

    # Retrieve all census_geo_id values for the city
    query = """
    SELECT census_geo_id
    FROM census_geo
    WHERE LOWER(census_geo_name) ILIKE %s
    """
    cursor.execute(query, (f'%{city_name}%',))
    census_geo_ids = cursor.fetchall()

    
    if not census_geo_ids:
        conn.close()
        return jsonify({'error': 'No census_geo_id found for the city'}), 404

    # Flatten to list
    census_geo_ids = [id[0] for id in census_geo_ids]

    # Retrieve census earnings for all census_geo_ids
    query = """
    SELECT year, avg_household_earnings
    FROM census_earnings
    WHERE census_geo_id = ANY(%s)
    """
    cursor.execute(query, (census_geo_ids,))
    earnings = cursor.fetchall()
    
    conn.close()

    if not earnings:
        return jsonify({'error': 'No earnings data found for the city'}), 404

    # Format the data for JSON response
    earnings_data = [{'year': row[0], 'avg_household_earnings': row[1]} for row in earnings]

    return jsonify(earnings_data)
    
if __name__ == '__main__':
     print("Flask running on port 8001")
     app.run(debug=True, host='0.0.0.0', port=8001)