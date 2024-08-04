from flask import Flask, request, jsonify
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
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
        host='localhost' 
    )
    return conn

# Functions for Routes
def get_geo_ids(city_name):
    conn = connect_db()
    query = """
    SELECT bls_geo_id, zillow_geo_id, census_geo_name
    FROM geo_map
    WHERE LOWER(city) LIKE LOWER(?)
    """
    result = pd.read_sql_query(query, conn, params=(city_name,))
    conn.close()
    
    if result.empty:
        return None
    
    # Extract the IDs
    ids = result.iloc[0]
    return {
        'bls_geo_id': ids['bls_geo_id'],
        'zillow_geo_id': ids['zillow_geo_id'],
        'census_geo_id': ids['census_geo_name']
    }

def get_data_from_table(table_name, table_id):
    conn = connect_db()
    query = f"""
    SELECT *
    FROM {table_name}
    WHERE id = ?
    """
    result = pd.read_sql_query(query, conn, params=(table_id,))
    conn.close()
    return result

def get_all_data_for_city(city_name):
    table_ids = get_geo_ids(city_name)
    if table_ids is None:
        return None
    
    data = {
        "bls_cpi": get_data_from_table('bls_cpi', table_ids['bls_geo_id']),
        "census_earnings": get_data_from_table('census_earnings', table_ids['census_geo_id']),
        "zillow_rent_index": get_data_from_table('zillow_rent_index', table_ids['zillow_geo_id'])
    }
    return data
    
#Prompts / Home Route
@app.route('/')
def home():
    return """
    Welcome to the Group 6's Inflation Information API. Here are the options you can use:
    1. /city/<city_name>: Get information about a specific city.
    2. /stats/<parameter>: Get average statistics for a given parameter (cpi, rent_index, household_income).
    3. /trend/<parameter>: Get a graph of trends over time for a given parameter (cpi, rent_index, household_income).
    """

#Routes
@app.route('/city/<city_name>', methods=['GET'])
def get_city_info(city_name):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        data = get_all_data_for_city(city_name)
        if data is None:
            return jsonify({"message": f"No data available for city '{city_name}'."}), 404
    
        # Convert dataframes to JSON
        response_data = {
            "bls_cpi": data["bls_cpi"].to_json(orient='records'),
            "census_earnings": data["census_earnings"].to_json(orient='records'),
            "zillow_rent_index": data["zillow_rent_index"].to_json(orient='records')
        }
        ids = result.iloc[0]
        return jsonify(response_data), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
        
if __name__ == '__main__':
    app.run(debug=True)