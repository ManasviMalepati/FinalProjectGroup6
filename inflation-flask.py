from flask import Flask, request, jsonify, session
import requests
import pandas as pd 
import numpy as np
import psycopg2


app = Flask(__name__)

# establish postgres connection
conn = psycopg2.connect(
    dbname='inflation',
    user='jhu',
    password='jhu123',
    host='postgres',  
    port='5432'
)
cur = conn.cursor()

# @app.before_request
# def make_session_permanent():
#     session.permanent = True
#     app.permanent_session_lifetime = timedelta(minutes=30)


@app.route('/api/get_metros', methods=['GET'])
def get_metros():

    global geo
   
    sql = (
        """
       SELECT z.zillow_geo_name
       , case when z.zillow_geo_name = 'Urban Honolulu, HI' then 'HON' 
            else left(replace(regexp_replace(upper(z.zillow_geo_name),'[^A-Z]', ''), ' ', '') , 3)
            end code
       from zillow_geo z
       join geo_map m 
       on z.zillow_geo_id = m.zillow_geo_id
        """ )
    cur.execute(sql)

    data = cur.fetchall()
    code = []
    name = []
    for d in data:
        code.append(d[1])
        name.append(d[0])

    geo = dict(zip(code, name))

    # return all records as json object
    return jsonify(geo)


@app.route('/api/aggregate_data', methods=['POST'])
def aggregate_data():

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



if __name__ == '__main__':
    geo = {}
    agg = pd.DataFrame()
    port = 8001
    app.secret_key = 'xxxxx'
    app.run(debug=True, port=port)
    



