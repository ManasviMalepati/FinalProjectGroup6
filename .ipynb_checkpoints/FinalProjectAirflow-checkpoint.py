#Final project
#Grou[ 6
import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np

# define the default arguments
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG
with DAG('final_project_group6', default_args=default_args, schedule_interval='@monthly') as dag:

    def load_data():
        conn = psycopg2.connect(
        dbname='inflation',
        user='jhu',
        password='jhu123',
        host='postgres',  
        port='5432'
        )
        ##Retrieving the 
        cur_psql = conn.cursor()
        cur_psql.execute(""" select 
        	bc.year 
        	, bc.month 
        	, bc.cpi_value
        	, bs.series_name
        	, bg.bls_geo_name
        	, z.zillow_rent_index_value
        	, c.avg_household_earnings as acs_annual_earnings_per_household
        	from bls_cpi bc
        	join bls_series bs on bc.series_id = bs.series_id
        	join bls_geo bg on bs.bls_geo_id = bg.bls_geo_id
        	join geo_map g on bg.bls_geo_id = g.bls_geo_id
        	left join census_earnings c 
        		on g.census_geo_id = c.census_geo_id
        		and bc.year = c.year 
        	left join zillow_rent_index z on g.zillow_geo_id = z.zillow_geo_id 
        		and bc.year = z.year 
        		and bc.month = z.month
        	where bc.year >= 2015 
        	and bc.year <= 2022""")
        rows = cur_psql.fetchall()
        consolidated_data = [{'Year':row[0], 'Month': row[1], 'CPI': row[2], 'Series Name': row[3], 'Geo Name': row[4], 'Annual Earnings per Household': row[5]} for row in rows]
        return jsonify(consolidated_data)

    def save_data():
        consolidated_data = load_data()
        consolidated_df = pd.DataFrame(consolidated_data)
        full_path = os.path.join(os.path.dirname(__file__), 'consolidated_data.csv')
        consolidated_df.to_csv(full_path, index=False)


        # Nothing required here for submission - this function is complete

    
    load_data_task = PythonOperator(
            task_id="load_data_task",
            python_callable=load_data
        ) 
    save_data_task = PythonOperator(
            task_id="save_data_task",
            python_callable=save_data
        )  

    
    
    load_data_task >> save_data_task 
    