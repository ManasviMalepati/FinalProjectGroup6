#Final project
#Grou[ 6
import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np
import papermill as pm
from airflow.operators.papermill_operator import PapermillOperator



# define the default arguments
default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 4, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG
with DAG('final_project_group6', default_args=default_args, schedule_interval='@monthly') as dag:

    def joined_data():
        conn = psycopg2.connect(
        dbname='inflation',
        user='jhu',
        password='jhu123',
        host='postgres',  
        port='5432'
        )
        ##Retrieving the 
        cur_psql = conn.cursor()
        sql = (
        """
        SELECT bc.year, bc.month,
        zg.zillow_geo_name as metro_name
       , z.zillow_rent_index_value
	   , c.avg_household_earnings
       , avg( case when bs.series_name like (%s) then bc.cpi_value end ) as unleaded_gas_cpi
	   , avg( case when bs.series_name like (%s) then bc.cpi_value end) as electricity_cpi
	   , avg( case when bs.series_name ilike (%s) then bc.cpi_value end ) as utility_gas_cpi
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
        """)
        cur_psql.execute(sql, ('%Gasoline, unleaded regular%','%Electricity%','%Utility (piped) gas%'))

        rows = cur_psql.fetchall()
        consolidated_data = [{'year':row[0], 'month': row[1], 'metro_name': row[2], 'zillow_rent_index_value': row[3], 'avg_household_earnings': row[4], 'unleaded_gas_cpi': row[5], 'electricity_cpi':row[6],'utility_gas_cpi':row[7]} for row in rows]
        consolidated_df = pd.DataFrame(consolidated_data)
        consolidated_df.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/JoinedData.csv", index=False)
        

    load_data_task = BashOperator(
            task_id='load_data_task',
            bash_command='python3 /home/jhu/FinalProjectGroup6/DataTransformationPython.py'
    )
    joined_data_task = PythonOperator(
            task_id="joined_data_task",
            python_callable=joined_data
        ) 

    create_api_task = BashOperator(
            task_id='create_api_task',
            bash_command='python3 /home/jhu/FinalProjectGroup6/Group6API.py'
    )
    
    load_data_task >> joined_data_task >> create_api_task

