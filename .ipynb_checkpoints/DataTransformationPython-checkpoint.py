import psycopg2
from psycopg2 import sql
import pandas as pd
import numpy as np


### LOAD SOURCE DATA TO PYTHON

# The following data needs to be in data folder:
# - bls data: ap.data.0.Current, ap.series, ap.area
# - zillow data: metro_zori_uc_sfrcondomfr_sm_month.csv
# - census data: ACSDT1Y2015.B19051-Data.csv, ACSDT1Y2016.B19051-Data.csv, ACSDT1Y2017.B19051-Data.csv, ACSDT1Y2018.B19051-Data.csv, ACSDT1Y2019.B19051-Data.csv, ACSDT1Y2021.B19051-Data.csv, ACSDT1Y2022.B19051-Data.csv
# - mapping file: geo_map.csv


# BLS DATA

# read bls cpi data  
source_bls_cpi = pd.read_csv( "/home/jhu/FinalProjectGroup6/CPIData/ap.data.0.Current", sep='\t',
            dtype =  { 'series_id': str, 'year': int, 'period': str, 'value': float, 'footnote_codes': str } )

# read bls series (product types) attribute data 
source_bls_series = pd.read_csv( "/home/jhu/FinalProjectGroup6/CPIData/ap.series", sep='\t' )

# read bls geo attribute data
source_bls_geo = pd.read_csv( "/home/jhu/FinalProjectGroup6/CPIData/ap.area", sep='\t' )


# ZILLOW DATA 

# read zillow data
source_zillow = pd.read_csv( "/home/jhu/FinalProjectGroup6/ZORIData/metro_zori_uc_sfrcondomfr_sm_month.csv" )

# CENSUS EARNINGS DATA 

# create data frame to lead each census year into
source_census = pd.DataFrame()
columns = ["geography", "geographic_area_name", 
           "estimate_total", "margin_of_error_total",
           "estimate_total_with_earnings", "margin_of_error_total_with_earnings",
           "estimate_total_no_earnings", "margin_of_error_total_no_earnings", 
           "na"]
           

# loop through available years of data
census_years = [ 2015, 2016, 2017, 2018, 2019, 2021, 2022 ]
for year in census_years: 
    # read in file
    census_temp = pd.read_csv( "/home/jhu/FinalProjectGroup6/CensusEarningsData/ACSDT1Y"+str(year)+".B19051-Data.csv", header=1 )
    # rename columns for consistency when appending
    census_temp.columns = columns 
    # add year column
    census_temp["year"] = year 
    # insert into df to union all data files 
    source_census = pd.concat( [source_census, census_temp], ignore_index = True )

# read in geo_map, no modifications need
source_geo_map = pd.read_csv( "/home/jhu/FinalProjectGroup6/geo_map.csv")

### CLEAN AND TRANSORM DATA TO BE LOADED TO DATABASE

# BLS CPI DATA 

print("hellow")
# copy source data 
bronze_bls_cpi = source_bls_cpi.copy()

# rename columns to remove white spaces 
bronze_bls_cpi.columns = ["series_id", "year", "period", "cpi_value", "footnotes"]

# convert period to month number
bronze_bls_cpi["month"] = bronze_bls_cpi["period"].apply( lambda x: int(x.replace("M", "")))

#strip white spaces from series_id
bronze_bls_cpi['series_id'] = bronze_bls_cpi['series_id'].str.strip()

# convert cpi_value to numeric 
bronze_bls_cpi['cpi_value'] = pd.to_numeric(bronze_bls_cpi['cpi_value'], errors="coerce")

# remove unnecessary columns
bronze_bls_cpi = bronze_bls_cpi[["series_id", "year", "month", "cpi_value"]]

# filling na values with 0 for cpi value
bronze_bls_cpi = bronze_bls_cpi.fillna({'cpi_value':0})


# BLS SERIES DATA 

# update column names to get rid of additional spaces
# rename columns for database consistency 
source_bls_series.columns = ["series_id", "bls_geo_id", "item_code", "series_name", "footnotes", 
                             "begin_year", "begin_period", "end_year", "end_period"]

# strip white spaces from series_id
source_bls_series['series_id'] = source_bls_series['series_id'].str.strip()

# remove unnecessary columns 
bronze_bls_series = source_bls_series[["series_id", "series_name", "bls_geo_id"]]

# BLS GEO DATA 

# update column names for database consistency
bronze_bls_geo = source_bls_geo
bronze_bls_geo.columns = ["bls_geo_id", "bls_geo_name"]


# ZILLOW ZORI DATA

# upivot dates to columns 
zillow_columns =  source_zillow.columns
zillow_dims = ["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]
zillow_dates = [c for c in zillow_columns if c not in zillow_dims]
int_zillow_unpiv = pd.melt(source_zillow, id_vars=zillow_dims, value_vars=zillow_dates)

# keep only msa records
int_zillow_unpiv = int_zillow_unpiv[int_zillow_unpiv["RegionType"]=="msa"]

# rename columns for database consistency
int_zillow_unpiv.columns = ["zillow_geo_id", "size_rank", "zillow_geo_name", "geo_type", "state_name", "date", "zillow_rent_index_value"]

# extract year and month from date
int_zillow_unpiv["year"]=int_zillow_unpiv["date"].str[:4].apply(lambda x: int(x))
int_zillow_unpiv["month"]=int_zillow_unpiv["date"].str[:7].str[-2:].apply(lambda x: int(x))

# normalize data by splitting values and attributes into two tables
# exclude unnecessary/duplicative data
bronze_zillow_geo = int_zillow_unpiv[["zillow_geo_id", "zillow_geo_name"]].drop_duplicates()
bronze_zillow_rent_index = int_zillow_unpiv[["zillow_geo_id", "year", "month", "zillow_rent_index_value"]]

# filling rent index null values with 0
bronze_zillow_rent_index = bronze_zillow_rent_index.fillna({'zillow_rent_index_value':0})


# CENSUS EARNINGS DATA

#remove uncessary/duplicative columns 
int_census = source_census[["geography", "geographic_area_name", "year",
             "estimate_total", "estimate_total_with_earnings", "estimate_total_no_earnings"]]

#rename columns for database consistency
int_census.columns = ["census_geo_id", "census_geo_name", "year", "avg_household_earnings", 
            "avg_household_earnings_with_earnings", "avg_household_earnings_no_earnings" ]


#exclude records that are not metro areas 
int_census = int_census[int_census["census_geo_name"].str.contains("Metro Area")]

# normalize data by splitting values and attributes into two tables
bronze_census_geo = int_census[["census_geo_id", "census_geo_name"]].drop_duplicates()
bronze_census_earnings = int_census[["census_geo_id", "year", "avg_household_earnings", 
            "avg_household_earnings_with_earnings", "avg_household_earnings_no_earnings" ]]

bronze_census_earnings = bronze_census_earnings.fillna({'avg_household_earnings':0,'avg_household_earnings_with_earnings':0, 'avg_household_earnings_no_earnings':0})

# GEO MAP 
# remove geo name to normalize
bronze_geo_map = source_geo_map.drop(columns=["bls_geo_name"])


### CREATE DATABASE

# establish postgres connection to existing database
conn = psycopg2.connect(
    dbname='jhu',
    user='jhu',
    password='jhu123',
    host='postgres',  
    port='5432'
)
cur = conn.cursor()
conn.autocommit = True

# createn new database within python
cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'inflation'")
if not cur.fetchone():    # if not exists
    cur.execute(sql.SQL('CREATE DATABASE {};').format(sql.Identifier("inflation")))
conn.close()

# connect to newly created inflation database
conn = psycopg2.connect(
    dbname='inflation',
    user='jhu',
    password='jhu123',
    host='postgres',  
    port='5432'
)
cur = conn.cursor()
conn.autocommit = True


### INSERT DATA 

# BLS_CPI 
# create bls_cpi posgtres table
cur.execute("DROP TABLE IF EXISTS bls_cpi;")
sql = (
        """
        CREATE TABLE bls_cpi (
            id SERIAL PRIMARY KEY,
            series_id VARCHAR(13) NOT NULL,
            year INT NOT NULL, 
            month INT NOT NULL,
            cpi_value NUMERIC
        )
        """ )
cur.execute(sql)

# insert data into table
# starting index from 1 instead of 0 for SQL ingestion
bronze_bls_cpi.index = bronze_bls_cpi.index + 1 
bronze_bls_cpi.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_bls_cpi_transformed.csv")

sql = (
        """
        COPY bls_cpi 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_bls_cpi_transformed.csv' CSV HEADER NULL 'NA';
        """ )
cur.execute(sql)
# records = list(bronze_bls_cpi.itertuples(index=False, name=None))


# sql = (
#         """
#         INSERT INTO bls_cpi (
#             series_id, year, month, cpi_value
#         ) VALUES (%s, %s, %s, %s)
#         """ )
# cur.executemany(sql, records)

# BLS SERIES
# create bls_series posgtres table
cur.execute("DROP TABLE IF EXISTS bls_series;")
sql = (
        """
        CREATE TABLE bls_series (
            series_id VARCHAR(13) PRIMARY KEY,
            series_name VARCHAR(150) NOT NULL, 
            bls_geo_id VARCHAR(4) NOT NULL
        )
        """ )
cur.execute(sql)

bronze_bls_series.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bls_series_transformed.csv", index = False)

sql = (
        """
        COPY bls_series 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bls_series_transformed.csv' CSV HEADER NULL 'NA';
        """ )
cur.execute(sql)
# # insert data into bls_series table
# records = list(bronze_bls_series.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO bls_series (
#             series_id, series_name, bls_geo_id
#         ) VALUES (%s, %s, %s)
#         """ )
# cur.executemany(sql, records)

# BLS GEO

# create bls_geo posgtres table
cur.execute("DROP TABLE IF EXISTS bls_geo;")
sql = (
        """
        CREATE TABLE bls_geo (
            bls_geo_id VARCHAR(4) PRIMARY KEY,
            bls_geo_name VARCHAR(50) NOT NULL 
        )
        """ )
cur.execute(sql)

# insert data into bls_geo table
# records = list(bronze_bls_geo.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO bls_geo (
#             bls_geo_id, bls_geo_name
#         ) VALUES (%s, %s)
#         """ )
# cur.executemany(sql, records)

bronze_bls_geo.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_bls_geo_transformed.csv", index = False)

sql = (
        """
        COPY bls_geo 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_bls_geo_transformed.csv' CSV HEADER NULL 'NA';
        """ )

cur.execute(sql)

# ZILLOW GEO

# create zillow_geo
cur.execute("DROP TABLE IF EXISTS zillow_geo;")
sql = (
        """
        CREATE TABLE zillow_geo (
            zillow_geo_id INT PRIMARY KEY,
            zillow_geo_name VARCHAR(25) NOT NULL 
        )
        """ )
cur.execute(sql)

# insert data into zillow_geo table
# records = list(bronze_zillow_geo.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO zillow_geo (
#             zillow_geo_id, zillow_geo_name
#         ) VALUES (%s, %s)
#         """ )
# cur.executemany(sql, records)

bronze_zillow_geo.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_zillow_geo_transformed.csv", index = False)

sql = (
        """
        COPY zillow_geo 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_zillow_geo_transformed.csv' CSV HEADER NULL 'NA';
        """ )
cur.execute(sql)

# ZILLOW RENT INDEX

# create zillow_rent_index
cur.execute("DROP TABLE IF EXISTS zillow_rent_index;")
sql = (
        """
        CREATE TABLE zillow_rent_index (
            id SERIAL PRIMARY KEY,
            zillow_geo_id INT NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            zillow_rent_index_value NUMERIC
        )
        """ )
cur.execute(sql)

# insert data into zillow_rent_index table
# records = list(bronze_zillow_rent_index.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO zillow_rent_index (
#             zillow_geo_id, year, month, zillow_rent_index_value
#         ) VALUES (%s, %s, %s, %s)
#         """ )
# cur.executemany(sql, records)
bronze_zillow_rent_index.index = bronze_zillow_rent_index.index+1
bronze_zillow_rent_index.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_zillow_rent_index_transformed.csv")

sql = (
        """
        COPY zillow_rent_index 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_zillow_rent_index_transformed.csv' CSV HEADER NULL 'NA';
        """ )

cur.execute(sql)


# CENSUS GEO

# create census_geo
cur.execute("DROP TABLE IF EXISTS census_geo;")
sql = (
        """
        CREATE TABLE census_geo (
            census_geo_id VARCHAR(14) PRIMARY KEY,
            census_geo_name VARCHAR(60) NOT NULL 
        )
        """ )
cur.execute(sql)

# insert data into census_geo
# records = list(bronze_census_geo.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO census_geo (
#             census_geo_id, census_geo_name
#         ) VALUES (%s, %s)
#         """ )
# cur.executemany(sql, records)
bronze_census_geo.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_census_geo_transformed.csv", index = False)

sql = (
        """
        COPY census_geo 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_census_geo_transformed.csv' CSV HEADER NULL 'NA';
        """ )
cur.execute(sql)
# CENSUS EARNINGS
# create census_earnings
cur.execute("DROP TABLE IF EXISTS census_earnings;")
sql = (
        """
        CREATE TABLE census_earnings (
            id SERIAL PRIMARY KEY,
            census_geo_id VARCHAR(14) NOT NULL,
            year INT NOT NULL,
            avg_household_earnings INT, 
            avg_household_earnings_with_earnings INT, 
            avg_household_earnings_no_earnings INT
        )
        """ )
cur.execute(sql)
# insert data into census_earnings
# records = list(bronze_census_earnings.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO census_earnings (
#             census_geo_id, year, avg_household_earnings,
#             avg_household_earnings_with_earnings, 
#             avg_household_earnings_no_earnings
#         ) VALUES (%s, %s, %s, %s, %s)
#         """ )
# cur.executemany(sql, records)

#Changing the index to start from 1 instead of 0 to ingest into SQL
bronze_census_earnings.index = bronze_census_earnings.index+1
bronze_census_earnings.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_census_earnings_transformed.csv")

sql = (
        """
        COPY census_earnings 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_census_earnings_transformed.csv' CSV HEADER NULL 'NA';
        """ )

cur.execute(sql)

# GEO MAP
# create geo_map
cur.execute("DROP TABLE IF EXISTS geo_map;")
sql = (
        """
        CREATE TABLE geo_map (
            bls_geo_id VARCHAR(4) PRIMARY KEY,
            zillow_geo_id  INT NOT NULL,
            census_geo_id VARCHAR(14) NOT NULL
        )
        """ )
cur.execute(sql)
# insert data into geo_map
# records = list(bronze_geo_map.itertuples(index=False, name=None))

# sql = (
#         """
#         INSERT INTO geo_map (
#             bls_geo_id, zillow_geo_id, census_geo_id
#         ) VALUES (%s, %s, %s)
#         """ )
# cur.executemany(sql, records)
bronze_geo_map.rename(columns  = {'census_geo_name':'census_geo_id'}, inplace = True)
bronze_geo_map.to_csv("/home/jhu/FinalProjectGroup6/TransformedData/bronze_geo_map_transformed.csv", index = False)
sql = (
        """
        COPY geo_map 
        FROM '/home/jhu/FinalProjectGroup6/TransformedData/bronze_geo_map_transformed.csv' CSV HEADER NULL 'NA';
        """ )
cur.execute(sql)








