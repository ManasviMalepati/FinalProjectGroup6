--CREATING SCHEMA (assume JHU database)

CREATE SCHEMA group6Final;

-- CREATING TABLES

CREATE TABLE group6Final.bls_cpi(
	series_id VARCHAR(20), --Restrict?
    month INTEGER,
    year INTEGER,
    cpi_value NUMERIC,
    PRIMARY KEY (series_id, month, year)
);

CREATE TABLE group6Final.bls_series(
	series_id VARCHAR(20),
	bls_geo_id VARCHAR(4), -- AKA area code, all were 4 that were visible. Could just use text or not restrict. 
	series_name TEXT,
	PRIMARY KEY (series_id)
);

CREATE TABLE group6Final.bls_geo(
	bls_geo_id VARCHAR(4),
	bls_geo_name TEXT,
	PRIMARY KEY (bls_geo_id)
);

CREATE TABLE group6Final.zillow_rent_index(
	zillow_geo_id INTEGER,
	month INTEGER,
	year INTEGER, 
	zillow_rent_index_value NUMERIC,
	PRIMARY KEY (zillow_geo_id, month, year)
);

CREATE TABLE group6Final.zillow_geo(
	zillow_geo_id INTEGER, 
	zillow_geo_name TEXT, --cities
	PRIMARY KEY (zillow_geo_id)
);

CREATE TABLE group6Final.census_earnings(
	census_geo_id VARCHAR(20), --called census_geo_name in mapping table
	year INTEGER,
	avg_household_earnings INTEGER,
	avg_household_earnings_with_earnings INTEGER,
	avg_household_earnings_no_earnings INTEGER,
	PRIMARY KEY (census_geo_id, year)
);

CREATE TABLE group6Final.census_geo(
	census_geo_id VARCHAR(20), --called name in geo mapping table
	census_geo_name TEXT  --not the name in the mapping table
	PRIMARY KEY (census_geo_id)
);

CREATE TABLE group6Final.geo_mapping_table(
	bls_geo_id VARCHAR(4),
	zillow_geo_id INTEGER, --If any start with zero this should be text
	census_geo_id VARCHAR(20) --called name in data
	PRIMARY KEY (bls_geo_id)
);

--LOADING DATA (can copy and paste - change table name, columns, and paths)
COPY group6Final.bls_cpi (series_id, month, year, cpi_value)
FROM 'ADD PATH BASED ON NAMES OF FILES STORED'
DELIMITER ','
CSV HEADER;