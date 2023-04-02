-- SETUP OF SNOWFLAKE ENVIRONMENT


-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ------------------------------------------------------------------------------------------


USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE WEATHER_ROLE;
GRANT ROLE WEATHER_ROLE TO ROLE SYSADMIN;
GRANT ROLE WEATHER_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE WEATHER_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE WEATHER_ROLE;

-- Databases
CREATE OR REPLACE DATABASE CR_WEATHER;
GRANT OWNERSHIP ON DATABASE CR_WEATHER TO ROLE WEATHER_ROLE;


-- Warehouses
CREATE OR REPLACE WAREHOUSE WEATHER_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 60, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE WEATHER_ROLE;
USE WAREHOUSE WEATHER_WH;
USE DATABASE CR_WEATHER;

CREATE OR REPLACE SCHEMA WEATHER;

USE SCHEMA WEATHER;

CREATE TABLE IF NOT EXISTS RAW_WEATHER_STAGE (
    data variant
);


CREATE TABLE IF NOT EXISTS FINAL_WEATHER (
    date timestamp,
    city_id number,
    city_name text,
    country text,
    temp float,
    feels_like float,
    temp_min float,
    temp_max float,
    pressure float,
    humidity float,
    weather_main text,
    weather_description text,
    clouds float,
    wind_speed float,
    visibility float,
    coord_lon float,
    coord_lat float
);


CREATE STREAM IF NOT EXISTS RAW_WEATHER_STAGE_STREAM ON TABLE RAW_WEATHER_STAGE;

CREATE STAGE IF NOT EXISTS WEATHER;

