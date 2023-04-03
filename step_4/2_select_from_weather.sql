--UPDATE WEATHER TABLE AND SELECT DATA
USE WAREHOUSE WEAThER_WH;
USE SCHEMA CR_WEATHER.WEATHER;
USE ROLE WEATHER_ROLE;

-- ----------------------------------------------------------------------------
-- Step #1: Call INSERT_INTO_WEATHER sproc
-- ----------------------------------------------------------------------------

CALL insert_into_weather();

-- ----------------------------------------------------------------------------
-- Step #2: Select * from FINAL_WEATHER
-- ----------------------------------------------------------------------------


SELECT utc_to_tz(date), date, city_name, weather_main, weather_description, temp, feels_like, temp_min, temp_max, pressure, humidity, clouds, wind_speed, visibility
FROM FINAL_WEATHER order by 3,  1 desc;


-- ----------------------------------------------------------------------------
-- Step #3: Select * from view WEATHER_HEREDIA (to be run after creating views)
-- ----------------------------------------------------------------------------

-- SELECT FROM VIEW WEATHER_HEREDIA

SELECT * FROM WEATHER_HEREDIA;

