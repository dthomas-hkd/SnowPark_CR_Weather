--CREATE VIEWS PER PROVINCE

USE WAREHOUSE COMPUTE_WH;
USE SCHEMA HLB_TEST.PUBLIC;

CALL INSERT_INTO_WEATHER();

SELECT UTC_TO_TZ(date), city_name, weather_main, weather_description, temp, feels_like, temp_min, temp_max, pressure, humidity, clouds, wind_speed, visibility
FROM WEATHER order by 1 desc;


