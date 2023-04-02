--UPDATE WEATHER TABLE AND SELECT DATA
USE WAREHOUSE WEAThER_WH;
USE SCHEMA CR_WEATHER.WEATHER;

USE ROLE WEATHER_ROLE;


CALL INSERT_INTO_WEATHER();


SELECT utc_to_tz(date), date, city_name, weather_main, weather_description, temp, feels_like, temp_min, temp_max, pressure, humidity, clouds, wind_speed, visibility
FROM FINAL_WEATHER order by 3,  1 desc;


-- SELECT FROM VIEW WEATHER_HEREDIA

SELECT * FROM WEATHER_HEREDIA;

