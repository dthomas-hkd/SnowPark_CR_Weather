from snowflake.snowpark import Session

def insert_cleaned_data(session: Session) -> str:

    validate_CLEANED_RAW_WEATHER = session.sql("SELECT * FROM CLEANED_RAW_WEATHER")
    
    validate_CLEANED_RAW_WEATHER_result = (validate_CLEANED_RAW_WEATHER.count())

    if validate_CLEANED_RAW_WEATHER_result > 0:

        insert_result = session.sql("""
        
            INSERT INTO HLB_TEST.PUBLIC.WEATHER( date, city_id, city_name, country, temp, feels_like, temp_min, temp_max, pressure,
                        humidity, weather_main, weather_description, clouds, wind_speed, visibility, coord_lon,coord_lat)
            SELECT 
                TO_TIMESTAMP_NTZ($1:dt),
                $1:id,
                $1:name,
                $1:sys.country,
                $1:main.temp,
                $1:main.feels_like,
                $1:main.temp_min,
                $1:main.temp_max,
                $1:main.pressure,
                $1:main.humidity,
                $1:weather[0].main,
                $1:weather[0].description,
                $1:clouds.all,
                $1:wind.speed,
                $1:visibility,
                $1:coord.lon,
                $1:coord.lat
            FROM HLB_TEST.PUBLIC.CLEANED_RAW_WEATHER;

        """).collect()

        result = (insert_result[0].as_dict())

        rows_inserted =  result['number of rows inserted'] 

        if rows_inserted > 0:

            session.sql("truncate table HLB_TEST.PUBLIC.CLEANED_RAW_WEATHER")

        return ("Successfully inserted "+ str(rows_inserted)+ "rows and truncated table CLEANED_RAW_WEATHER")

    else:
        return ("No data found in CLEANED_RAW_WEATHER")

