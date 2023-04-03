from snowflake.snowpark import Session

def insert_cleaned_data(session: Session) -> str:

    validate_RAW_WEATHER_STAGE_STREAM = session.sql("SELECT * FROM RAW_WEATHER_STAGE_STREAM WHERE METADATA$ACTION = 'INSERT'")
    
    validate_RAW_WEATHER_STAGE_STREAM_result  = (validate_RAW_WEATHER_STAGE_STREAM.count())

    if validate_RAW_WEATHER_STAGE_STREAM_result > 0:

        insert_result = session.sql("""
            INSERT INTO CR_WEATHER.WEATHER.FINAL_WEATHER ( date, city_id, city_name, country, temp, feels_like, temp_min, temp_max, pressure,
                        humidity, weather_main, weather_description, clouds, wind_speed, visibility, coord_lon,coord_lat)
            SELECT DISTINCT
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
            FROM CR_WEATHER.WEATHER.RAW_WEATHER_STAGE_STREAM
            WHERE METADATA$ACTION = 'INSERT'
        """).collect()

        result = (insert_result[0].as_dict())

        rows_inserted =  result['number of rows inserted'] 

        truncate_result = session.sql("truncate table CR_WEATHER.WEATHER.RAW_WEATHER_STAGE")

        return ("Successfully inserted "+ str(rows_inserted)+ " rows into table FINAL_WEATHER and cleaned table RAW_WEATHER_STAGE")

    else:
        return ("No data found in RAW_WEATHER_STAGE_STREAM")
