from snowflake.snowpark import Session
# from utils import snowpark_utils

def insert_cleaned_data(session: Session) -> str:

    validate_CLEANED_RAW_WEATHER_STREAM = session.sql("SELECT * FROM CLEANED_RAW_WEATHER_STREAM WHERE METADATA$ACTION = 'INSERT'")
    
    validate_CLEANED_RAW_WEATHER_STREAM_result = (validate_CLEANED_RAW_WEATHER_STREAM.count())

    if validate_CLEANED_RAW_WEATHER_STREAM_result > 0:

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
            FROM HLB_TEST.PUBLIC.CLEANED_RAW_WEATHER_STREAM
            WHERE METADATA$ACTION = 'INSERT'

        """).collect()

        result = (insert_result[0].as_dict())

        rows_inserted =  result['number of rows inserted'] 

        return ("Successfully inserted "+ str(rows_inserted)+ " rows into table WEATHER and truncated table CLEANED_RAW_WEATHER")

    else:
        return ("No data found in CLEANED_RAW_WEATHER_STREAM")

# session = snowpark_utils.get_snowpark_session()

# insert_cleaned_data(session)