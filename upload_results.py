from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from utils import snowpark_utils
import sys, os


def insert_cleaned_data () -> int:

    session.use_database('HLB_TEST')
    session.use_schema('PUBLIC')
    session.use_role('ACCOUNTADMIN')

    insert_result = session.sql("""
    
        INSERT INTO WEATHER( date, city_id, city_name, country, temp, feels_like, temp_min, temp_max, pressure,
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
        FROM cleaned_raw_weather;

    """).collect()

    result = (insert_result[0].as_dict())

    print( 'Number of rows inserted' )
    print( result['number of rows inserted'] )


    if result['number of rows inserted']  > 0:
        session.use_database('HLB_TEST')
        session.use_schema('PUBLIC')
        session.use_role('ACCOUNTADMIN')

        clean_stage_table_result = session.sql("truncate table cleaned_raw_weather").collect()
        
        result = (clean_stage_table_result[0].as_dict())

        print( 'Cleaned results:' )
        print( result )

        return result['number of rows inserted']

if __name__ == "__main__":
  
    session = snowpark_utils.get_snowpark_session()

    processed_rows = insert_cleaned_data()


    