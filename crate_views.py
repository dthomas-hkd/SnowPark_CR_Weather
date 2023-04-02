from snowflake.snowpark import Session
from utils import snowpark_utils


session = snowpark_utils.get_snowpark_session()

weather_table = session.table("Weather")

print(weather_table.count())

provinces = weather_table.select("city_name").distinct().collect()

print(provinces)