from snowflake.snowpark import Session
from utils import snowpark_utils

  
session = snowpark_utils.get_snowpark_session()

session.add_packages("snowflake-snowpark-python")   

result = utc_to_tz_udf = session.sproc.register_from_file(
    file_path="/Users/davidthomas/Desktop/CR_Weather/SnowPark_CR_Weather/upload_results.py",
    func_name="insert_cleaned_data",
    name="insert_into_weather",
    is_permanent=True,
    stage_location ="weather",
    replace=True
)

print(result)