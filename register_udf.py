from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from utils import snowpark_utils

  
session = snowpark_utils.get_snowpark_session()

session.add_packages("python-dateutil")   

result = utc_to_tz_udf = session.udf.register_from_file(
    file_path="/Users/davidthomas/Desktop/CR_Weather/SnowPark_CR_Weather/utc_to_tz.py",
    func_name="utc_to_tz",
    name="utc_to_tz",
    is_permanent=True,
    stage_location ="weather",
    replace=True
)

print(result)