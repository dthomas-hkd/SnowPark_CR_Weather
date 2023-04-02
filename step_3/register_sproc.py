import sys

sys.path.insert(1, 'SnowPark_CR_Weather')

from utils import snowpark_utils

session = snowpark_utils.get_snowpark_session()

session.add_packages("snowflake-snowpark-python")   

result = utc_to_tz_udf = session.sproc.register_from_file(
    file_path="SnowPark_CR_Weather/step_3/upload_results.py",
    func_name="insert_cleaned_data",
    name="insert_into_weather",
    is_permanent=True,
    stage_location ="weather",
    replace=True
)

print(result)