import sys

sys.path.insert(1, 'SnowPark_CR_Weather')

from utils import snowpark_utils

session = snowpark_utils.get_snowpark_session()

session.add_packages("python-dateutil")   

session.use_role("WEATHER_ROLE")   

result = utc_to_tz_udf = session.udf.register_from_file(
    file_path="SnowPark_CR_Weather/step_3/utc_to_tz.py",
    func_name="utc_to_tz",
    name="utc_to_tz",
    is_permanent=True,
    stage_location ="weather",
    replace=True
)

print(result)
