from snowflake.snowpark.functions import col, call_udf
from unidecode import unidecode
import sys

sys.path.insert(1, 'SnowPark_CR_Weather')

from utils import snowpark_utils

session = snowpark_utils.get_snowpark_session()

weather_table = session.table("FINAL_WEATHER")

provinces_df = weather_table.select("city_name").distinct().collect()

provinces = []

for province in provinces_df:
    print("Processing "+province[0])

    province_name = province[0].split("Provincia de ")[-1]
    province_name_uni = unidecode( province_name.replace(" ", "_") ).upper()

    province_data = weather_table.filter(col ("city_name") == province[0] )
    province_data = province_data.with_column("date", call_udf("UTC_TO_TZ",col("date")))

    result = province_data.createOrReplaceView("WEATHER_"+province_name_uni)
    
    print(result[0][0])
    print()
