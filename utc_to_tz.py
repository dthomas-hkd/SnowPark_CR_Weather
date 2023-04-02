from dateutil.parser import isoparse
from dateutil.relativedelta import *


def utc_to_tz(str_utc: str) -> str:

    dt = isoparse(str_utc)

    str_time_stamp = dt+relativedelta(hours=-6)

    print(str_time_stamp)
    
    return str( str_time_stamp )
