# import os
# from dotenv import load_dotenv
# import json

from datetime import datetime
from dateutil.relativedelta import relativedelta

def getTimeRange(relative_days_positive):
    dateTimeVar = (
        datetime.fromtimestamp(
            int(
                (
                    datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    + relativedelta(days=-relative_days_positive)
                ).timestamp()
            )
        ).strftime("%Y%m%d")
        + "-"
        + datetime.fromtimestamp(
            int(
                (
                    datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                ).timestamp()
            )
        ).strftime("%Y%m%d")
    )
    return dateTimeVar
