#
# This file is for handy reusable functions that don't really belong in the my_message_db class
#
import datetime

import pandas as pd


def create_timestamp(year, month=None, day=None, hour=0, minute=0, second=0, ):
    return int(datetime.datetime(year, month, day, hour, minute, second).timestamp())


def df_timestamp_index(df: pd.DataFrame, column: str = 'time') -> pd.DataFrame:
    df[column] = pd.to_datetime(df[column] * 10 ** 9)
    return df.set_index(column)
