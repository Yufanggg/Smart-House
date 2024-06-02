import sys
import gzip
import pandas as pd
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData, DateTime, String
import os
# self-defined class
from home_messages_db import HomeMessagesDB

import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime, date, time
#from home_messages_db import HomeMessagesDB #the subsequent 3 imports are for the homemessages class to work
#from sqlalchemy.dialects.sqlite import insert
#from sqlalchemy import create_engine, MetaData, text, Table, Column, Float, Integer, String, PrimaryKeyConstraint, select, inspect
#from sqlite3 import Error
#import sys #this is for the help command to work - IF one is needed here (I think only the other 3 tools have a help command requirement, tell me if I understood that correctly pls)

#requirements: pip install openmeteo-requests & pip install requests-cache retry-requests numpy pandas





def main(db_url: str, start: str, end:str):

    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)
    # start = input("Provide Starting Date of Measurement (in YYYY-MM-DD format): ")
    # end = input("Provide Ending Date of Measurement (in YYYY-MM-DD format): ")
    
    dt_start = datetime.strptime(start, '%Y-%m-%d').date()
    dt_end = datetime.strptime(end, '%Y-%m-%d').date()
    
    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive" 
    params = {
	"latitude": 52.196835,
	"longitude": 4.4833946,
	"start_date": dt_start,
	"end_date": dt_end,
    "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "wind_speed_10m", "wind_direction_10m", "soil_temperature_0_to_7cm"]
    }
    
    responses = openmeteo.weather_api(url, params=params)
    
    # Process first location. Add a for-loop for multiple locations or weather models 
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s") #offset seconds is also 0 in teacher's link
    
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relativehumidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()
    hourly_windspeed_10m = hourly.Variables(4).ValuesAsNumpy()
    hourly_winddirection_10m = hourly.Variables(5).ValuesAsNumpy()
    hourly_soil_temperature_0_to_7cm = hourly.Variables(6).ValuesAsNumpy()
    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True), #check if it has to be changed to gmt somehow, since the data is the same as for gmt in the teacher's link
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()), 
        inclusive = "left")}
    
    hourly_data["temperature_2m_°C"] = hourly_temperature_2m
    hourly_data["relativehumidity_2m_%"] = hourly_relativehumidity_2m
    hourly_data["rain_mm"] = hourly_rain
    hourly_data["snowfall_cm"] = hourly_snowfall
    hourly_data["windspeed_10m_km/h"] = hourly_windspeed_10m
    hourly_data["winddirection_10m_°"] = hourly_winddirection_10m
    hourly_data["soil_temperature_0_to_7cm_°C"] = hourly_soil_temperature_0_to_7cm
    hourly_dataframe = pd.DataFrame(data = hourly_data)

    # convert time to timestamp and set as index
    hourly_dataframe['unixtime'] = pd.to_datetime(hourly_dataframe['date']).astype('int64').div(10**9).astype(int)
    # print(hourly_dataframe["unixtime"])

    #initalize the db class
    mydb = HomeMessagesDB(db_url)
    # mydb = HomeMessagesDB('db/pythondqlite.db')
    Keys = {"date": DateTime(),"unixtime": Integer(), "temperature_2m_°C": Float(), "relativehumidity_2m": Float(), "rain_mm": Float(), 
            "snowfall_cm": Float(), "windspeed_10m_km": Float(),"winddirection_10m_°": Float(), "soil_temperature_0_to_7cm_°C": Float()}
    mydb.insert_df(df = hourly_dataframe, table = "openweathermap", dtype = Keys, if_exists = "replace")



if __name__ == '__main__':

    if len(sys.argv) < 1:
        print('no options, please view -h')
        exit()

    if sys.argv[1] in ['-h', '--help']:
        print("Usage: your_script.py [options]")
        print("Options:")
        print("  -h, --help    Show this help message")
        print("  -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)")
        print(" example:")
        print("openweathermap_self.py -d sqlite:///myhome.db 2022-01-01 2024-04-01")

    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3] or not sys.argv[4]:
            print('invalid options, please view -h')
            exit()
        print(sys.argv[4])

        main(sys.argv[2], sys.argv[3], sys.argv[4])

    if sys.argv[1] == '-r':
        pass