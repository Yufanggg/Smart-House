import requests #these 3 are for the API to fetch, and the json to read the data
from urllib.parse import urlparse
import json
import pandas as pd
from datetime import datetime, date, time
#import validators - this doesn't work for me for some reason, but it seems like the code (the errors) works without it, correct me if I'm wrong
from home_messages_db import HomeMessagesDB #the subsequent 3 imports are for the homemessages class to work
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine, MetaData, text, Table, Column, Float, Integer, String, PrimaryKeyConstraint, select, inspect
from sqlite3 import Error
import sys #this is for the help command to work - IF one is needed here (I think only the other 3 tools have a help command requirement, tell me if I understood that correctly pls)



#MAIN AND KINDA ONLY ISSUE: THE API FOR FETCHING PAST DATA DOESNT WORK
#It is the url given on the website, my account is "student" so I should have access to historical data, the api key is correct, yet it doesn't work



#def get_weather(city, year, month, day, start_hour, end_hour)
    #Put the entire weather-fetching here, replace the city input with the city being a function argument once the Past data fetching works, etc.

#def make_table(weather_df) - this will be done in one go in get_weather, but I put that here for now in case I am forgetting about something

api_key = '91f1f5e2fd5fb820b2c2e8645c5d99b5'

answer = input("Do you wish to access current or past data? Answer either Current or Past: ") #not needed if we are to deal only with historic data, I put it here and included the current option because                                                                                          #it was the only option that worked at first
if answer=="Current": #current data
    while True:
        city = input("Enter City Name: ")
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric' 
        r = requests.get(url)
        if r.status_code == 404:
            print("Incorrect city name")
        else:
            break
    
    data = r.json()
    lat = data['coord']['lat'] 
    lon = data['coord']['lon']

    city = city[0].upper() + city[1:] #What does this do?
    time_now = datetime.now()
    start = end = datetime.timestamp(time_now) #converts the current time to Unix Time

elif answer=="Past":
    while True:
        city = input("Enter City Name: ")
        url_initial = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric' 
        r = requests.get(url_initial)
        if r.status_code == 404:
            print("Incorrect city name")
        else:
            break
    
    city = city[0].upper() + city[1:]
    response = requests.get(url_initial)
    data = response.json()

    print(response)

    lat = data['coord']['lat'] 
    lon = data['coord']['lon']

    year = int(input("Enter Year of Measurement: "))
    month = int(input("Enter Month (as a number) of Measurement: "))
    day = int(input("Enter Day of Measurement: "))
    start_hour = int(input("Enter Starting Hour of Measurement: "))
    end_hour = int(input("Enter Final Hour of Measurement: "))

    dt_start = datetime.combine(date(year, month, day), time(start_hour))
    dt_end = datetime.combine(date(year, month, day), time(end_hour))

    start = datetime.timestamp(dt_start)
    end = datetime.timestamp(dt_end)

    url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&type=hour&start={start}&end={end}&units=metric&appid={api_key}' #historic data
    #url = f'https://history.openweathermap.org/data/2.5/history/city?lat=52.2400&lon=4.4500&type=hour&start=1715292000&end=1715378400&appid=91f1f5e2fd5fb820b2c2e8645c5d99b5&units=metric'# - example: Noordwijk data from the entire day of 10th May

else:
    raise ValueError("Incorrect input")


response = requests.get(url)
data = response.json()

temp = data['main']['temp'] #if &units=metric doesnt work and the temp is in Kelvin, subtract 273.15 to convert it to Celsius
felt_temp = data['main']['feels_like']
wind = data['wind']['speed'] * 3.6 #converting the wind speed unit from m/s to km/h
press = data['main']['pressure']
humid = data['main']['humidity']
desc = data['weather'][0]['description'] #the description is tbh probably not needed, but I guess it also doesn't hurt to keep it

weather_data = [[lat, lon, start, end, city, temp, 'Celsius', 'temperature'],
            [lat, lon, start, end, city, felt_temp, 'Celsius', 'felt temperature'],
            [lat, lon, start, end, city, wind, 'km/h', 'wind speed'],
            [lat, lon, start, end, city, press, 'Pa', 'pressure'],
            [lat, lon, start, end, city, humid, '%', 'humidity'],
            [lat, lon, start, end, city, desc, 'description', 'description']]
weather_df = pd.DataFrame(weather_data, columns=['Latitude', 'Longitude', 'Start Time', 'End Time', 'City', 'Value', 'Unit', 'Measure'])

print(weather_df) #This is in df format but no longer shows the exact Unix Time in contrast to just using print() to show data

#I guess this is not needed if we decide not to store the weather data in our database but I decided to put this prototype here for now anyway:
#home_db = HomeMessagesDB('sqlite:///db/pythondqlite.db')
#keys = {'Latitude': Float(), 'Longitude': Float(), 'Start Time': Float(), 'End Time': Float(), 'City': String()}
#home_db.insert_df(df = weather_df, table = 'weather', dtype = keys, if_exists = 'replace') #Is this only gonna replace if the data are identical, or the entire table as long as the key column names are the same?


