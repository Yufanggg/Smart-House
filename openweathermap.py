import requests
import pandas as pd
from datetime import datetime


api_key = '91f1f5e2fd5fb820b2c2e8645c5d99b5'


city = input("Enter City Name: ")
answer = input("Do you wish to access current or past data? Answer either Current or Past: ") #not needed if we are to deal only with historic data, I put it here and included the current option because
                                                                                            #it was the only option that worked at first

if answer=="Current":
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric' #current data
    #raise an error if there is no city with the provided name "Incorrect city name"

    time_now = datetime.now()
    start = end = datetime.timestamp(time_now) #converts the current time to Unix Time

elif answer=="Past":
    
    url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={api_key}' #getting the latitude and longitude of a given city, limit=1 because we only want the API to find 1 city
    #raise an error if there is no city with the provided name "Incorrect city name"

    response = requests.get(url)
    data = response.json()

    #lat = data['main']['lat'] doesnt work rn, I dont know yet why, but here is the link to the geocoding API: https://openweathermap.org/api/geocoding-api
    #lon = data['main']['lon']

    year = input("Enter Year of Measurement: ")
    #month = input("Enter Month of Measurement: ")
    #day = input("Enter Day of Measurement: ")
    #start_hour = input("Enter Starting Date of Measurement: ")
    #end_hour = input("Enter Final Date of Measurement: ")

    #start = combine year, month, day, start_hour & convert the date to Unix time (or ask for date in format year/month/day...)
    #end = same as above

    #url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}type=hour&start={start}&end={end}&appid={api_key}&units=metric' - historic data
    #url = f'https://history.openweathermap.org/data/2.5/history/city?lat=52.2400&lon=4.4500&type=hour&start=1715292000&end=1715378400&appid={api_key}&units=metric' - Noordwijk data from the entire day of 10th May

else:
    raise ValueError("Incorrect input")


response = requests.get(url)
data = response.json()

#Note: If &units=metric doesnt work and the temp is in Kelvin, subtract 273.15 to convert it to Celsius

temp = data['main']['temp']
wind = data['wind']['speed']
press = data['main']['pressure']
humid = data['main']['humidity']
desc = data['weather'][0]['description']

print('Start Time:', start, 'seconds')
print('End Time:', end, 'seconds')
print('Temperature:',temp,'°C')
print('Wind:',wind)
print('Pressure: ',press)
print('Humidity: ',humid)
print('Description:',desc)

data_df = [[start, end, city, temp, wind, press, humid, desc]]
df = pd.DataFrame(data_df, columns=['Start Time', 'End Time', 'City', 'Temp', 'Wind', 'Pressure', 'Humidity', 'Description'])
print(df) #in df format but no longer shows the exact Unix Time

