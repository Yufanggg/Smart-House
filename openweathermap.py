import requests
import pandas as pd
import datetime
from datetime import datetime, date, time
from urllib.parse import urlparse
import validators

api_key = '91f1f5e2fd5fb820b2c2e8645c5d99b5'

lat = 0
lon = 0
url = ''
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
        #raise an error if there is no city with the provided name "Incorrect city name"

    city = city[0].upper() + city[1:]
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
        #getting the latitude and longitude of a given city, limit=1 because we only want the API to find 1 city
         #raise an error if there is no city with the provided name "Incorrect city name"
    city = city[0].upper() + city[1:]
    response = requests.get(url_initial)
    data = response.json()

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
    #url = f'https://history.openweathermap.org/data/2.5/history/city?lat=52.2400&lon=4.4500&type=hour&start=1715292000&end=1715378400&appid=91f1f5e2fd5fb820b2c2e8645c5d99b5&units=metric' - Noordwijk data from the entire day of 10th May

else:
    raise ValueError("Incorrect input")


#Note: If &units=metric doesnt work and the temp is in Kelvin, subtract 273.15 to convert it to Celsius
response = requests.get(url)
data = response.json()

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

