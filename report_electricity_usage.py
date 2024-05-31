from models import Pe1T1, Pe1T2, SmartThingsGround
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import between
from home_messages_db import HomeMessagesDB
from functions import create_timestamp, df_timestamp_index
import statistics
import datetime
import calendar
import numpy as np


class ReportElcUsage:
    """
    This class is to be called in the report_electricity_usage report
    goal is to make the report more readable
    """
    MyDB = NotImplementedError

    def __init__(self):
        self.MyDB = HomeMessagesDB('sqlite:///db/pythondqlite.db')
        
    def _df_elc(self, start_date, end_date):
        t1 = self.MyDB.read_model_df(
            Pe1T1,
            between(Pe1T1.time, start_date, end_date))

        t2 = self.MyDB.read_model_df(
            Pe1T2,
            between(Pe1T2.time, start_date, end_date))

        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t2 = df_timestamp_index(t2)

        df = pd.concat([t1, t2]).fillna(0)

        # Resample on month
        df = df.resample('ME').sum()

        # Convert to megawatt per hour
        df['imported T1'] = df['imported T1'] / 1000
        df['imported T2'] = df['imported T2'] / 1000
       
        return df

    def figure1(self):
        df = self._df_elc(create_timestamp(2022, 4, 1), create_timestamp(2024, 4, 1))
        sns.set_theme(style="darkgrid")
        # convert to wide format
        df_melted = df.reset_index().melt(
            id_vars='time',
            value_vars=['imported T1', 'imported T2'],
            var_name='Tariff',
            value_name='Total Usage (MWh)'
        )
        sns.lineplot(
            x='time',
            y='Total Usage (MWh)', hue='Tariff', data=df_melted)
        plt.ylabel('Total Usage (MWh)')
        plt.title('Total Electricity Consumption')
        plt.tick_params(axis='x', labelrotation = 90)
        plt.show()

    def figure2(self):
        df = self._df_elc(create_timestamp(2023, 1, 1), create_timestamp(2023, 12, 31))
        sns.set_theme(style="darkgrid")
        # convert to wide format
        df_melted = df.reset_index().melt(
            id_vars='time',
            value_vars=['imported T1', 'imported T2'],
            var_name='Tariff',
            value_name='Total Usage (MWh)'
        )
        sns.barplot(
            x='time',
            y='Total Usage (MWh)', hue='Tariff', data=df_melted)
        plt.ylabel('Total Usage (MWh)')
        plt.title('Total Electricity Consumption')
        plt.tick_params(axis='x', labelrotation = 90)
        plt.show()

    def march_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1, between(Pe1T1.time, create_timestamp(2023, 3, 1), create_timestamp(2023, 3, 31)))
        t2 = self.MyDB.read_model_df(Pe1T2, between(Pe1T2.time, create_timestamp(2023, 3, 1), create_timestamp(2023, 3, 31)))
        
        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t2 = df_timestamp_index(t2) 

        df = pd.concat([t1,t2]).fillna(0)

        # Resample on month
        df = df.resample('D').sum()

        print(df)

    def oct_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1, between(Pe1T1.time, create_timestamp(2023, 10, 1), create_timestamp(2023, 10, 31)))
        t2 = self.MyDB.read_model_df(Pe1T2, between(Pe1T2.time, create_timestamp(2023, 10, 1), create_timestamp(2023, 10, 31)))
        
        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t2 = df_timestamp_index(t2) 

        df = pd.concat([t1,t2]).fillna(0)

        # Resample on month
        df = df.resample('D').sum()

        print(df)

    def motion(self):
        df = self.MyDB.read_model_df(SmartThingsGround, SmartThingsGround.attribute == "motion")
        df = df_timestamp_index(df)
        df = df[[ 'value']]

        with pd.option_context("future.no_silent_downcasting", True):
            df = df.replace(to_replace='active', value=1)
            df = df.replace(to_replace='inactive', value=0)

        df = df.resample('D').sum()

        sns.set_theme(style="darkgrid")
        # convert to wide format

        sns.lineplot(x='time',y='value', data=df)
        plt.ylabel('Motion triggers')
        plt.title('Total Movement ground level')
        plt.tick_params(axis='x', labelrotation = 90)
        plt.show()

    def temps(self, month, name):
        
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        days_march = list(chunks(month['temperature_2m_°C'], 24))
        max_temp = list()
        days = list(range(1,32))
        for i in days_march:
            max_temp.append(max(i))
        df = pd.DataFrame({'Day':days,
                            'Max Temp':max_temp})
        
        sns.lineplot(x = 'Day',y='Max Temp', data = df)
        plt.ylabel('Temperature °C')
        plt.title('Max Temperature per Day')
        plt.show()

        average_temp = statistics.mean(max_temp)
        print('Average temp in ', name, ': ', average_temp) 
    
    def figure3(self):
        df = self._df_elc(create_timestamp(2022, 4, 1), create_timestamp(2022, 12, 31))
        sns.set_theme(style="darkgrid")
        # convert to wide format
        df_melted = df.reset_index().melt(
            id_vars='time',
            value_vars=['imported T1', 'imported T2'],
            var_name='Tariff',
            value_name='Total Usage (MWh)'
        )
        sns.barplot(
            x='time',
            y='Total Usage (MWh)', hue='Tariff', data=df_melted)
        plt.ylabel('Total Usage (MWh)')
        plt.title('Total Electricity Consumption')
        plt.tick_params(axis='x', labelrotation = 90)
        plt.show()

    def figure4(self):
        df = self._df_elc(create_timestamp(2024, 1, 1), create_timestamp(2024, 12, 31))
        sns.set_theme(style="darkgrid")
        # convert to wide format
        df_melted = df.reset_index().melt(
            id_vars='time',
            value_vars=['imported T1', 'imported T2'],
            var_name='Tariff',
            value_name='Total Usage (MWh)'
        )
        sns.barplot(
            x='time',
            y='Total Usage (MWh)', hue='Tariff', data=df_melted)
        plt.ylabel('Total Usage (MWh)')
        plt.title('Total Electricity Consumption')
        plt.tick_params(axis='x', labelrotation = 90)
        plt.show()
    
    def jan_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1, between(Pe1T1.time, create_timestamp(2023, 1, 2), create_timestamp(2023, 2, 1)))
        
        # Set the timestamp index
        t1 = df_timestamp_index(t1) 
        t1 = t1.resample('D').sum()
        #Separate the data into each week
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        
        weeks_jan = list(chunks(t1, 7))
        month = []
        for i in weeks_jan:
            month.append(i, ignore_index = True)
        return month
    
    def dec_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1, between(Pe1T1.time, create_timestamp(2023, 12, 2), create_timestamp(2024, 1, 1)))
        
        # Set the timestamp index
        t1 = df_timestamp_index(t1) 
        t1 = t1.resample('D').sum()
        #Separate the data into each week
        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        
        weeks_dec = list(chunks(t1, 7))
        days = ['Friday', 'Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday']
        weekday = pd.DataFrame()
        for j in range(0,6):
            for i in weeks_dec:
                weekday["{}".format(days[j])] = i.index[j]
        print(weekday)

            