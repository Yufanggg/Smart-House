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


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

class ReportElcUsage:
    """
    This class is to be called in the report_electricity_usage report
    goal is to make the report more readable
    """
    MyDB = NotImplementedError

    def __init__(self):
        self.MyDB = HomeMessagesDB('sqlite:///myhome.db')

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
        plt.tick_params(axis='x', labelrotation=90)
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
        plt.tick_params(axis='x', labelrotation=90)
        plt.show()

    def march_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1,
                                     between(Pe1T1.time, create_timestamp(2023, 3, 1), create_timestamp(2023, 3, 31)))
        t2 = self.MyDB.read_model_df(Pe1T2,
                                     between(Pe1T2.time, create_timestamp(2023, 3, 1), create_timestamp(2023, 3, 31)))

        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t2 = df_timestamp_index(t2)

        df = pd.concat([t1, t2]).fillna(0)

        # Resample on month
        df = df.resample('D').sum()

        print(df)

    def oct_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1,
                                     between(Pe1T1.time, create_timestamp(2023, 10, 1), create_timestamp(2023, 10, 31)))
        t2 = self.MyDB.read_model_df(Pe1T2,
                                     between(Pe1T2.time, create_timestamp(2023, 10, 1), create_timestamp(2023, 10, 31)))

        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t2 = df_timestamp_index(t2)

        df = pd.concat([t1, t2]).fillna(0)

        # Resample on month
        df = df.resample('D').sum()

        print(df)

    def motion(self):
        df = self.MyDB.read_model_df(SmartThingsGround, SmartThingsGround.attribute == "motion")
        df = df_timestamp_index(df)
        df = df[['value']]

        with pd.option_context("future.no_silent_downcasting", True):
            df = df.replace(to_replace='active', value=1)
            df = df.replace(to_replace='inactive', value=0)

        df = df.resample('D').sum()

        sns.set_theme(style="darkgrid")
        # convert to wide format

        sns.lineplot(x='time', y='value', data=df)
        plt.ylabel('Motion triggers')
        plt.title('Total Movement ground level')
        plt.tick_params(axis='x', labelrotation=90)
        plt.show()

    def temps(self, month, name):

        def chunks(lst, n):
            """Yield successive n-sized chunks from lst."""
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        days_march = list(chunks(month['temperature_2m_°C'], 24))
        max_temp = list()
        days = list(range(1, 32))
        for i in days_march:
            max_temp.append(max(i))
        df = pd.DataFrame({'Day': days,
                           'Max Temp': max_temp})

        sns.lineplot(x='Day', y='Max Temp', data=df)
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
        plt.tick_params(axis='x', labelrotation=90)
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
        plt.tick_params(axis='x', labelrotation=90)
        plt.show()

    def jan_table(self):
        t1 = self.MyDB.read_model_df(Pe1T1,
            between(Pe1T1.time, create_timestamp(2022, 12, 31), create_timestamp(2023, 2, 1))
        )

        # remove accumulation
        t1['imported T1'] = t1['imported T1'].diff()

        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t1 = t1.resample('D').sum()

        # drop the first rows because of strange cutoff
        t1 = t1.iloc[3:]

        days = [ 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        i = 0
        for index, row in t1.iterrows():
            t1.at[index, 'weekday'] = days[i % 7]
            i += 1

        #1. Extract Week Number
        t1['week_number'] = t1.index.isocalendar().week

        # 4. Create Line Plot with Hue for Week Number
        sns.lineplot(data=t1, x='weekday', y='imported T1', hue='week_number',)

        # 5. Add Labels and Title
        plt.xlabel('Weekday')
        plt.ylabel('Total Usage (MWh)')
        plt.title('Weekly Electricity Usage Trends (January 2023)')
        plt.legend(title='Week Number')

        # 6. Show the Plot
        plt.show()

    def dec_table(self):
        t1 = self.MyDB.read_model_df(
            Pe1T1,
            between(Pe1T1.time, create_timestamp(2023, 11, 30), create_timestamp(2024, 1, 1))
        )

        # remove accumulation
        t1['imported T1'] = t1['imported T1'].diff()

        # Set the timestamp index
        t1 = df_timestamp_index(t1)
        t1 = t1.resample('D').sum()

        # drop the first rows because of strange cutoff
        t1 = t1.iloc[2:]

        days = ['Friday', 'Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday']
        i = 0
        for index, row in t1.iterrows():
            t1.at[index, 'weekday'] = days[i % 7]
            i += 1

        #1. Extract Week Number
        t1['week_number'] = t1.index.isocalendar().week

        # 4. Create Line Plot with Hue for Week Number
        sns.lineplot(data=t1, x='weekday', y='imported T1', hue='week_number',)

        # 5. Add Labels and Title
        plt.xlabel('Weekday')
        plt.ylabel('Total Usage (MWh)')
        plt.title('Weekly Electricity Usage Trends (December 2023)')
        plt.legend(title='Week Number')

        # 6. Show the Plot
        plt.show()
