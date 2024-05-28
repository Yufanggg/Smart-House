from models import Pe1T1, Pe1T2
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import between
from home_messages_db import HomeMessagesDB
from functions import create_timestamp, df_timestamp_index


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
        plt.show()
