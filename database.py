import sys
from sqlalchemy import Column, Integer, Float, String
import os
# self-defined class
from home_messages_db import HomeMessagesDB


def main(db_url: str, table_name: str):
    mydb = HomeMessagesDB(db_url)
    if table_name == "p1g":
        mydb.create_table(name=table_name, columns=[
            Column('time', Integer(), nullable=True),
            Column('unixtime', Integer(), nullable=True),
            Column('Total gas used', Float(), nullable=True)])

    elif table_name == "p1e":
        mydb.create_table(name=table_name, columns=[
            Column('time', Integer(), primary_key=True),
            Column('imported T2', Float(), nullable=True),
            Column('exported T2', Float(), nullable=True)])

    elif table_name == "smartthings":
        mydb.create_table(name=table_name, columns=[
            Column('loc', String(), nullable=True),
            Column('level', String(), nullable=True),
            Column('name', String(), nullable=True),
            Column('time', Integer(), nullable=True),
            Column('capability', String(), nullable=True),
            Column('attribute', String(), nullable=True),
            Column('value', String(), nullable=True),
            Column('unit', String(), nullable=True)])

    elif table_name == "openweathermap":
        mydb.create_table(name=table_name, columns=[
            Column('date', String(), nullable=True),
            Column('unixtime', Integer(), nullable=True),
            Column('temperature_2m_°C', Float(), nullable=True),
            Column('relativehumidity_2m', Float(), nullable=True),
            Column('rain_mm', Float(), nullable=True),
            Column('snowfall_cm', Float(), nullable=True),
            Column('windspeed_10m_km', Float(), nullable=True),
            Column('winddirection_10m_°', Float(), nullable=True),
            Column('soil_temperature_0_to_7cm_°C', Float(), nullable=True)]
                          )


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
        print("databse.py -d sqlite:///myhome.db p1g")

    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()

        main(sys.argv[2], sys.argv[3])

    if sys.argv[1] == '-r':
        pass
