import sys
import gzip
import pandas as pd
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData, String
import os
# self-defined class
from home_messages_db import HomeMessagesDB

def main(db_url: str, filepath: str):

    if os.path.isdir(filepath): # if the filepath is a dictionary, update the filepath
        files = os.listdir(filepath)
        filepath = [filepath + "\\" + file for file in files] # updated filepath, which is a list

        df = [pd.read_csv(gzip.open(file, 'rb'),index_col= False) for file in filepath]

        # df = pd.read_csv(gzip.open(filepath[0], 'rb'),index_col= False) 
        

        
        # check whether all dataframe in df has identical columns, name & number
        if all([set(df[0].columns) == set(dataframe.columns) for dataframe in df]):
            df = pd.concat(df)
        else:
            raise ValueError("Sorry, different files have different columns")
    
    else:
        # read the file into pandas
        df = pd.read_csv(gzip.open(filepath, 'rb'),index_col= False)

    # convert time to timestamp and set as index
    df['unixtime'] = pd.to_datetime(df['time']).astype('int64').div(10**9).astype(int)

    # print(df.head(10))
    # print(df.shape)

    #initalize the db class
    mydb = HomeMessagesDB(db_url)
    Keys = {"time": String(), "unixtime": Integer(), "Total gas used": Float()}
    mydb.insert_df(df = df, table = "p1g", dtype = Keys, if_exists = "replace")



if __name__ == '__main__':

    if len(sys.argv) < 1:
        print('no options, please view -h')
        exit()

    if sys.argv[1] in ['-h', '--help']:
        print("Usage: your_script.py [options]")
        print("Options:")
        print("  -h, --help    Show this help message")
        print("  -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)")
        print(" example1:")
        print("p1g.py -d sqlite:///myhome.db P1g-2022-01-01-2022-07-10.csv.gz")
        print(" example2:")
        print("p1g.py -d sqlite:///myhome.db .\data\P1g ")

    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()

        main(sys.argv[2], sys.argv[3])

    if sys.argv[1] == '-r':
        pass