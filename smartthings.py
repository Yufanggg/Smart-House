import sys
import gzip
import pandas as pd
from os import listdir
from os.path import isfile, join
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData, String
from home_messages_db import HomeMessagesDB

# TO DO: process multiple .tsv.gz files in one line

TABLE_NAME = 'smartthings'
TABLE_COLUMNS = ['loc', 'level', 'name', 'epoch', 'capability', 'attribute', 'value', 'unit']
TABLE_TYPES = [String, String, String, Integer, String, String, String, String]

def main(db_url: str, filepath: str):

    # read tsv as dataframe
    df = pd.read_csv(filepath, compression='gzip', sep='\t')

    # data cleanup: drop any fully duplicate rows + fully NaN rows
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True, how='all')

    # insert df into TABLE
    print(f"Inserting {sys.argv[3]} into {sys.argv[2]}...")
    MyDB.insert_df(df=df, table=TABLE_NAME, dtype = dict(zip(TABLE_COLUMNS, TABLE_TYPES)), if_exists='append')


if __name__ == '__main__':
    if len(sys.argv) < 1:
        print('no options, please view -h')
        exit()

    if sys.argv[1] in ['-h', '--help']:
        print("""
Usage: smartthings.py [options]
Options:
    -h, --help    Show this help message
    -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
       example:
       smartthings.py -d sqlite:///myhome.db smartthings.202210.tsv.gz
            """)
        
    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()
        
        # initialise MyDB and create table if not exists in db
        # if table exists, will just print "Database already has table" and continue to main()
        MyDB = HomeMessagesDB(sys.argv[2])
        MyDB.create_table(name=TABLE_NAME, columns=TABLE_COLUMNS, columnTypes=TABLE_TYPES )

        # main function (loading / cleaning / inserting of data into db)
        main(sys.argv[2], sys.argv[3])
