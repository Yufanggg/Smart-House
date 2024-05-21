import sys
import gzip
import pandas as pd
import glob
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData, String
from home_messages_db import HomeMessagesDB

TABLE_NAME = 'smartthings'
TABLE_COLUMNS = ['loc', 'level', 'name', 'time', 'capability', 'attribute', 'value', 'unit']
TABLE_TYPES = [String, String, String, Integer, String, String, String, String]

def main(db_url: str, filepath: str):

    # read tsv as dataframe
    df = pd.read_csv(filepath, compression='gzip', sep='\t')

    # data cleanup: drop any fully duplicate rows + fully NaN rows
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True, how='all')

    df.rename(columns={'epoch': 'time'}, inplace=True)

    # insert df into TABLE
    print(f"Inserting {filepath} into {db_url}...")
    MyDB.insert_df(df=df, table=TABLE_NAME, dtype = dict(zip(TABLE_COLUMNS, TABLE_TYPES)), if_exists='append', chunk_size=5000)


if __name__ == '__main__':
    sys.argv = [str(arg) for arg in sys.argv]

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
       smartthings.py -d 'sqlite:///myhome.db' 'data/smartthings/smartthings.202210.tsv.gz'
       smartthings.py -d 'sqlite:///myhome.db' 'data/smartthings/smartthings.*'
            """)
        
    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()
        
        # initialise MyDB and create table if not exists in db
        # if table exists, will just print "Database already has table" and continue to main()
        MyDB = HomeMessagesDB(sys.argv[2])
        MyDB.create_table(name=TABLE_NAME, columns=[
            Column('loc', String(), nullable=True),
            Column('level', String(), nullable=True),
            Column('name', String(), nullable=True),
            Column('time', Integer(), nullable=True),
            Column('capability', String(), nullable=True),
            Column('attribute', String(), nullable=True),
            Column('value', String(), nullable=True),
            Column('unit', String(), nullable=True),
        ])

        # main function (loading / cleaning / inserting of data into db)
        for file in glob.glob(sys.argv[3]):
            main(sys.argv[2], file)
        print("Done.")
