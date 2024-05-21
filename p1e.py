import sys
import gzip
import pandas as pd
from sqlalchemy import Column, Integer, Float
from os import listdir
from home_messages_db import HomeMessagesDB

TABLE_T1_NAME = 'p1e_t1'
TABLE_T2_NAME = 'p1e_t2'


def main(db_url: str, filepath: str):
    # read the file into pandas
    try:
        print(filepath)
        df = pd.read_csv(gzip.open(filepath, 'rb'))
    except:
        print('cant read file ' + filepath)

    # convert time to timestamp and set as index
    df.index = pd.to_datetime(df['time']).astype(int).div(10 ** 9).astype(int)
    df = df.drop('time', axis=1)
    df.columns = ['imported T1', 'imported T2', 'exported T1', 'exported T2']

    mydb = HomeMessagesDB('sqlite:///' + db_url)
    mydb.create_table(TABLE_T1_NAME, [
        Column('time', Integer(), primary_key=True),
        Column('imported T1', Float(), nullable=True),
        Column('exported T1', Float(), nullable=True),
    ])

    mydb.create_table(TABLE_T2_NAME, [
        Column('time', Integer(), primary_key=True),
        Column('imported T2', Float(), nullable=True),
        Column('exported T2', Float(), nullable=True),
    ])

    t1 = df[['imported T1', 'exported T1']]
    mydb.insert_df(t1, TABLE_T1_NAME, {
        'imported T1': Float(),
        'exported T1': Float()
    }, chunk_size=1000)

    t2 = df[['imported T2', 'exported T2']]
    mydb.insert_df(t2, TABLE_T2_NAME, {
        'imported T2': Float(),
        'exported T2': Float()
    }, chunk_size=1000)


if __name__ == '__main__':

    if len(sys.argv) < 1:
        print('no options, please view -h')
        exit()

    if sys.argv[1] in ['-h', '--help']:
        print("""
Usage: p1e.py [options]
Options:
    -h, --help    Show this help message
    -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
       example:
       p1e.py -d sqlite:///myhome.db P1e-2022-12-01-2023-01-10.csv.gz
    -r the recursive fully automated option. Just import the whole datasource without
       further specification
            """)

    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()

        main(sys.argv[2], sys.argv[3])

    if sys.argv[1] == '-r':
        db_url = 'db/pythondqlite.db'
        dir_path = 'data/P1e/'
        for filepath in listdir(dir_path):
            main(db_url, dir_path + filepath)
