import sys
import gzip
import pandas as pd
from sqlalchemy import Integer, Float
from os import listdir
from home_messages_db import HomeMessagesDB
from models import Pe1T1, Pe1T2
from sqlalchemy import inspect
from pathlib import Path


def main(db_url: str, filepath: Path):
    # read the file into pandas
    try:
        print(filepath)
        df = pd.read_csv(gzip.open(str(filepath), 'rb'))
    except:
        print('cant read file ' + str(filepath))

    # convert time to timestamp and set as index
    df['time'] = pd.to_datetime(df['time']).astype(int).div(10 ** 9).astype(int)
    df.columns = ['time', 'imported T1', 'imported T2', 'exported T1', 'exported T2']
    df = df.fillna(0)

    mydb = HomeMessagesDB('sqlite:///' + db_url)

    if not inspect(mydb.engine).has_table(Pe1T1.__tablename__):
        Pe1T1.__table__.create(bind=mydb.engine)

    if not inspect(mydb.engine).has_table(Pe1T2.__tablename__):
        Pe1T2.__table__.create(bind=mydb.engine)

    t1 = df[['time', 'imported T1', 'exported T1']]
    mydb.insert_df(t1, Pe1T1.__tablename__, {
        'time': Integer(),
        'imported T1': Float(),
        'exported T1': Float()
    }, chunk_size=1000)

    t2 = df[['time', 'imported T2', 'exported T2']]
    mydb.insert_df(t2, Pe1T2.__tablename__, {
        'time': Integer(),
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

        main(sys.argv[2], Path(sys.argv[3]))

    if sys.argv[1] == '-r':
        db_url = 'myhome.db'
        dir_path = 'data/P1e/'
        for filepath in listdir(dir_path):
            main(db_url, Path(dir_path + filepath))
