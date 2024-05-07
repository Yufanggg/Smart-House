import sys
import gzip
import pandas as pd
from os import listdir
from os.path import isfile, join
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData

TABLE_T1_NAME = 'p1e_t1'
TABLE_T2_NAME = 'p1e_t2'
TABLE_T1_COLUMNS = ['Electricity imported T1', 'Electricity exported T1']
TABLE_T2_COLUMNS = ['Electricity imported T2', 'Electricity exported T2']


def define_table(columns: list, name: str, metadata: MetaData):
    Table(name, metadata,
          Column('time', Integer(), primary_key=True),
          Column(columns[0], Float(), nullable=True),
          Column(columns[1], Float(), nullable=True),
          )
    return metadata


def insert_custom(table, conn, keys, data_iter):
    inserts = []
    for row in data_iter:
        # skip the empty rows
        if row[1] is None:
            continue
        inserts.append(dict(zip(keys, row)))
    stmt = (
        insert(table.table)
        .values(inserts)
    )

    stmt = stmt.on_conflict_do_nothing()
    result = conn.execute(stmt)
    print(f"inserted rows {result.rowcount}")
    return result.rowcount


def main(db_url: str, filepath: str):

    # read the file into pandas
    df = pd.read_csv(gzip.open(filepath, 'rb'))

    # convert time to timestamp and set as index
    df.index = pd.to_datetime(df['time']).astype(int).div(10 ** 9).astype(int)

    # split the tables
    t1 = df[TABLE_T1_COLUMNS]
    t2 = df[TABLE_T2_COLUMNS]

    # creating the tables
    engine = create_engine('sqlite:///db/pythondqlite.db')
    metadata = MetaData()

    if not inspect(engine).has_table(TABLE_T1_NAME):
        print(f"Creating {TABLE_T1_NAME} table")
        metadata = define_table(TABLE_T1_COLUMNS, TABLE_T1_NAME, metadata)
        metadata.create_all(engine)

    if not inspect(engine).has_table(TABLE_T2_NAME):
        print(f"Creating {TABLE_T2_NAME} table")
        metadata = define_table(TABLE_T2_COLUMNS, TABLE_T2_NAME, metadata)
        metadata.create_all(engine)

    with engine.connect() as conn:
        t1.to_sql(name=TABLE_T1_NAME, con=conn, if_exists="replace", method=insert_custom)
        t2.to_sql(name=TABLE_T2_NAME, con=conn, if_exists="replace", method=insert_custom)


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
        pass
