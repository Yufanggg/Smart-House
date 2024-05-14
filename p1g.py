import sys
import gzip
import pandas as pd
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine

def insert_custom(table, conn, keys, data_iter):


    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = (
        insert(table.table)
        .values(data)
    )
    stmt = stmt.on_duplicate_key_update(b=stmt.inserted.b, c=stmt.inserted.c)
    result = conn.execute(stmt)
    return result.rowcount


def main(db_url: str, filepath: str):

    # read the file into pandas
    df = pd.read_csv(gzip.open(filepath, 'rb'))

    # convert time to timestamp and set as index
    df.index = pd.to_datetime(df['time']).astype(int).div(10**9).astype(int)

    # split the tables
    t1 = df[['Electricity imported T1', 'Electricity exported T1']]
    t2 = df[['Electricity imported T2', 'Electricity exported T2']]

    # use sqlalchemy for custom insert logic
    engine = create_engine('db/pythondqlite.db')
    with engine.connect() as conn:
        t1.to_sql(name="p1e_t1", con=conn, if_exists="append", method=insert_custom)
        t2.to_sql(name="p1e_t1", con=conn, if_exists="append", method=insert_custom)


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
        print("p1e.py -d sqlite:///myhome.db P1e-2022-12-01-2023-01-10.csv.gz")

    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()

        main(sys.argv[2], sys.argv[3])

    if sys.argv[1] == '-r':
        pass