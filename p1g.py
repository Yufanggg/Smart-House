import sys
import gzip
import pandas as pd
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData

# self-defined class
from home_messages_db import HomeMessagesDB




def define_table(columns: list, name: str, metadata: MetaData):
    Table(name, metadata,
          Column('time', Integer(), primary_key=True),
          Column(columns[0], Float(), nullable=True),
          Column(columns[1], Float(), nullable=True),
          )
    return metadata

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



def insert_df(self, df: pd.DataFrame, table: str, if_exists: str = 'append'):
        """
        Inserts pd.dataframe into db-table.
        
        Params:
        ------
        - df (pd.Dataframe): df to be inserted
        - table (str): name of table 
        - if_exists (str): 'fail', 'replace', 'append'
        - dtype (dict): dict of column('key'), SQLAlchemy datatype('value')-pairs (e.g. {'time': Integer()} )
        """
        def insert_custom(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            stmt = table.insert().values(data)
            stmt = stmt.on_conflict_do_noting()
            #stmt = stmt.on_duplicate_key_update(b=stmt.inserted.b, c=stmt.inserted.c)
            result = conn.execute(stmt)
            print(result.rowcount)
            return result.rowcount

        with self.engine.connect() as conn:
            df.to_sql(name = table, con=conn, if_exists= if_exists, method=insert_custom)



def main(db_url: str, filepath: str):

    # read the file into pandas
    df = pd.read_csv(gzip.open(filepath, 'rb'),index_col= False)

    # convert time to timestamp and set as index
    df.index = pd.to_datetime(df['time']).astype('int64').div(10**9).astype(int)
    print(df.head(10))

    #initalize the db class
    mydb = HomeMessagesDB('db/pythondqlite.db')
    Table_Name = 'p1g'

    'some_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('data', Integer, nullable=False,
           sqlite_on_conflict_not_null='FAIL')
)
    mydb.insert_df(df = df, table = "p1g")



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
        print("p1g.py -d sqlite:///myhome.db P1g-2022-01-01-2022-07-10.csv.gz")

    if sys.argv[1] == '-d':
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()

        main(sys.argv[2], sys.argv[3])

    if sys.argv[1] == '-r':
        pass