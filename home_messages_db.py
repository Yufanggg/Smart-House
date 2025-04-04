from sqlalchemy import create_engine, MetaData, text, Table, Column, inspect
import pandas as pd
from sqlite3 import Error
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import sessionmaker
from typing import Any

"""
This file defines the HomeMessagesDB-class to interact with SQLite db.

TO DO:
- error handling
"""


class HomeMessagesDB:
    def __init__(self, url: str):
        self.url = url
        self.engine = create_engine(url)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

    def __repr__(self):
        """ More informative __repr__. """
        self.metadata.reflect(bind=self.engine)

        preamble = f"A HomeMessagesDB-class instance. Connected to: {self.url} \n\nTABLES \t\tCOLUMNS\n\n"

        table_info = (
            "\n".join(f"{i :<10}\t{[column.key for column in self.metadata.tables[i].c]}" for i in
                      self.metadata.tables.keys())
        )

        return preamble + table_info

    def read_model_df(self, model: Any, filter_stmt: Any) -> pd.DataFrame:
        """
        Query a SqlAlchemy model and return it's filtered content in a dataframe

        :param model: sqlAlchemy model
        :param filter_stmt: sqlAlchemy filter statement
        :return:
        """
        Session = sessionmaker(bind=self.engine)
        with Session() as session:
            df = pd.read_sql(session.query(model).filter(filter_stmt).statement, session.bind)
        return df

    def query(self, stmt, rawSQL: bool = False, as_df: bool = False):
        """
        Query the database.

        Usage: .query(stmt, rawSQL=False, as_df=False)

        Params:
        ------
        - stmt: SQLAlchemy construct or (if rawSQL=True) 'raw' SQL expression
        - rawSQL (bool): if True, use direct SQL expression (e.g. 'SELECT * from...')
        - as_df (bool): if True, returns data as pd.df

        Returns:
        -------
        Data as list of tuples or (if as_df=True) as pd.Dataframe.
        """

        if rawSQL:
            with self.engine.connect() as conn:
                result = conn.execute(text(stmt)).fetchall()
        else:
            with self.engine.connect() as conn:
                result = conn.execute(stmt).fetchall()

        if as_df:
            return pd.DataFrame(result)
        else:
            return result

    def create_table(self, name: str, columns: list):
        """
        Define and create table in database.

        Usage: .create_table(name, columns)

        Params:
        ------
        - name (str): name of to-be-created table
        - colums (list): list of SQLAlchemy columns
            ex.
            [

            ]
        - columnTypes (list): list of SQLAlchemy data types (e.g. [Float, String])
        - primaryKey (str): name of column to be labeled as primary key
        """
        try:
            if inspect(self.engine).has_table(name):
                print(f"Database already has table named {name}")

            else:
                print(f"Creating {name} table")
                Table(
                    name,
                    self.metadata,
                    *columns,
                )
                self.metadata.create_all(self.engine)
                self.metadata.reflect(bind=self.engine)
                print(f"{name} table was successfully created")

        except Error as e:
            print(e)

    def delete_table(self, name):
        """
        Delete a table from database.

        Usage: .delete_table(name)

        Params:
        ------
        - name (str): name of to-be-deleted table
        """
        try:
            if not inspect(self.engine).has_table(name):
                print(f"Table named {name} not in database.")
            else:
                with self.engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {name}"))
                    self.metadata.clear()  # seems necessary, otherwise dropped table seems to stick in metadata (https://github.com/sqlalchemy/sqlalchemy/issues/5112)
                    self.metadata.reflect(bind=self.engine)

        except Error as e:
            print(e)

    def insert_df(self, df: pd.DataFrame, table: str, dtype: dict, if_exists: str = 'append', type:str = "raw", chunk_size: int = 500):
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
            stmt = insert(table.table).values(data)
            stmt = stmt.on_conflict_do_nothing()
            result = conn.execute(stmt)
            print("number of rows newly storted")
            print(result.rowcount)
            return result.rowcount

        if type == "raw":
            with self.engine.connect() as conn:
                df.to_sql(name=table, con=conn, index=False,
                          if_exists=if_exists, method=insert_custom,
                                                dtype=dtype, chunksize=chunk_size)
        elif type == "clean":
            pass
