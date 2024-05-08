from sqlalchemy import create_engine, MetaData, text, Table, Column, Float, Integer, String, PrimaryKeyConstraint, select, inspect
import pandas as pd

"""
This file defines the HomeMessagesDB-class to interact with SQLite db.

TO DO:
- prettify __repr__ method
- method to insert data (from df) into db-table
- method to delete tables and / or entire db
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

        return (f"Connected to: {self.url}, tables: {self.metadata.tables.keys()}")
        # To do: nicer printing + list columns and observations for each table in db

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
        
    def create_table(self, name: str, columns: list, columnTypes: list, primaryKey: str):
        """
        Define and create table in database.

        Usage: .create_table(name, columns, columnTypes)

        Params:
        ------
        - name (str): name of to-be-created table
        - colums (list): list of columns in table (e.g. ['a', 'b'])
        - columnTypes (list): list of SQLAlchemy data types (e.g. [Float, String])
        """
        if not inspect(self.engine).has_table(name):
            print(f"Creating {name} table")

            table = Table(
                name,
                self.metadata,
                *[Column(name, type) for name, type in zip(columns, columnTypes)],
                PrimaryKeyConstraint(primaryKey)      
            )
            self.metadata.create_all(self.engine)
        else:
            print(f"Database already has table named {name}")

    def delete_table():
        # add options to delete single tables and/or entire database?
        ...

    # ---- BROKEN? ------
    # def insert_df(self, df: pd.DataFrame, table: str, if_exists: str, dtype: dict):
    #     """
    #     Inserts pd.dataframe into db-table.
        
    #     Params:
    #     ------
    #     - df (pd.Dataframe): df to be inserted
    #     - table (str): name of table 
    #     - if_exists (str): 'fail', 'replace', 'append'
    #     - dtype (dict): dict of column('key'), SQLAlchemy datatype('value')-pairs (e.g. {'time': Integer()} )
    #     """
    #     df.to_sql(table, self.engine, if_exists=if_exists, dtype=dtype)

        


    