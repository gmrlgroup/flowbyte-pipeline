import pyodbc
import sqlalchemy
from sqlalchemy.dialects.mssql import dialect
from sqlalchemy import create_engine, MetaData, Table, bindparam
from sqlalchemy import MetaData, and_
import pandas as pd
import urllib
import os
import pyarrow as pa
import pyarrow.parquet as pq
from .log import *
from flowbyte.sql import MSSQL


def get_sql(prefix):
    """
    Create SQL Object
    """

    # get GMRLDW_SERVER from environment
    server = os.getenv(f'{prefix}_SERVER')
    database = os.getenv(f'{prefix}_DATABASE')
    user = os.getenv(f'{prefix}_USER')
    password = os.getenv(f'{prefix}_PASSWORD')

    sql = MSSQL(
        connection_type="pyodbc",
        host = server,
        database = database,
        username = user,
        password = password,
        driver = "ODBC Driver 17 for SQL Server"
    )

    sql.connect()


    return sql

def get_connection_details(prefix):
    server = os.getenv(f"{prefix}_SERVER")
    database = os.getenv(f"{prefix}_DATABASE")
    username = os.getenv(f"{prefix}_USER")
    password = os.getenv(f"{prefix}_PASSWORD")

    return server, database, username, password



def connect(server, database, user, password, connection_type="pyodbc"):
    if connection_type == "pyodbc":
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + server + ";DATABASE=" + database + ";UID=" + user + ";PWD=" + password +";CHARSET=UTF8")
    elif connection_type == "sqlalchemy":
        connect_string = urllib.parse.quote_plus(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={user};PWD={password};CHARSET=UTF8")
        conn = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connect_string}', fast_executemany=True)
    
    return conn

