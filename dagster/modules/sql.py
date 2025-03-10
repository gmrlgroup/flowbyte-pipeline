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







def get_db_credentials():


    server, database, username, password = get_connection_details("SETUP")

    sql_setup = MSSQL(
        host=server,
        username=username,
        password=password,
        database=database,
        driver="ODBC Driver 17 for SQL Server",
        connection_type="sqlalchemy"

        )
    
    query = f"""SELECT * FROM [data].[database_credentials]"""

    sql_setup.connect()

    df = sql_setup.get_data(query, chunksize=1000)

    return df



def init_sql(db_credentials):
    host = db_credentials['host'].iloc[0]
    database = db_credentials['database_name'].iloc[0]
    username = db_credentials['username'].iloc[0]
    password = db_credentials['password'].iloc[0]

    # server, database, username, password = sql.get_connection_details("SOURCE")
    sql = MSSQL(
        host=host,
        username=username,
        password=password,
        database=database,
        driver="ODBC Driver 17 for SQL Server",
        connection_type="sqlalchemy"

        )
    
    return sql

# def connect(server, database, user, password, connection_type="pyodbc"):
#     if connection_type == "pyodbc":
#         conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=" + server + ";DATABASE=" + database + ";UID=" + user + ";PWD=" + password +";CHARSET=UTF8")
#     elif connection_type == "sqlalchemy":
#         connect_string = urllib.parse.quote_plus(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={user};PWD={password};CHARSET=UTF8")
#         conn = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connect_string}', fast_executemany=True)
    
#     return conn



def get_max_incremental_value(sql: MSSQL, table_mapping, incremental_col):

    log_info("Getting Max Incremental Value from Destination Table")
    
    # Get table name
    table = table_mapping['destination_table'].iloc[0]

    # Create the query to get max incremental value
    query = f"SELECT MAX({incremental_col}) as cdc_key FROM {table}"

    int_columns = [incremental_col]

    # Get max incremental value
    max_value = sql.get_data(query, integer_columns=int_columns)

    log_info(max_value)

    # if max_value.empty:
    if pd.isna(max_value.loc[0, 'cdc_key']):
        return 0

    cdc_key = max_value['cdc_key'].iloc[0]

    return cdc_key



def generate_query(table_mapping, field_mappings, incremental_col=None, max_incremental_value=None, schema=None):


    
    main_columns = []      # columns coming from the main table (alias i)
    dest_table = table_mapping['source_table'].iloc[0]
    if schema:
        dest_table = f"[{schema}].[{dest_table}]"
    else:
        dest_table = f"[dbo].[{dest_table}]"

    for mapping in field_mappings:
        src_col = mapping["source_column"]
        dest_col = mapping["destination_column"]
        

        main_columns.append(f"i.[{src_col}] AS [{dest_col}]")

    # Build the SELECT clause by concatenating main and attribute columns.
    select_clause = ", ".join(main_columns) #+ attribute_columns)
    
    # Build the FROM clause. We assume that both tables are in the same database
    query = f"SELECT {select_clause}\n"
    query += f"FROM {dest_table} i\n"

    if incremental_col:
        query += f"WHERE i.{incremental_col} > {max_incremental_value}\n"
    
    
    return query




def print_progress(records, message="Extracted records so far"):
    log_info(f"{message}: {records}")