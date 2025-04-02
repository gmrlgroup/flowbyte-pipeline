from flowbyte.sql import MSSQL
from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition
import os

import sys
sys.path.append('..')
from modules import sql, log


env = os.getenv('ENV')

server, database, username, password = sql.get_connection_details("SETUP")
sql_setup = MSSQL(
    host=server,
    username=username,
    password=password,
    database=database,
    driver="ODBC Driver 17 for SQL Server",
    connection_type="sqlalchemy"

    )







def get_databases(database_type):

    sql_setup.connect()

    query = f"""SELECT [host]
                        ,[name]
                        ,[type]
                        FROM [data].[database]
                        where [type] = '{database_type}'
            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)

    if env == "DEV":
        log.log_info(df)

    df['database_id'] = df['host'].astype(str) + "|" + df['name'].astype(str)


    return df['database_id'].unique().tolist()




mssql_database_partitions = StaticPartitionsDefinition(get_databases('mssql'))