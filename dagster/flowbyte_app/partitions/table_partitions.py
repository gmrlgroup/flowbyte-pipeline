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







def get_databases(source_type, destination_type):

    sql_setup.connect()

    query = f"""SELECT  tm.[source_host],
                        tm.[source_database],
                        tm.[source_table],
                        tm.[source_api_endpoint],
                        tm.[destination_host],
                        tm.[destination_database],
                        tm.[destination_table],
                        tm.[destination_api_endpoint],
                        tm.[query],
                        tm.[is_attribute],
                        tm.[attribute_table_name],
                        tm.[temp_table_name],
                        tm.[is_incremental],
                        tm.[incremental_column]

                    FROM [dbo].[table_mapping] as tm
                    LEFT JOIN [data].[database] as source ON source.host = tm.source_host and source.[name] = tm.source_database
                    LEFT JOIN [data].[database] as destincation ON destincation.host = tm.destination_host and destincation.[name] = tm.destination_database

                    WHERE source.[type] = '{source_type}' AND destincation.[type] = '{destination_type}'
            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)

    if env == "DEV":
        log.log_info(df)

    df['database_id'] = df['source_host'].astype(str) + "/" + df['source_database'].astype(str)


    return df['database_id'].unique().tolist()



def get_tables(source_type, destination_type):

    sql_setup.connect()

    query = f"""SELECT  tm.[source_host],
                        tm.[source_database],
                        tm.[source_table],
                        tm.[source_api_endpoint],
                        tm.[destination_host],
                        tm.[destination_database],
                        tm.[destination_table],
                        tm.[destination_api_endpoint],
                        tm.[query],
                        tm.[is_attribute],
                        tm.[attribute_table_name],
                        tm.[temp_table_name],
                        tm.[is_incremental],
                        tm.[incremental_column]

                    FROM [dbo].[table_mapping] as tm
                    LEFT JOIN [data].[database] as source ON source.host = tm.source_host and source.[name] = tm.source_database
                    LEFT JOIN [data].[database] as destincation ON destincation.host = tm.destination_host and destincation.[name] = tm.destination_database

                    WHERE source.[type] = '{source_type}' AND destincation.[type] = '{destination_type}'
                    AND tm.[is_deleted] = 0
                    ORDER BY tm.[sequence]
            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)

    if env == "DEV":
        log.log_info(df)

    df['source_database_id'] = df['source_host'].astype(str) + "/" + df['source_database'].astype(str) + "/" + df['source_table'].astype(str)
    df['destination_database_id'] = df['destination_host'].astype(str) + "/" + df['destination_database'].astype(str) + "/" + df['destination_table'].astype(str)
    df['partition_key'] = df['destination_database_id'].astype(str) + "|" + df['source_database_id'].astype(str)

    source_databases = df['source_database_id'].unique().tolist()
    destination_databases = df['destination_database_id'].unique().tolist()
    partition_keys = df['partition_key'].unique().tolist()

    return source_databases, destination_databases, partition_keys






source_tables, destination_tables, partition_keys = get_tables('mssql', 'mssql')

source_adls_tables, destination_duckdb_tables, partition_keys = get_tables('adls', 'duckdb')

database_partitions = StaticPartitionsDefinition(get_databases('mssql', 'mssql'))
adls_duckdb_partitions = StaticPartitionsDefinition(get_databases('adls', 'duckdb'))

table_partitions = StaticPartitionsDefinition(source_tables)

source_table_partitions = StaticPartitionsDefinition(source_tables)
destination_table_partitions = StaticPartitionsDefinition(destination_tables)

adls_table_partitions = StaticPartitionsDefinition(source_adls_tables)
duckdb_table_partitions = StaticPartitionsDefinition(destination_duckdb_tables)

# Create two PartitionDefinitions
db_to_db_partitions = MultiPartitionsDefinition(
    {"source": source_table_partitions, "destination": destination_table_partitions}
)

adls_to_duckdb_partitions = MultiPartitionsDefinition(
    {"source": adls_table_partitions, "destination": duckdb_table_partitions}
)

