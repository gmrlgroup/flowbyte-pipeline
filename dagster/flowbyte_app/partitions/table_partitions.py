from flowbyte.sql import MSSQL
from dagster import StaticPartitionsDefinition, MultiPartitionsDefinition
import os

import sys
sys.path.append('..')
from modules import sql, log
import platform


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

    df['database_id'] = df['source_host'].str.replace(':', 'windows') + "/" + df['source_database'].astype(str)
    # df['database_id'] = df['source_host'].astype(str) + "/" + df['source_database'].astype(str)
    


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

    if df.empty:
        return ([], [], [])

    # df['source_database_id'] = df['source_host'].str.replace(':', 'windows') + "/" + df['source_database'].astype(str) + "/" + df['source_table'].astype(str)
    # df['destination_database_id'] = df['destination_host'].str.replace(':', 'windows') + "/" + df['destination_database'].astype(str) + "/" + df['destination_table'].astype(str)
    
    if platform.system() == "Windows":
        df['source_host'] = df['source_host'].str.replace(':', 'windows')
        df['source_host'] = df['source_host'].str.replace('\\\\', 'network\\backslash')
        df['destination_host'] = df['destination_host'].str.replace(':', 'windows')
        df['destination_host'] = df['destination_host'].str.replace('\\\\', 'network\\backslash')


    df['source_database_id'] = df.apply(lambda row: os.path.join(row['source_host'], str(row['source_database']), str(row['source_table'])), axis=1)
    df['destination_database_id'] = df.apply(lambda row: os.path.join(row['destination_host'], str(row['destination_database']), str(row['destination_table'])), axis=1)

    df['partition_key'] = df['destination_database_id'].astype(str) + "|" + df['source_database_id'].astype(str)





    








    source_databases = df['source_database_id'].unique().tolist() if df['source_database_id'].notnull().any() else []
    destination_databases = df['destination_database_id'].unique().tolist() if df['destination_database_id'].notnull().any() else []
    partition_keys = df['partition_key'].unique().tolist() if df['partition_key'].notnull().any() else []

    return source_databases, destination_databases, partition_keys






source_tables, destination_tables, partition_keys = get_tables('mssql', 'mssql') 

source_adls_tables, destination_duckdb_tables, partition_keys = get_tables('adls', 'duckdb')

source_db_tables, destination_duckdb_tables_2, partition_keys = get_tables('mssql', 'duckdb')

source_duckdb_tables, destination_duckdb_tables_3, partition_keys = get_tables('duckdb', 'duckdb')

source_duckdb_tables_2, destination_db_tables, partition_keys = get_tables('duckdb', 'mssql')

# database_partitions = StaticPartitionsDefinition(get_databases('mssql', 'mssql'))
# adls_duckdb_partitions = StaticPartitionsDefinition(get_databases('adls', 'duckdb'))

# table_partitions = StaticPartitionsDefinition(source_tables)
database_partitions = StaticPartitionsDefinition(get_databases('mssql', 'duckdb'))

source_table_partitions = StaticPartitionsDefinition(source_tables)
destination_table_partitions = StaticPartitionsDefinition(destination_tables)

# if source_adls_tables is not adls_source_table_partitions is none
adls_source_table_partitions = StaticPartitionsDefinition(source_adls_tables)
duckdb_dest_table_partitions = StaticPartitionsDefinition(destination_duckdb_tables)

db_source_table_partitions = StaticPartitionsDefinition(source_db_tables)
duckdb_dest_table_partitions_2 = StaticPartitionsDefinition(destination_duckdb_tables_2)

duckdb_source_table_partitions = StaticPartitionsDefinition(source_duckdb_tables)
duckdb_dest_table_partitions_3 = StaticPartitionsDefinition(destination_duckdb_tables_3)

duckdb_source_table_partitions_2 = StaticPartitionsDefinition(source_duckdb_tables_2)
db_dest_table_partitions = StaticPartitionsDefinition(destination_db_tables)

# Create two PartitionDefinitions
db_to_db_partitions = MultiPartitionsDefinition(
    {"source": source_table_partitions, "destination": destination_table_partitions}
)

adls_to_duckdb_partitions = MultiPartitionsDefinition(
    {"source": db_source_table_partitions, "destination": duckdb_dest_table_partitions_2}
)


adls_to_duckdb_partitions = MultiPartitionsDefinition(
    {"source": db_source_table_partitions, "destination": duckdb_dest_table_partitions_2}
)

duckdb_to_duckdb_partitions = MultiPartitionsDefinition(
    {"source": duckdb_source_table_partitions, "destination": duckdb_dest_table_partitions_3}
)

duckdb_to_db_partitions = MultiPartitionsDefinition(
    {"source": duckdb_source_table_partitions_2, "destination": db_dest_table_partitions}
)