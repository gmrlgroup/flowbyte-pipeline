import os
import pandas as pd
from dagster import MetadataValue, Output, asset, StaticPartitionsDefinition, MultiPartitionKey
from flowbyte.sql import MSSQL
import os
import duckdb
from flowbyte_app.partitions import adls_to_duckdb_partitions


import sys
sys.path.append('..')
from modules import sql, log, models


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




# table_partitions = StaticPartitionsDefinition(get_tables('mssql', 'duckdb'))




@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=adls_to_duckdb_partitions)
def get_table_mapping_duckdb(context):
    """
    Get Table Mappings
    """


    partition_key: MultiPartitionKey = context.partition_key.keys_by_dimension

    source_key = partition_key["source"]
    destination_key = partition_key["destination"]

    source_host = "/".join(source_key.split("/")[:-2])
    if 'file.core.windows.net' not in source_host:
        source_host = source_host.replace('windows', ':')
    source_db = source_key.split("/")[-2]
    table_name = source_key.split("/")[-1]

    destination_host = "/".join(destination_key.split("/")[:-2])
    if 'file.core.windows.net' not in destination_host:
        destination_host = destination_host.replace('windows', ':')
    destination_db = destination_key.split("/")[-2]
    destination_table_name = destination_key.split("/")[-1]


    sql_setup.connect()

    query = f"""SELECT  tm.[source_host],
                        tm.[source_database],
                        tm.[source_table],
                        tm.[source_api_endpoint],
                        tm.[destination_host],
                        tm.[destination_database],
                        tm.[destination_table],
                        tm.[destination_schema],
                        tm.[destination_api_endpoint],
                        tm.[query],
                        tm.[is_attribute],
                        tm.[attribute_table_name],
                        tm.[temp_table_name],
                        tm.[is_incremental],
                        tm.[incremental_column],
                        tm.[source_schema]

            FROM [dbo].[table_mapping] as tm
            LEFT JOIN [data].[database] as source ON source.host = tm.source_host and source.[name] = tm.source_database
            LEFT JOIN [data].[database] as destincation ON destincation.host = tm.destination_host and destincation.[name] = tm.destination_database

            WHERE [source_table] = '{table_name}' AND source_host = '{source_host}' AND source_database = '{source_db}'
            AND [destination_table] = '{destination_table_name}' AND destination_host = '{destination_host}' AND destination_database = '{destination_db}'
            AND tm.[is_deleted] = 0

            AND source.[type] = 'mssql' AND destincation.[type] = 'duckdb'

            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)
    

    if env == "DEV":
        log.log_info(df)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=adls_to_duckdb_partitions)
def get_field_mapping_duckdb(context):
    """
    Get Field Mappings
    """


    partition_key: MultiPartitionKey = context.partition_key.keys_by_dimension

    source_key = partition_key["source"]
    destination_key = partition_key["destination"]

    source_host = "/".join(source_key.split("/")[:-2])
    if 'file.core.windows.net' not in source_host:
        source_host = source_host.replace('windows', ':')
    source_db = source_key.split("/")[-2]
    table_name = source_key.split("/")[-1]

    destination_host = "/".join(destination_key.split("/")[:-2])
    if 'file.core.windows.net' not in destination_host:
        destination_host = destination_host.replace('windows', ':')
    destination_db = destination_key.split("/")[-2]
    destination_table_name = destination_key.split("/")[-1]


    sql_setup.connect()


    query = f"""SELECT    fm.[source_host],
                        fm.[source_database],
                        fm.[source_table],
                        fm.[source_column],
                        fm.[destination_host],
                        fm.[destination_database],
                        fm.[destination_table],
                        fm.[destination_column],
                        fm.[source_data_type],
                        fm.[destination_data_type],
                        fm.[is_group_by],
                        fm.[is_sum],
                        fm.[is_count],
                        fm.[filter_query],
                        fm.[default_value],
                        fm.[is_attribute],
                        fm.[is_attribute_key],
                        fm.[is_primary_key]

                FROM [dbo].[field_mapping] as fm
                    LEFT JOIN [data].[database] as source ON source.host = fm.source_host and source.[name] = fm.source_database
                    LEFT JOIN [data].[database] as destincation ON destincation.host = fm.destination_host and destincation.[name] = fm.destination_database

                WHERE [source_table] = '{table_name}' AND source_host = '{source_host}' AND source_database = '{source_db}'
                AND [destination_table] = '{destination_table_name}' AND destination_host = '{destination_host}' AND destination_database = '{destination_db}'


                AND source.[type] = 'mssql' AND destincation.[type] = 'duckdb'
            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)

    if env == "DEV":
        log.log_info(df)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)




@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="extract", io_manager_key="parquet_io_manager", partitions_def=adls_to_duckdb_partitions)
def get_source_data_duckdb(context, get_table_mapping_duckdb, get_field_mapping_duckdb, config: models.QueryModel):
    """
    Get Data from Source
    """

    log.log_debug(f'Table Mapping: {get_table_mapping_duckdb}')
    table_mapping = get_table_mapping_duckdb
    table_mapping_no_attribute = table_mapping[table_mapping['is_attribute'] == 0]

    destination_host = table_mapping_no_attribute['destination_host'].iloc[0]
    destination_database = table_mapping_no_attribute['destination_database'].iloc[0]
    source_host = table_mapping_no_attribute['source_host'].iloc[0]
    source_database = table_mapping_no_attribute['source_database'].iloc[0]
    source_schema = table_mapping_no_attribute['source_schema'].iloc[0]

    if 'file.core.windows.net' not in source_host:
        source_host = source_host.replace('windows', ':')
    if 'file.core.windows.net' not in destination_host:
        destination_host = destination_host.replace('windows', ':')
    
    
    # get db credentials where database name is equal to the source database and host is equal to the source host
    df_credentials = sql.get_db_credentials()
    db_credentials_source = df_credentials[(df_credentials['database_name'] == source_database) & (df_credentials['host'] == source_host)]
    db_credentials_dest = df_credentials[(df_credentials['database_name'] == destination_database) & (df_credentials['host'] == destination_host)]
    

    sql_source = sql.init_sql(db_credentials_source)
    sql_source.connect()


    sql_destination = sql.init_sql(db_credentials_dest)
    sql_destination.connect()

    # check if table is_attribute is True
    is_incremental = table_mapping['is_incremental'].iloc[0]
    incremental_col = table_mapping['incremental_column'].iloc[0]

    field_mapping = get_field_mapping_duckdb

    # convert field_mapping to dictionary
    mapping_data = field_mapping.to_dict(orient='records')

    if env == "DEV":
        log.log_debug(f"Mapping Data: {mapping_data}")


    # check if the query in QueryModel is not empty
    if config.query:
        query = config.query

    # check if the query in table mapping is not empty and contains SELECT and FROM
    elif table_mapping['query'].iloc[0] and "SELECT" in table_mapping['query'].iloc[0] and "FROM" in table_mapping['query'].iloc[0]:
        # get the query from the table mapping
        query = table_mapping['query'].iloc[0]

    else:
        if is_incremental:
            incremental_col = table_mapping['incremental_column'].iloc[0]
            max_incremental_value = sql.get_max_incremental_value(sql=sql_destination, table_mapping=table_mapping_no_attribute, incremental_col=incremental_col)
            query = sql.generate_query(table_mapping_no_attribute, mapping_data, incremental_col, max_incremental_value, schema=source_schema)

        else:
            query = sql.generate_query(table_mapping_no_attribute, mapping_data, schema=source_schema)
            if config.where:
                query += f"WHERE {config.where}"

            log.log_info(f"Non Incremental Query: {query}")
    
    
    if env == "DEV":
        log.log_debug(f"Query: {query}")


    object_columns = []
    float_columns = []
    integer_columns = []
    # obcjec columns start with NVARCHAR
    object_columns = field_mapping[field_mapping['source_data_type'].str.contains('TEXT')]
    object_columns = object_columns['source_column'].tolist()

    # int columns contains INT
    integer_columns = field_mapping[field_mapping['source_data_type'].str.contains('INT')]
    integer_columns = integer_columns['source_column'].tolist()

    # float columns contains DECIMAL
    float_columns = field_mapping[field_mapping['source_data_type'].str.contains('DECIMAL')]
    float_columns = float_columns['source_column'].tolist()


    if env == "DEV":
        log.log_debug(f"Object Columns: {object_columns}")
        log.log_debug(f"Integer Columns: {integer_columns}")
        log.log_debug(f"Float Columns: {float_columns}")

        log.log_debug(f"HOST: {sql_source.host}")
        log.log_debug(f"DATABASE: {sql_source.database}")

    df = sql_source.get_data(query, chunksize=100000, object_columns=object_columns, float_columns=float_columns, integer_columns=integer_columns, progress_callback=sql.print_progress, message="Extracted ")

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)


@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="transform", io_manager_key="parquet_io_manager", partitions_def=adls_to_duckdb_partitions)
def transform_data_duckdb(context, get_table_mapping_duckdb, get_field_mapping_duckdb, get_source_data_duckdb):
    """
    Transform Data
    """

    df = get_source_data_duckdb

    log.log_info(df)

    if df is None or  df.empty:
        return Output(value=df)

    field_mapping = get_field_mapping_duckdb

    log.log_info(field_mapping)

    table_name = field_mapping['destination_table'].iloc[0]

    field_mapping = field_mapping[
        (field_mapping['destination_table'] == table_name)
    ]

    # Assume field_mapping is a DataFrame with columns 'source_column' and 'destination_column'
    mapping_dict = dict(zip(field_mapping['source_column'], field_mapping['destination_column']))

    # Rename the columns in df based on the mapping
    df = df.rename(columns=mapping_dict)

    # Get the list of destination columns
    destination_cols = field_mapping['destination_column'].tolist()


    df = df[destination_cols]


    # # rename columns
    # columns = field_mapping['destination_column'].tolist()

    # log.log_info(columns)

    # for i in range(len(columns)):
    #     df.rename(columns={columns[i]: field_mapping['destination_column'].iloc[i]}, inplace=1)

    # # remove columns that are not in the field mapping
    # df = df[columns]


    # convert columns
    # group the columns in dict based on distinct values of the destination_data_type
    columns = field_mapping['destination_column'].tolist()
    data_types = field_mapping['destination_data_type'].unique()
    column_data_types = {}

    # group the columns based on the data type in the field mapping in colum_data_types
    for data_type in data_types:
        column_data_types[data_type] = field_mapping[field_mapping['destination_data_type'] == data_type]['destination_column'].tolist()

    if 'VARCHAR' in column_data_types:
        text_columns = column_data_types['VARCHAR']

    # check if column_data_types containsa key that has INT in it: ex INTEGER, BIGINT, INT
    integer_keys = [key for key in column_data_types if "INT" in key]
    if  integer_keys:
        
        integer_columns = column_data_types[integer_keys[0]]

        
        for col in integer_columns:
            # replace null with 0
            df[col] = df[col].fillna(0)
            df[col] = df[col].replace("", 0)
            df[col] = df[col].astype("int64")

    if 'DECIMAL' in column_data_types:
        decimal_columns = column_data_types['DECIMAL']

        for col in decimal_columns:
            # replace null with 0
            df[col] = df[col].fillna(0)
            df[col] = df[col].replace("", 0)
            df[col] = df[col].astype("float64")

    if 'BOOLEAN' in column_data_types:
        boolean_columns = column_data_types['BOOLEAN']

        for col in boolean_columns:
            df[col] = df[col].fillna(0)
            df[col] = df[col].replace("", 0)
            df[col] = df[col].astype("bool")


    metadata = {
        "columns": MetadataValue.md(", ".join(df.columns))
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=adls_to_duckdb_partitions)
def add_destination_data_duckdb(context, get_table_mapping_duckdb, get_field_mapping_duckdb, transform_data_duckdb):
    """
    Add Data to Destination
    """

    

    df = transform_data_duckdb

    log.log_info(f"INFO: {df.dtypes}")

    if df is None or  df.empty:
        return Output(value=df)
    
    # Get table_name for partition
    
    table_mapping = get_table_mapping_duckdb[get_table_mapping_duckdb['is_attribute'] == 0]
    # field_mapping = get_field_mapping_duckdb

    destination_host = table_mapping['destination_host'].iloc[0]
    destination_database = table_mapping['destination_database'].iloc[0]

    if 'file.core.windows.net' not in destination_host:
        destination_host = destination_host.replace('windows', ':')

    df_credentials = sql.get_db_credentials()
    db_credentials = df_credentials[(df_credentials['database_name'] == destination_database) & (df_credentials['host'] == destination_host)]

    log.log_info(db_credentials)

    table_name = table_mapping['destination_table'].iloc[0]
    is_incremental = table_mapping['is_incremental'].iloc[0]
    temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
    schema = table_mapping['destination_schema'].iloc[0]
    temp_table_name = table_mapping['temp_table_name'].iloc[0]

    db = f"{destination_host}/{destination_database}.duckdb"
    # db = f"{destination_database}"
    con = duckdb.connect(db)  # Use ":memory:" for in-memory DB
    # sql_destination = init_sql(db_credentials)
    # sql_destination.connect()




    schema_parts = []
    for col, dtype in df.dtypes.items():
        if "float" in str(dtype):
            schema_parts.append(f'"{col}" DECIMAL(38,20)')  # Force float columns to DOUBLE

        elif "int" in str(dtype):
            schema_parts.append(f'"{col}" INT64')
            
        elif "datetime" in str(dtype):
            schema_parts.append(f'"{col}" TIMESTAMP')
        # if boolean
        elif "bool" in str(dtype):
            schema_parts.append(f'"{col}" BOOLEAN')
        else:
            schema_parts.append(f'"{col}" STRING')

    schema_sql = ", ".join(schema_parts)

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_sql});"

    log.log_info(f"Creating Table: {create_table_sql}")

    con.execute(create_table_sql)
    
    
    
    # get list of destination columns from field mapping
    columns = df.columns.tolist()




    # check if is_incremental is True
    if is_incremental:
        # change the schema to tmp and define the temp table name and the target table name
        # This will create a temp table and insert the data into it
       
        # source_table_name = f"{temp_schema}.{temp_table_name}"
        # target_table_name = f"{schema}.{table_name}"

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name}_temp ({schema_sql});"

        con.execute(create_table_sql)

        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

        # delete data that are in table_name_temp and table_name both
        con.execute(f"DELETE FROM {table_name} WHERE EXISTS (SELECT 1 FROM {table_name}_temp WHERE {table_name}_temp.id = {table_name}.id)")

        con.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_temp")

        
        # truncate data from temp table
        log.log_info("Truncating Data from Temp Table")
        # sql_destination.truncate_table(schema_name=temp_schema, table_name=temp_table_name)
        
        # log.log_info("Inserting Data into Temp Table")
        # sql_destination.insert_data(schema=temp_schema, table_name=temp_table_name, insert_records=df, chunksize=10000)

        # # get destination columns from field mapping that have is_primary_key = True
        # primary_keys = field_mapping[field_mapping['is_primary_key'] == 1]['destination_column'].tolist()

        # # update the target table from the temp table
        # log.log_info("Updating Data in Target Table")
        # sql_destination.upsert_from_table(df=df, target_table=target_table_name, source_table=source_table_name, key_columns=primary_keys, delete_not_matched=False)

        

    else:
        log.log_info("Inserting Data into Table")
        # sql_destination.insert_data(schema=schema, table_name=table_name, insert_records=df, chunksize=10000)

        con.execute(f"INSERT INTO {table_name} SELECT * FROM df")

        con.close()


    metadata = {
        "columns": MetadataValue.md(", ".join(columns)),
        "rows_count": str(len(df)),
        "columns_count": str(len(columns))
    }

    return Output(value=None, metadata=metadata)






