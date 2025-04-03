import os
import pandas as pd
from dagster import MetadataValue, Output, asset, StaticPartitionsDefinition, MultiPartitionKey
from flowbyte.sql import MSSQL
import os
import duckdb
from flowbyte_app.partitions import duckdb_to_db_partitions


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

@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
def get_db_credentials_1():
    
    query = f"""SELECT * FROM [dbo].[database_credential]"""

    sql_setup.connect()

    df = sql_setup.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)


@asset(owners=["romy.bouabdo@gmrlgroup.com", "team:data-sci"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
def get_table_mapping_duckdb_db(context):
    """
    Get Table Mappings
    """


    partition_key: MultiPartitionKey = context.partition_key.keys_by_dimension

    source_key = partition_key["source"]
    destination_key = partition_key["destination"]

    # c:/duckdb/storage/sales_dataset/sales_line
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

            AND source.[type] = 'duckdb' AND destincation.[type] = 'mssql'

            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)
    log.log_info(df)
    

    if env == "DEV":
        log.log_info(df)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["romy.bouabdo@gmrlgroup.com", "team:data-sci"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
def get_field_mapping_duckdb_db(context):
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


                AND source.[type] = 'duckdb' AND destincation.[type] = 'mssql'
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




@asset(owners=["romy.bouabdo@gmrlgroup.com", "team:data-sci"], compute_kind="sql", group_name="extract", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
def get_source_data_duckdb_db(context, get_db_credentials_1, get_table_mapping_duckdb_db, get_field_mapping_duckdb_db, config: models.QueryModel):
    """
    Get Data from Source
    """

    # partition_key: MultiPartitionKey = context.partition_key.keys_by_dimension
    # destination_key = partition_key["destination"]
    # destination_table_name = destination_key.split("/")[-1]

    log.log_debug(f'Table Mapping: {get_table_mapping_duckdb_db}')
    table_mapping = get_table_mapping_duckdb_db
    table_mapping_no_attribute = table_mapping[table_mapping['is_attribute'] == 0]

    destination_host = table_mapping_no_attribute['destination_host'].iloc[0]
    destination_database = table_mapping_no_attribute['destination_database'].iloc[0]
    source_host = table_mapping_no_attribute['source_host'].iloc[0]
    source_database = table_mapping_no_attribute['source_database'].iloc[0]
    source_schema = table_mapping_no_attribute['source_schema'].iloc[0]
    destination_table_name = table_mapping_no_attribute['destination_table'].iloc[0]
    
    
    # get db credentials where database name is equal to the source database and host is equal to the source host
    # df_credentials = sql.get_db_credentials()
    df_credentials = get_db_credentials_1
    log.log_info(f"DB Credentials: {df_credentials}")
    log.log_info(f"DATABASE NAME: {destination_database}, HOST: {destination_host}")
    # db_credentials_source = df_credentials[(df_credentials['database_name'] == source_database) & (df_credentials['host'] == source_host)]
    db_credentials_dest = df_credentials[(df_credentials['database_name'] == destination_database) & (df_credentials['host'] == destination_host)]
    log.log_info(f"DB Credentials Destination: {db_credentials_dest}")

    # replace cwindows to C:
    if 'file.core.windows.net' not in source_host:
        source_host = source_host.replace('windows', ':')
    if 'file.core.windows.net' not in destination_host:
        destination_host = destination_host.replace('windows', ':')

    if 'file.core.windows.net' in source_host:
        source_db = f"{source_host}\{source_database}.duckdb"
    else:
        source_db = f"{source_host}/{source_database}.duckdb"
    log.log_info(f"Source DB: {source_db}")
    


    source_con = duckdb.connect(source_db, read_only = False)
    # destination_con = duckdb.connect(destination_db, read_only = False)


    sql_destination = sql.init_sql(db_credentials_dest)
    sql_destination.connect()

    # check if table is_attribute is True
    is_incremental = table_mapping['is_incremental'].iloc[0]
    incremental_col = table_mapping['incremental_column'].iloc[0]

    field_mapping = get_field_mapping_duckdb_db

    # convert field_mapping to dictionary
    mapping_data = field_mapping.to_dict(orient='records')

    if env == "DEV":
        log.log_debug(f"Mapping Data: {mapping_data}")


    # check if the query in QueryModel is not empty
    if config.query:
        query = config.query

    # elif config.where:
    #     query_where = config.where

    # check if the query in table mapping is not empty and contains SELECT and FROM
    elif table_mapping['query'].iloc[0] and "SELECT" in table_mapping['query'].iloc[0] and "FROM" in table_mapping['query'].iloc[0]:
        # get the query from the table mapping
        query = table_mapping['query'].iloc[0]

    else:
        if is_incremental:
            incremental_col = table_mapping['incremental_column'].iloc[0]
            # max_incremental_value = sql.get_max_incremental_value(sql=sql_destination, table_mapping=table_mapping_no_attribute, incremental_col=incremental_col)
            max_incremental_query = f"""
            SELECT MAX({incremental_col}) as cdc_key FROM {destination_table_name}
            """
            log.log_info(f"Max Incremental Query: {max_incremental_query}")
            max_incremental_df = sql_destination.get_full_data(max_incremental_query)
            max_incremental_value = max_incremental_df['cdc_key'].iloc[0]
            query = sql.generate_duckdb_query(table_mapping_no_attribute, mapping_data, incremental_col, max_incremental_value, schema=source_schema)

        else:
            query = sql.generate_duckdb_query(table_mapping_no_attribute, mapping_data, schema=source_schema)
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

        # log.log_debug(f"HOST: {sql_source.host}")
        # log.log_debug(f"DATABASE: {sql_source.database}")

    # df = sql_source.get_data(query, chunksize=100000, object_columns=object_columns, float_columns=float_columns, integer_columns=integer_columns, progress_callback=sql.print_progress, message="Extracted ")
    df = source_con.execute(query).fetchdf()
    source_con.close()
    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)


@asset(owners=["romy.bouabdo@gmrlgroup.com", "team:data-sci"], compute_kind="sql", group_name="transform", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
def transform_data_duckdb_db(context, get_table_mapping_duckdb_db, get_field_mapping_duckdb_db, get_source_data_duckdb_db):
    """
    Transform Data
    """

    df = get_source_data_duckdb_db

    log.log_info(df)

    if df is None or  df.empty:
        return Output(value=df)

    field_mapping = get_field_mapping_duckdb_db

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



@asset(owners=["romy.bouabdo@gmrlgroup.com", "team:data-sci"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
def add_destination_data_duckdb_db(context, get_db_credentials_1, get_table_mapping_duckdb_db, get_field_mapping_duckdb_db, transform_data_duckdb_db):
    """
    Add Data to Destination
    """

    

    df = transform_data_duckdb_db

    log.log_info(f"INFO: {df.dtypes}")

    if df is None or  df.empty:
        return Output(value=df)
    
    # Get table_name for partition
    
    table_mapping = get_table_mapping_duckdb_db[get_table_mapping_duckdb_db['is_attribute'] == 0]
    field_mapping = get_field_mapping_duckdb_db

    destination_host = table_mapping['destination_host'].iloc[0]
    destination_database = table_mapping['destination_database'].iloc[0]

    # df_credentials = sql.get_db_credentials()
    df_credentials = get_db_credentials_1
    db_credentials = df_credentials[(df_credentials['database_name'] == destination_database) & (df_credentials['host'] == destination_host)]

    log.log_info(db_credentials)

    table_name = table_mapping['destination_table'].iloc[0]
    is_incremental = table_mapping['is_incremental'].iloc[0]
    temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
    schema = table_mapping['destination_schema'].iloc[0]
    temp_table_name = table_mapping['temp_table_name'].iloc[0]
    
    
    # sql_destination.connect()
    sql_destination = sql.init_sql(db_credentials)
    sql_destination.connect()
    
    # get list of destination columns from field mapping
    columns = df.columns.tolist()

    if env == "DEV":
        log.log_debug(f"table_mapping: {table_mapping}")
        log.log_debug(f"field_mapping: {field_mapping}")
        log.log_debug(f"table_name: {table_name}")
        log.log_debug(f"columns: {columns}")
        log.log_debug(f"is_incremental: {is_incremental}")
        log.log_debug(f"temp_schema: {temp_schema}")
        log.log_debug(f"temp_table_name: {temp_table_name}")



    # check if is_incremental is True
    if is_incremental:
        # change the schema to tmp and define the temp table name and the target table name
        # This will create a temp table and insert the data into it
       
        source_table_name = f"{temp_schema}.{temp_table_name}"
        target_table_name = f"{schema}.{table_name}"

        log.log_info(f"SOURCE TABLE NAME: {source_table_name}")
        log.log_info(f"TARGET TABLE NAME: {target_table_name}")

        # truncate data from temp table
        log.log_info("Truncating Data from Temp Table")
        sql_destination.truncate_table(schema_name=temp_schema, table_name=temp_table_name)
        
        log.log_info("Inserting Data into Temp Table")
        sql_destination.insert_data(schema=temp_schema, table_name=temp_table_name, insert_records=df, chunksize=10000)

        # get destination columns from field mapping that have is_primary_key = True
        primary_keys = field_mapping[field_mapping['is_primary_key'] == 1]['destination_column'].tolist()

        # update the target table from the temp table
        log.log_info("Updating Data in Target Table")
        sql_destination.upsert_from_table(df=df, target_table=target_table_name, source_table=source_table_name, key_columns=primary_keys, delete_not_matched=False)

        

    else:
        log.log_info("Inserting Data into Table")
        log.log_info(f"SCHEMA: {schema}, TABLE NAME: {table_name}")
        sql_destination.insert_data(schema=schema, table_name=table_name, insert_records=df, chunksize=10000)


    metadata = {
        "columns": MetadataValue.md(", ".join(columns)),
        "rows_count": str(len(df)),
        "columns_count": str(len(columns))
    }

    return Output(value=None, metadata=metadata)

    
# @asset(owners=["romy.bouabdo@gmrlgroup.com", "team:data-sci"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=duckdb_to_db_partitions)
# def add_destination_attributes_duckdb_db(context, get_table_mapping_duckdb_db, get_field_mapping_duckdb_db, transform_data_duckdb_db):
#     """
#     Add Attributes to Destination
#     """

#     df = transform_data_duckdb_db

#     if df is None or  df.empty:
#         return Output(value=df)

#     # get the first destination_table from the field mapping
#     table_mapping = get_table_mapping_duckdb_db[get_table_mapping_duckdb_db['is_attribute'] == 1]
#     field_mapping = get_field_mapping_duckdb_db[get_field_mapping_duckdb_db['is_attribute'] == 1]
#     table_name = table_mapping['destination_table'].iloc[0]
#     log.log_info(table_name)

#     destination_host = table_mapping['destination_host'].iloc[0]
#     destination_host = destination_host.replace('windows', ':')
#     destination_database = table_mapping['destination_database'].iloc[0]

#     df_credentials = sql.get_db_credentials()
#     db_credentials_dest = df_credentials[(df_credentials['database_name'] == destination_database) & (df_credentials['host'] == destination_host)]


#     is_incremental = table_mapping['is_incremental'].iloc[0]
#     temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
#     schema = "dbo"
#     temp_table_name = table_mapping['temp_table_name'].iloc[0]

#     sql_destination = sql.init_sql(db_credentials_dest)
#     sql_destination.connect()
    
#     # get list of destination columns from field mapping
#     columns = df.columns.tolist()

    
#     # check if is_incremental is True
#     if is_incremental:
#         # change the schema to tmp and define the temp table name and the target table name
#         # This will create a temp table and insert the data into it
       
#         source_table_name = f"{temp_schema}.{temp_table_name}"
#         target_table_name = f"{schema}.{table_name}"

#         # truncate data from temp table
#         log.log_info("Truncating Data from Temp Table")
#         sql_destination.truncate_table(schema_name=temp_schema, table_name=temp_table_name)
        
#         log.log_info("Inserting Data into Temp Table")
#         sql_destination.insert_data(schema=temp_schema, table_name=temp_table_name, insert_records=df, chunksize=10000)

#         # get destination columns from field mapping that have is_primary_key = True
#         primary_keys = field_mapping[field_mapping['is_primary_key'] == 1]['destination_column'].tolist()

#         # update the target table from the temp table
#         log.log_info("Updating Data in Target Table")
#         sql_destination.upsert_from_table(df=df, target_table=target_table_name, source_table=source_table_name, key_columns=primary_keys, delete_not_matched=False)

        

#     else:
#         log.log_info("Inserting Data into Table")
#         sql_destination.insert_data(schema=schema, table_name=table_name, insert_records=df, chunksize=10000)


#     metadata = {
#         "columns": MetadataValue.md(", ".join(columns)),
#         "rows_count": str(len(df)),
#         "columns_count": str(len(columns))
#     }

#     return Output(value=None, metadata=metadata)








