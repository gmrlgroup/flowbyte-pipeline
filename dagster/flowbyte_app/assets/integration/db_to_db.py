import os
import pandas as pd
from dagster import MetadataValue, Output, asset, StaticPartitionsDefinition, MultiPartitionKey
from flowbyte.sql import MSSQL
import os
from flowbyte_app.partitions import db_to_db_partitions
import platform
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








# @asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
# def get_db_credentials():
    
#     query = f"""SELECT * FROM [dbo].[database_credential]"""

#     sql_setup.connect()

#     df = sql_setup.get_data(query, chunksize=1000)

#     metadata = {
#         "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
#     }

#     return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def get_table_mapping(context):
    """
    Get Table Mappings
    """

    partition_key: MultiPartitionKey = context.partition_key.keys_by_dimension

    log.log_info(partition_key)
    
    source_key = partition_key["source"]
    destination_key = partition_key["destination"]

    if platform.system() == "Windows":
        source_host = source_key.split("\\")[0]
        source_db = source_key.split("\\")[1]
        source_table_name = source_key.split("\\")[2]

        destination_host = destination_key.split("\\")[0]
        destination_db = destination_key.split("\\")[1]
        destination_table_name = destination_key.split("\\")[2]

        if 'file.core.windows.net' not in source_host:
            source_host = source_host.replace('windows', ':')
        source_host = source_host.replace('network\\backslash', '\\\\')
        if 'file.core.windows.net' not in destination_host:
            destination_host = destination_host.replace('windows', ':')
        destination_host = destination_host.replace('network\\backslash', '\\\\')

    else:
        source_host = source_key.split("/")[0]
        source_db = source_key.split("/")[1]
        source_table_name = source_key.split("/")[2]

        destination_host = destination_key.split("/")[0]
        destination_db = destination_key.split("/")[1]
        destination_table_name = destination_key.split("/")[2]


    # source_host = context.partition_key.split("/")[0]
    # source_db = context.partition_key.split("/")[1]
    # table_name = context.partition_key.split("/")[2]


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
                        tm.[incremental_column]

            FROM [dbo].[table_mapping] as tm
            LEFT JOIN [data].[database] as source ON source.host = tm.source_host and source.[name] = tm.source_database
            LEFT JOIN [data].[database] as destincation ON destincation.host = tm.destination_host and destincation.[name] = tm.destination_database

            WHERE [source_table] = '{source_table_name}' AND source_host = '{source_host}' AND source_database = '{source_db}'
            AND [destination_table] = '{destination_table_name}' AND destination_host = '{destination_host}' AND destination_database = '{destination_db}'

            AND source.[type] = 'mssql' AND destincation.[type] = 'mssql'
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def get_field_mapping(context):
    """
    Get Field Mappings
    """

    # source_host = context.partition_key.split("/")[0]
    # source_db = context.partition_key.split("/")[1]
    # table_name = context.partition_key.split("/")[2]

    partition_key: MultiPartitionKey = context.partition_key.keys_by_dimension
    source_key = partition_key["source"]
    destination_key = partition_key["destination"]

    if platform.system() == "Windows":
        source_host = source_key.split("\\")[0]
        source_db = source_key.split("\\")[1]
        table_name = source_key.split("\\")[2]

        destination_host = destination_key.split("\\")[0]
        destination_db = destination_key.split("\\")[1]
        destination_table_name = destination_key.split("\\")[2]

        if 'file.core.windows.net' not in source_host:
            source_host = source_host.replace('windows', ':')
        source_host = source_host.replace('network\\backslash', '\\\\')
        if 'file.core.windows.net' not in destination_host:
            destination_host = destination_host.replace('windows', ':')
        destination_host = destination_host.replace('network\\backslash', '\\\\')

    else:
        source_host = source_key.split("/")[0]
        source_db = source_key.split("/")[1]
        table_name = source_key.split("/")[2]

        destination_host = destination_key.split("/")[0]
        destination_db = destination_key.split("/")[1]
        destination_table_name = destination_key.split("/")[2]




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

                AND source.[type] = 'mssql' AND destincation.[type] = 'mssql'
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="extract", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def get_source_data(context, get_table_mapping, get_field_mapping, config: models.QueryModel):
    """
    Get Data from Source
    """

    log.log_debug(f'Table Mapping: {get_table_mapping}')
    table_mapping = get_table_mapping
    table_mapping_no_attribute = table_mapping[table_mapping['is_attribute'] == 0]

    destination_host = table_mapping_no_attribute['destination_host'].iloc[0]
    destination_database = table_mapping_no_attribute['destination_database'].iloc[0]
    source_host = table_mapping_no_attribute['source_host'].iloc[0]
    source_database = table_mapping_no_attribute['source_database'].iloc[0]


    if platform.system() == "Windows":

        if 'file.core.windows.net' not in source_host:
            source_host = source_host.replace('windows', ':')
        source_host = source_host.replace('network\\backslash', '\\\\')
        if 'file.core.windows.net' not in destination_host:
            destination_host = destination_host.replace('windows', ':')
        destination_host = destination_host.replace('network\\backslash', '\\\\')

    
    # get db credentials where database name is equal to the source database and host is equal to the source host
    db_credentials_source = sql.get_db_credentials(host=source_host, database_name=source_database)
    db_credentials_dest = sql.get_db_credentials(host=destination_host, database_name=destination_database)
    # db_credentials_source = get_db_credentials[(get_db_credentials['database_name'] == source_database) & (get_db_credentials['host'] == source_host)]
    # db_credentials_dest = get_db_credentials[(get_db_credentials['database_name'] == destination_database) & (get_db_credentials['host'] == destination_host)]
    
    

    sql_source = sql.init_sql(db_credentials_source)
    sql_source.connect()


    sql_destination = sql.init_sql(db_credentials_dest)
    sql_destination.connect()

    # check if table is_attribute is True
    is_incremental = table_mapping['is_incremental'].iloc[0]
    incremental_col = table_mapping['incremental_column'].iloc[0]

    field_mapping = get_field_mapping

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
            query = sql.generate_query(table_mapping_no_attribute, mapping_data, incremental_col, max_incremental_value)

        else:
            query = sql.generate_query(table_mapping_no_attribute, mapping_data)
            if config.where:
                query += f"WHERE {config.where}"
    
    
    if env == "DEV":
        log.log_debug(f"Query: {query}")

    
    object_columns = []
    float_columns = []
    integer_columns = []
    # obcjec columns start with NVARCHAR
    object_columns = field_mapping[field_mapping['source_data_type'].str.startswith('TEXT')]
    object_columns = object_columns['source_column'].tolist()

    # int columns contains INT
    integer_columns = field_mapping[field_mapping['source_data_type'].str.contains('INT')]
    integer_columns = integer_columns['source_column'].tolist()

    # float columns contains DECIMAL
    float_columns = field_mapping[field_mapping['source_data_type'].str.startswith('DECIMAL')]
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="transform", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def transform_data(context, get_field_mapping, get_source_data):
    """
    Transform Data
    """

    df = get_source_data

    log.log_info(df)

    if df is None or  df.empty:
        return Output(value=df)


    field_mapping = get_field_mapping


    

    log.log_info(field_mapping)

    # Check if the filtered DataFrame is not empty
    if not field_mapping.empty:
        # Retrieve the first value from the 'destination_table' column
        table_name = field_mapping['destination_table'].iloc[0]
    else:
        # Handle the case where no matching rows are found
        table_name = None
        # Optionally, raise an exception or log a message
    

    field_mapping = field_mapping[
        (field_mapping['destination_table'] == table_name)
    ]


    # rename columns
    columns = field_mapping['destination_column'].tolist()

    for i in range(len(columns)):
        df.rename(columns={columns[i]: field_mapping['destination_column'].iloc[i]}, inplace=1)

    # remove columns that are not in the field mapping
    df = df[field_mapping['destination_column'].tolist()]


    metadata = {
        "columns": MetadataValue.md(", ".join(df.columns))
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="transform", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def transform_attributes(context, get_field_mapping, get_source_data):
    """
    Transform Attributes
    """

    # Get table_name for partition
    table_name = context.partition_key

    df = get_source_data

    if df is None or  df.empty:
        return Output(value=df)

    # field_mapping = get_field_mapping

    # get list of source columns from field mapping where is_attribute is False
    field_mapping = get_field_mapping[get_field_mapping['is_attribute'] == 1]


    # rename columns
    columns = field_mapping['source_column'].tolist()

    for i in range(len(columns)):
        df.rename(columns={columns[i]: field_mapping['destination_column'].iloc[i]}, inplace=1)

    # remove columns that are not in the field mapping
    df = df[field_mapping['destination_column'].tolist()]

    # print types of the columns in df
    if env == "DEV":
        log.log_debug(get_field_mapping.dtypes)

    # check if any of the destination columns has is_attribute as True
    if field_mapping['is_attribute'].any():
        # get destination columns that have is_attrubute False
        id_vars = field_mapping[field_mapping['is_attribute_key'] == 1]['destination_column'].tolist()

        log.log_info(id_vars)

        # get destination columns that have is_attrubute True
        value_vars = field_mapping[field_mapping['is_attribute_key'] == 0]['destination_column'].tolist()

        log.log_info(value_vars)

        df = pd.melt(df, id_vars=id_vars, value_vars=value_vars, var_name='attribute', value_name='value')

        if env == "DEV":
            log.log_debug(df)


    metadata = {
        "columns": MetadataValue.md(", ".join(df.columns))
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def add_destination_data(context, get_table_mapping, get_field_mapping, transform_data):
    """
    Add Data to Destination
    """
    df = transform_data

    if df is None or  df.empty:
        return Output(value=df)
    
    # Get table_name for partition
    
    table_mapping = get_table_mapping[get_table_mapping['is_attribute'] == 0]
    field_mapping = get_field_mapping

    destination_host = table_mapping['destination_host'].iloc[0]
    destination_database = table_mapping['destination_database'].iloc[0]

    if platform.system() == "Windows":
        if 'file.core.windows.net' not in destination_host:
            destination_host = destination_host.replace('windows', ':')
        destination_host = destination_host.replace('network\\backslash', '\\\\')

    db_credentials = sql.get_db_credentials(host=destination_host, database_name=destination_database)
    # db_credentials = get_db_credentials[(get_db_credentials['database_name'] == destination_database) & (get_db_credentials['host'] == destination_host)]

    log.log_info(db_credentials)

    table_name = table_mapping['destination_table'].iloc[0]
    is_incremental = table_mapping['is_incremental'].iloc[0]
    temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
    # schema = "dbo"
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
        sql_destination.insert_data(schema=schema, table_name=table_name, insert_records=df, chunksize=10000)


    metadata = {
        "columns": MetadataValue.md(", ".join(columns)),
        "rows_count": str(len(df)),
        "columns_count": str(len(columns))
    }

    return Output(value=None, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=db_to_db_partitions)
def add_destination_attributes(context, get_table_mapping, get_field_mapping, transform_attributes):
    """
    Add Attributes to Destination
    """

    df = transform_attributes

    if df is None or  df.empty:
        return Output(value=df)

    # get the first destination_table from the field mapping
    table_mapping = get_table_mapping[get_table_mapping['is_attribute'] == 1]
    field_mapping = get_field_mapping[get_field_mapping['is_attribute'] == 1]
    table_name = table_mapping['destination_table'].iloc[0]
    log.log_info(table_name)

    destination_host = table_mapping['destination_host'].iloc[0]
    if platform.system() == "Windows":
        if 'file.core.windows.net' not in destination_host:
            destination_host = destination_host.replace('windows', ':')
        destination_host = destination_host.replace('network\\backslash', '\\\\')
    destination_database = table_mapping['destination_database'].iloc[0]
    db_credentials = sql.get_db_credentials(host=destination_host, database_name=destination_database)
    # db_credentials = get_db_credentials[(get_db_credentials['database_name'] == destination_database) & (get_db_credentials['host'] == destination_host)]


    is_incremental = table_mapping['is_incremental'].iloc[0]
    temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
    schema = "dbo"
    temp_table_name = table_mapping['temp_table_name'].iloc[0]

    sql_destination = sql.init_sql(db_credentials)
    sql_destination.connect()
    
    # get list of destination columns from field mapping
    columns = df.columns.tolist()

    
    # check if is_incremental is True
    if is_incremental:
        # change the schema to tmp and define the temp table name and the target table name
        # This will create a temp table and insert the data into it
       
        source_table_name = f"{temp_schema}.{temp_table_name}"
        target_table_name = f"{schema}.{table_name}"

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
        sql_destination.insert_data(schema=schema, table_name=table_name, insert_records=df, chunksize=10000)


    metadata = {
        "columns": MetadataValue.md(", ".join(columns)),
        "rows_count": str(len(df)),
        "columns_count": str(len(columns))
    }

    return Output(value=None, metadata=metadata)


