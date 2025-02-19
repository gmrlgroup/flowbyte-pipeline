import os
import pandas as pd
from dagster import MetadataValue, Output, asset, StaticPartitionsDefinition
from flowbyte.sql import MSSQL
import os


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
    connection_type="pyodbc"

    )



def get_databases():

    sql_setup.connect()

    query = f"""SELECT *

            FROM [dbo].[table_mapping]
            """

    if env == "DEV":
        log.log_debug(f"DEBUG: {query}")

    df = sql_setup.get_data(query, chunksize=1000)

    if env == "DEV":
        log.log_info(df)

    df['database_id'] = df['source_host'].astype(str) + "/" + df['source_database'].astype(str) + "/" + df['source_table'].astype(str)


    return df['database_id'].unique().tolist()


table_partitions = StaticPartitionsDefinition(get_databases())



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


def get_non_matching_rows(df1: pd.DataFrame, df2: pd.DataFrame, keys: list):
    """
    Returns rows from df1 that do not exist in df2 based on the specified keys.
    
    :param df1: First DataFrame
    :param df2: Second DataFrame
    :param keys: List of column names to be used as keys for comparison
    :return: Filtered DataFrame containing only rows from df1 that do not exist in df2
    """
    df_merged = df1.merge(df2, on=keys, how='left', indicator=True)
    df_filtered = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    return df_filtered


def print_progress(records, message="Extracted records so far"):
    log.log_info(f"{message}: {records}")


def generate_query(table_mapping, field_mappings, incremental_col=None, max_incremental_value=None):


    main_table = None
    main_columns = []      # columns coming from the main table (alias i)
    dest_table = table_mapping['source_table'].iloc[0]

    for mapping in field_mappings:
        src_col = mapping["source_column"]
        dest_col = mapping["destination_column"]
        

        main_columns.append(f"i.{src_col} AS {dest_col}")

    # Build the SELECT clause by concatenating main and attribute columns.
    select_clause = ", ".join(main_columns) #+ attribute_columns)
    
    # Build the FROM clause. We assume that both tables are in the same database
    query = f"SELECT {select_clause}\n"
    query += f"FROM {dest_table} i\n"

    if incremental_col:
        query += f"WHERE i.{incremental_col} > {max_incremental_value}\n"
    
    
    return query


def get_max_incremental_value(db_credentials, table_mapping, incremental_col):

    log.log_info("Getting Max Incremental Value from Destination Table")

    log.log_info(db_credentials)
    
    # Get table name
    table = table_mapping['destination_table'].iloc[0]

    # Create the query to get max incremental value
    query = f"SELECT MAX({incremental_col}) as cdc_key FROM {table}"

    int_columns = [incremental_col]

    sql_destination = init_sql(db_credentials)
    sql_destination.connect()

    # Get max incremental value
    max_value = sql_destination.get_data(query, integer_columns=int_columns)

    log.log_info(max_value)

    # if max_value.empty:
    if pd.isna(max_value.loc[0, 'cdc_key']):
        return 0

    cdc_key = max_value['cdc_key'].iloc[0]

    return cdc_key



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def get_db_credentials():
    
    query = f"""SELECT * FROM [dbo].[database_credential]"""

    sql_setup.connect()

    df = sql_setup.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def get_table_mapping(context):
    """
    Get Table Mappings
    """

    source_host = context.partition_key.split("/")[0]
    source_db = context.partition_key.split("/")[1]
    table_name = context.partition_key.split("/")[2]


    sql_setup.connect()

    query = f"""SELECT *

            FROM [dbo].[table_mapping]

            WHERE [source_table] = '{table_name}' AND source_host = '{source_host}' AND source_database = '{source_db}'
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="config", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def get_field_mapping(context):
    """
    Get Field Mappings
    """

    source_host = context.partition_key.split("/")[0]
    source_db = context.partition_key.split("/")[1]
    table_name = context.partition_key.split("/")[2]


    sql_setup.connect()


    query = f"""SELECT *

            FROM [dbo].[field_mapping]

            WHERE [source_table] = '{table_name}' AND source_host = '{source_host}' AND source_database = '{source_db}'
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="extract", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def get_source_data(context, get_db_credentials, get_table_mapping, get_field_mapping, config: models.QueryModel):
    """
    Get Data from Source
    """

    log.log_debug(get_table_mapping)
    table_mapping = get_table_mapping
    table_mapping_no_attribute = table_mapping[table_mapping['is_attribute'] == 0]

    destination_host = table_mapping_no_attribute['destination_host'].iloc[0]
    destination_database = table_mapping_no_attribute['destination_database'].iloc[0]
    
    # get db credentials where database name is equal to the source database and host is equal to the source host
    db_credentials = get_db_credentials[(get_db_credentials['database_name'] == destination_database) & (get_db_credentials['host'] == destination_host)]

    sql_source = init_sql(db_credentials)
    sql_source.connect()

    # check if table is_attribute is True
    is_incremental = table_mapping['is_incremental'].iloc[0]
    incremental_col = table_mapping['incremental_column'].iloc[0]

    field_mapping = get_field_mapping

    # convert field_mapping to dictionary
    mapping_data = field_mapping.to_dict(orient='records')

    if env == "DEV":
        log.log_info(mapping_data)


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
            max_incremental_value = get_max_incremental_value(db_credentials=db_credentials, table_mapping=table_mapping_no_attribute, incremental_col=incremental_col)
            query = generate_query(table_mapping_no_attribute, mapping_data, incremental_col, max_incremental_value)

        else:
            query = generate_query(table_mapping_no_attribute, mapping_data)
    
    
    if env == "DEV":
        log.log_info(query)

    
    object_columns = []
    object_columns = get_field_mapping[get_field_mapping['source_data_type'] == 'TEXT']
    object_columns = object_columns['source_column'].tolist()
    log.log_info(object_columns)

    df = sql_source.get_data(query, chunksize=100000, object_columns=object_columns, progress_callback=print_progress, message="Extracted ")

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="transform", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def transform_data(context, get_table_mapping, get_field_mapping, get_source_data):
    """
    Transform Data
    """

    df = get_source_data

    if df == None or  df.empty:
        return Output(value=df)


    destination_host = table_mapping['destination_host'].iloc[0]
    destination_db = table_mapping['destination_database'].iloc[0]
    field_mapping = get_field_mapping


    # Apply the filters using boolean indexing
    table_mapping = get_table_mapping.loc[
        (get_table_mapping['destination_host'] == destination_host) &
        (get_table_mapping['destination_database'] == destination_db)
    ]

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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="sql", group_name="transform", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def transform_attributes(context, get_field_mapping, get_source_data):
    """
    Transform Attributes
    """

    # Get table_name for partition
    table_name = context.partition_key

    df = get_source_data

    if df == None or  df.empty:
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def add_destination_data(context, get_db_credentials, get_table_mapping, get_field_mapping, transform_data):
    """
    Add Data to Destination
    """
    df = transform_data

    if df == None or  df.empty:
        return Output(value=df)
    
    # Get table_name for partition
    db_credentials = get_db_credentials
    table_mapping = get_table_mapping
    field_mapping = get_field_mapping

    table_name = table_mapping['destination_table'].iloc[0]
    is_incremental = table_mapping['is_incremental'].iloc[0]
    temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
    schema = "dbo"
    temp_table_name = table_mapping['temp_table_name'].iloc[0]
    
    
    # sql_destination.connect()
    sql_destination = init_sql(db_credentials)
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



@asset(owners=["kevork.keheian@flowbyte.dev", "team:data-eng"], compute_kind="api", group_name="load", io_manager_key="parquet_io_manager", partitions_def=table_partitions)
def add_destination_attributes(context, get_db_credentials, get_table_mapping, get_field_mapping, add_destination_data, transform_attributes):
    """
    Add Attributes to Destination
    """

    df = transform_attributes

    if df == None or  df.empty:
        return Output(value=df)

    # get the first destination_table from the field mapping
    table_mapping = get_table_mapping[get_table_mapping['is_attribute'] == 1]
    field_mapping = get_field_mapping[get_field_mapping['is_attribute'] == 1]
    table_name = table_mapping['destination_table'].iloc[0]
    log.log_info(table_name)

    is_incremental = table_mapping['is_incremental'].iloc[0]
    temp_schema = os.getenv('TEMP_SCHEMA') or "tmp"
    schema = "dbo"
    temp_table_name = table_mapping['temp_table_name'].iloc[0]

    sql_destination = init_sql(get_db_credentials)
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


