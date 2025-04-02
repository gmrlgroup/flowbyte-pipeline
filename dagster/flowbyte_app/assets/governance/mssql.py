import os
import pandas as pd
from dagster import MetadataValue, Output, asset, op, StaticPartitionsDefinition, MultiPartitionKey
from flowbyte.sql import MSSQL
import os
from flowbyte_app.partitions import mssql_database_partitions

import sys
sys.path.append('..')
from modules import sql, log, models





server, database, username, password = sql.get_connection_details("FB")
sql_fb = MSSQL(
    host=server,
    username=username,
    password=password,
    database=database,
    driver="ODBC Driver 17 for SQL Server",
    connection_type="pyodbc"

    )



@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="delete", io_manager_key="parquet_io_manager")
def delete_governance_tables():

    db_credentials = sql.get_db_credentials(host='13.95.30.119', database='flowbyte')

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    sql_fb.connect()
    sql_fb.delete_data(schema_name="data", table_name="column")
    sql_fb.delete_data(schema_name="data", table_name="index")
    sql_fb.delete_data(schema_name="data", table_name="table")






@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="source", io_manager_key="parquet_io_manager", partitions_def=mssql_database_partitions)
def get_tables_details(context):
    
    host = context.partition_key.split("|")[0]
    database = context.partition_key.split("|")[1]

    query = f"""

          SELECT 
            @@SERVERNAME AS [host],
            DB_NAME() AS [database_name],
            s.name AS [schema],
            t.name AS [name]
        FROM sys.tables t
        JOIN sys.schemas s 
            ON t.schema_id = s.schema_id
        JOIN sys.partitions p 
            ON t.object_id = p.object_id
        """



    db_credentials = sql.get_db_credentials(host=host, database=database)

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    df = sql_fb.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)





@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="source", io_manager_key="parquet_io_manager", partitions_def=mssql_database_partitions)
def get_coulumns_details(context):

    host = context.partition_key.split("|")[0]
    database = context.partition_key.split("|")[1]
    
    query = f"""

        SELECT
            @@SERVERNAME AS [host],
            DB_NAME() AS [database_name],
            s.name AS [schema],
            t.name AS [table_name],
            c.name AS [name],
            ty.name AS [data_type],
            c.precision,
            c.scale
        FROM sys.columns c
        JOIN sys.tables t 
            ON c.object_id = t.object_id
        JOIN sys.schemas s 
            ON t.schema_id = s.schema_id
        JOIN sys.types ty 
            ON c.user_type_id = ty.user_type_id
        ORDER BY s.name, t.name, c.column_id
        
        """

    db_credentials = sql.get_db_credentials(host=host, database=database)

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    df = sql_fb.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)





@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="source", io_manager_key="parquet_io_manager", partitions_def=mssql_database_partitions)
def get_indexes_details(context):
    
    host = context.partition_key.split("|")[0]
    database = context.partition_key.split("|")[1]

    query = f"""

            SELECT
            @@SERVERNAME AS [host],
            DB_NAME() AS [database_name],
            s.name AS [schema],
            t.name AS [table_name],
            i.name AS [name],
            -- Concatenate all column names for this index into one comma-separated list
            STUFF((
                SELECT ', ' + c.name
                FROM sys.index_columns ic
                INNER JOIN sys.columns c 
                    ON ic.object_id = c.object_id 
                AND ic.column_id = c.column_id
                WHERE ic.object_id = t.object_id
                AND ic.index_id = i.index_id
                ORDER BY ic.key_ordinal
                FOR XML PATH(''), TYPE
            ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS [columns],
            i.type_desc AS [type]
        FROM sys.tables t
        INNER JOIN sys.schemas s 
            ON t.schema_id = s.schema_id
        INNER JOIN sys.indexes i 
            ON t.object_id = i.object_id
        ORDER BY s.name, t.name, i.index_id
        
        """

    db_credentials = sql.get_db_credentials(host=host, database=database)

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    df = sql_fb.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)







@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="source", io_manager_key="parquet_io_manager", partitions_def=mssql_database_partitions)
def get_table_storage_usage_details(context):

    host = context.partition_key.split("|")[0]
    database = context.partition_key.split("|")[1]
    
    query = f"""

            SELECT
                o.create_date AS [creation_date],
                @@SERVERNAME AS [host],
                DB_NAME() AS [database_name],
                s.name AS [schema],
                t.name AS [table_name],
                SUM(p.rows) AS [row_count],
                (SUM(a.used_pages) * 8.0) / 1024 AS [used_space_mb],
                (SUM(a.total_pages) * 8.0) / 1024 AS [allocated_space_mb]
            FROM sys.tables t
            JOIN sys.schemas s 
                ON t.schema_id = s.schema_id
            JOIN sys.objects o 
                ON t.object_id = o.object_id
            JOIN sys.partitions p 
                ON t.object_id = p.object_id
            JOIN sys.allocation_units a 
                ON p.partition_id = a.container_id
            WHERE p.index_id IN (0, 1)
            GROUP BY o.create_date, s.name, t.name
            ORDER BY t.name
        """

    db_credentials = sql.get_db_credentials(host=host, database=database)

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    df = sql_fb.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)






@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="source", io_manager_key="parquet_io_manager", partitions_def=mssql_database_partitions)
def get_indexes_storage_usage_details(context):

    host = context.partition_key.split("|")[0]
    database = context.partition_key.split("|")[1]
    
    query = f"""
                    SELECT
                        GETDATE() AS [date],
                        @@SERVERNAME AS [host],
                        DB_NAME() AS [database],
                        s.name AS [schema],
                        t.name AS [table],
                        i.name AS [index_name],
                        STUFF
                        (
                            (
                                SELECT
                                    ', ' + c.name
                                FROM sys.index_columns ic
                                JOIN sys.columns c
                                    ON c.object_id = ic.object_id
                                AND c.column_id = ic.column_id
                                WHERE i.object_id = ic.object_id
                                AND i.index_id   = ic.index_id
                                ORDER BY ic.index_column_id
                                FOR XML PATH('')
                            ),
                            1,
                            2,
                            ''
                        ) AS [index_columns],
                        i.type_desc AS [type],
                        p.rows AS [row_count],
                        (SUM(a.used_pages) * 8.0) / 1024.0 AS [used_space_mb],
                        (SUM(a.total_pages) * 8.0) / 1024.0 AS [allocated_space_mb]
                    FROM sys.tables t
                    JOIN sys.schemas s
                        ON t.schema_id = s.schema_id
                    JOIN sys.indexes i
                        ON t.object_id = i.object_id
                    JOIN sys.partitions p
                        ON i.object_id = p.object_id
                    AND i.index_id = p.index_id
                    JOIN sys.allocation_units a
                        ON p.partition_id = a.container_id
                    WHERE t.is_ms_shipped = 0
                    GROUP BY
                        s.name,
                        t.name,
                        i.object_id,   
                        i.index_id,    
                        i.name,
                        i.type_desc,
                        p.rows

        """
    
    db_credentials = sql.get_db_credentials(host=host, database=database)

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    df = sql_fb.get_data(query, chunksize=1000)

    metadata = {
        "row_sql": MetadataValue.md("```SQL\n" + query + "\n```")
    }

    return Output(value=df, metadata=metadata)




@asset(owners=["peter.elhage@gmrlgroup.com", "team:data-eng"], compute_kind="sql", group_name="load", io_manager_key="parquet_io_manager", partitions_def=mssql_database_partitions)
def load_governance_tables(context, get_tables_details, get_coulumns_details, get_indexes_details, get_table_storage_usage_details, get_indexes_storage_usage_details):

    df = get_tables_details
    df_1 = get_coulumns_details
    df_2 = get_indexes_details
    df_3 = get_table_storage_usage_details
    df_4 = get_indexes_storage_usage_details


    host = context.partition_key.split("|")[0]
    database = context.partition_key.split("|")[1]

    db_credentials = sql.get_db_credentials(host=host, database=database)

    sql_fb = sql.init_sql(db_credentials)
    sql_fb.connect()

    sql_fb.insert_data(schema_name="data", table_name="table", df=df, chunksize=10000)

    sql_fb.insert_data(schema_name="data", table_name="column", df=df_1, chunksize=10000)

    sql_fb.insert_data(schema_name="data", table_name="index", df=df_2, chunksize=10000)

    sql_fb.insert_data(schema_name="data", table_name="table_storage_usage", df=df_3, chunksize=10000)

    sql_fb.insert_data(schema_name="data", table_name="index_storage_usage", df=df_4, chunksize=10000)


    return Output(value=None)









