from datetime import datetime
from dagster import (
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    build_run_status_sensor_context,
    DagsterInstance,
    build_schedule_from_partitioned_job,
    schedule,
    RunRequest
    
)

from flowbyte_app.assets import integration, governance
from flowbyte_app.sensors import notification_sensors as ns
from flowbyte_app.partitions import db_to_db_partitions
from flowbyte_app.schedules import db_to_db_schedule
import os 

import sys
sys.path.append('..')
from modules import log, models

# Check if notifications are enabled
SEND_SUCCESS_NOTIFICATION = os.getenv("SEND_SUCCESS_NOTIFICATION", "false").lower() == "true"


integration_assets = load_assets_from_modules([integration])
governance_assets = load_assets_from_modules([governance])




## INTEGRATION JOBS ##
db_2_db_job = define_asset_job(name="db_2_db", selection=["get_db_credentials", "get_table_mapping", "get_field_mapping", "get_source_data", "transform_data", "transform_attributes", "add_destination_data", "add_destination_attributes"])

db_2_duckdb_job = define_asset_job(name="db_2_duckdb", selection=["get_table_mapping_duckdb", "get_field_mapping_duckdb", "get_source_data_duckdb", "transform_data_duckdb", "add_destination_data_duckdb"])

adls_2_duckdb_job = define_asset_job(name="adls_2_duckdb", selection=["get_table_mapping_adls", "get_field_mapping_adls", "get_source_data_adls", "transform_data_adls", "add_destination_data_adls"])

duckdb_2_duckdb_job = define_asset_job(name="duckdb_2_duckdb", selection=["get_table_mapping_duckdb_duckdb", 
                                                                          "get_field_mapping_duckdb_duckdb", 
                                                                          "get_source_data_duckdb_duckdb", 
                                                                          "transform_data_duckdb_duckdb", 
                                                                          "add_destination_data_duckdb_duckdb"])

duckdb_2_db_job = define_asset_job(name="duckdb_2_db", selection=["get_db_credentials_1",
                                                                            "get_table_mapping_duckdb_db", 
                                                                          "get_field_mapping_duckdb_db", 
                                                                          "get_source_data_duckdb_db", 
                                                                          "transform_data_duckdb_db", 
                                                                          "add_destination_data_duckdb_db",
                                                                        #   "add_destination_attributes_duckdb_db"
                                                                          ])

# List all active sensors
sensors = [
    # add the sensors
]

if SEND_SUCCESS_NOTIFICATION:  # Add success sensor only if enabled
    sensors.append(ns.success_notification_sensor)


defs = Definitions(
    assets = [ *integration_assets ],
    jobs=[  
            db_2_db_job,
            db_2_duckdb_job,
            adls_2_duckdb_job,
            duckdb_2_duckdb_job,
            duckdb_2_db_job
        ],
    schedules = [ db_to_db_schedule ],
    sensors = sensors ,
    resources={
        "parquet_io_manager": models.PandasParquetIOManager(),
    }
)


