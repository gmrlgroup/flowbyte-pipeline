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

from governance.assets import database
from governance.sensors import notification_sensors as ns
from governance.schedules import mssql_schedule
from governance.partitions import db_to_db_partitions
import os 

import sys
sys.path.append('..')
from modules import log, models

# Check if notifications are enabled
SEND_SUCCESS_NOTIFICATION = os.getenv("SEND_SUCCESS_NOTIFICATION", "false").lower() == "true"


sql_assets = load_assets_from_modules([database])




## GOVERNANCE JOBS ##
mssql = define_asset_job(name="mssql", selection=["delete_governance_tables", "get_tables_details", "get_coulumns_details", "get_indexes_details", "get_table_storage_usage_details", "get_indexes_storage_usage_details", "load_governance_tables"])

# List all active sensors
sensors = [ 
    # add the sensors
]

if SEND_SUCCESS_NOTIFICATION:  # Add success sensor only if enabled
    sensors.append(ns.success_notification_sensor)


defs = Definitions(
    assets = [ *sql_assets ],
    jobs=[  
            mssql
        ],
    schedules = [mssql_schedule ],
    sensors = sensors ,
    resources={
        "parquet_io_manager": models.PandasParquetIOManager(),
    }
)


