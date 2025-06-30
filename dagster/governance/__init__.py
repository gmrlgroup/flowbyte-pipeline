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

from governance.assets import database, report
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
report_assets = load_assets_from_modules([report])




## GOVERNANCE JOBS ##
mssql = define_asset_job(name="mssql", selection=["delete_governance_tables", "get_tables_details", "get_coulumns_details", "get_indexes_details", "get_table_storage_usage_details", "get_indexes_storage_usage_details", "load_governance_tables"])
report_logs_job = define_asset_job(name="report_logs_job", selection=["delete_pbi_report_logs", "get_pbi_report_logs", "transform_pbi_report_logs", "load_pbi_report_logs","date_range_asset"])


# List all active sensors   
sensors = [ 
    # add the sensors
]

if SEND_SUCCESS_NOTIFICATION:  # Add success sensor only if enabled
    sensors.append(ns.success_notification_sensor)


defs = Definitions(
    assets = [ *sql_assets, *report_assets ],
    jobs=[  
            mssql,
            report_logs_job
        ],
    schedules = [mssql_schedule ],
    sensors = sensors ,
    resources={
        "parquet_io_manager": models.PandasParquetIOManager(),
    }
)