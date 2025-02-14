
from dagster import (
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    
)

from flowbyte_app.assets import integration

import sys
sys.path.append('..')
from modules import log, models




integration_assets = load_assets_from_modules([integration])




## INTEGRATION JOBS ##
db_2_db_job = define_asset_job(name="db_2_db", selection=["get_table_mapping", "get_field_mapping", "get_source_data", "transform_data", "transform_attributes", "add_destination_data", "add_destination_attributes"])



defs = Definitions(
    assets=[*integration_assets],
    jobs=[  
            db_2_db_job,
        ],
    schedules=[ ],
    sensors=[ ],
    resources={
        "parquet_io_manager": models.PandasParquetIOManager(),
    }
)


