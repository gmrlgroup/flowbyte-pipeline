from dagster import ScheduleDefinition, schedule, RunRequest,MultiPartitionKey
from datetime import datetime, timedelta

from governance.partitions import mssql_database_partitions



# Create a schedule to run the job daily
@schedule(
    job_name='mssql',
    # cron_schedule every day at 11:00 AM
    cron_schedule="0 11 * * *",  # Run at 11:00 AM every day
    execution_timezone="Asia/Beirut",
)
def mssql_schedule(context):
    """Process all databases integration runs."""
    partition_keys = mssql_database_partitions.get_partition_keys()
    # Create a run request for each partition (source and destination)
    return [
        RunRequest(
            run_key=f"{str(key)}",
            partition_key=key
        )
        for key in partition_keys #db_to_db_partitions.get_partition_keys()
    ]

#Schedule for report_logs @4am
report_logs_schedule = ScheduleDefinition(
    name='report_logs_schedule',                        
    job_name='report_logs_job',
    cron_schedule="0 4 * * *",  # Run at 4:00 AM every day
    execution_timezone="Asia/Beirut"
)