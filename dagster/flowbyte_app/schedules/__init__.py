from dagster import ScheduleDefinition, schedule, RunRequest,MultiPartitionKey
from datetime import datetime, timedelta

from flowbyte_app.partitions import db_to_db_partitions, partition_keys





# Create a schedule to run the job daily
@schedule(
    job_name='db_2_db',
    cron_schedule="20 15 * * *",  # Run at 1:00 AM every day
    execution_timezone="Asia/Beirut",
)
def db_to_db_schedule(context):
    """Process all db 2 db integration runs."""
    
    # Create a run request for each partition (source and destination)
    return [
        RunRequest(
            run_key=f"{str(key)}",
            partition_key=MultiPartitionKey({"source": key.split("|")[1], "destination": key.split("|")[0]})
        )
        for key in partition_keys #db_to_db_partitions.get_partition_keys()
    ]

