import requests
from dagster import SensorEvaluationContext, RunStatusSensorContext, run_status_sensor, DefaultSensorStatus, DagsterRunStatus
import os






    # Define a global sensor for all jobs
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,  # Triggers when any job completes successfully
    default_status=DefaultSensorStatus.RUNNING  # Ensures the sensor is active
)
def success_notification_sensor(context: RunStatusSensorContext):
    """Tracks all job completions and sends notifications."""

    run_stats = context.instance.get_run_stats(context.dagster_run.run_id)

    # Extract job details
    job_name = context.dagster_run.job_name
    
    # Extract the duration
    duration = run_stats.end_time - run_stats.start_time

    # Prepare payload
    payload = {
        "data": {
            "Name": job_name,
            "Description": "Dagster Job Completed",
            "Duration": f"{duration / 60:.2f} min"  # Convert seconds to minutes
        },
        "message": "Dagster job finished successfully"
    }

    # Send notification
    response = requests.post(
        os.getenv("NOTIFICATION_URL"),
        json=payload,
        verify=False  # Disable SSL verification for localhost
    )

    context.log.info(f"Notification sent for job {job_name}: {response.status_code}")



