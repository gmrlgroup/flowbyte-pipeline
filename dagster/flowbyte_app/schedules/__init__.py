# from dagster import ScheduleDefinition
# from datetime import datetime, timedelta




# gl_entry_job_schedule = ScheduleDefinition(
#     name="gl_entry_job_schedule",
#     job_name="gl_entry",
#     cron_schedule="0 7 * * *",  # At 07:00 every day
#     execution_timezone="Asia/Beirut",
#     run_config={
#         "ops": {
#             "get_balance_sheet_data_from_navdw": {
#                 "config": {
#                     "start_date": (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d'),
#                     "end_date": datetime.now().strftime('%Y-%m-%d')
#                 }
#             },
#             "get_pandl_data_from_navdw": {
#                 "config": {
#                     "start_date": (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d'),
#                     "end_date": datetime.now().strftime('%Y-%m-%d')            

#                 }
#             }
#         }   
#     }
# )