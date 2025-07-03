import os
import msal
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from dagster import (
    asset, Field, Noneable, Output
)
import duckdb
import sys

sys.path.append("..")
from modules import log

load_dotenv()

CLIENT_ID     = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
TENANT_ID     = os.getenv("AZURE_TENANT_ID")
SCOPE         = ["https://analysis.windows.net/powerbi/api/.default"]
FILE_PATH     = os.getenv("AZURE_FILE_PATH")
POWER_BI_API  = os.getenv("POWER_BI_API")
AUTHORITY_URL = f'https://login.microsoftonline.com/{TENANT_ID}'

START_DATE_FIXED = datetime(2025, 5, 1)
END_DATE_OFFSET  = 1
ROLLING_DAYS     = 4
FIELDS_TO_KEEP   = [
    "timestamp", "operation", "provider", "email", "ip_address", "workspace",
    "report", "report_type", "consumption_method"
]


# ✅ Centralized date range asset
@asset(config_schema={
    "start_date": Field(Noneable(str), is_required=False),
    "end_date": Field(Noneable(str), is_required=False),
})
def date_range_asset(context) -> tuple[str, str]:
    config = context.op_config
    if config.get("start_date") and config.get("end_date"):
        return config["start_date"], config["end_date"]
    else:
        end = datetime.now()- timedelta(days=END_DATE_OFFSET)
        start = end - timedelta(days=ROLLING_DAYS)
        return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


@asset(
    owners=["reem.bazz@gmrlgroup.com", "team:data-sci"],
    compute_kind="duckdb",
    group_name="Delete",
    io_manager_key="parquet_io_manager",
    tags={"dagster/storage_kind": "parquet"},
)
def delete_pbi_report_logs(get_pbi_report_logs,date_range_asset: tuple[str, str]):
    start_date, end_date = date_range_asset

    con = duckdb.connect(database=FILE_PATH, read_only=False)
    query = f"""
        DELETE FROM user_telemetry
        WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
    """
    log.log_info(query)
    con.execute(query)

    query = f"""
        SELECT max(timestamp)
        FROM user_telemetry
        WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
    """
    max_timestamp = con.execute(query).fetchone()
    log.log_info(f"Max timestamp after delete: {max_timestamp}")
    
    con.close()


@asset(
    owners=["reem.bazz@gmrlgroup.com", "team:data-sci"],
    compute_kind="API",
    group_name="extract",
    io_manager_key="parquet_io_manager",
    tags={"dagster/storage_kind": "parquet"},
)
def get_pbi_report_logs(date_range_asset: tuple[str, str]):
    start_date, end_date = date_range_asset
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY_URL, client_credential=CLIENT_SECRET
    )
    token_response = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in token_response:
        raise RuntimeError(f"Auth failed: {token_response.get('error_description')}")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token_response['access_token']}",
    }

    today_0 = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date_dt = end_date
    cutoff_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0) #end_date_dt - timedelta(days=ROLLING_DAYS - 1)

    start_date_dt = max(cutoff_date, START_DATE_FIXED)
    all_rows = []
    current = start_date_dt
    while current <= end_date_dt:
        start_str = current.strftime("%Y-%m-%dT00:00:00")
        end_str   = current.strftime("%Y-%m-%dT23:59:59")

        url = (
            "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
            f"?startDateTime='{start_str}'&endDateTime='{end_str}'"
        )

        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            log.log_info(f"⚠️  {current.date()} – request failed ({resp.status_code}); skipping.")
            current += timedelta(days=1)
            continue

        payload = resp.json()
        all_rows.extend(payload.get("activityEventEntities", []))
        cont_url = payload.get("continuationUri")

        while cont_url:
            cont_resp = requests.get(cont_url, headers=headers)
            if cont_resp.status_code != 200:
                log.log_info(f"⚠️  Continuation broken ({cont_resp.status_code}); abandoning day.")
                break
            cont_json = cont_resp.json()
            all_rows.extend(cont_json.get("activityEventEntities", []))
            cont_url = cont_json.get("continuationUri")

        log.log_info(f"📥  {current.date()} - {len(all_rows)} total rows so far")
        current += timedelta(days=1)

    if not all_rows:
        log.log_info("No new rows retrieved.")
        exit()

    temp_df = pd.DataFrame(all_rows)
    column_renames = {
        "CreationTime": "timestamp",
        "Operation": "operation",
        "Workload": "provider",
        "UserId": "email",
        "ClientIP": "ip_address",
        "WorkSpaceName": "workspace",
        "ReportName": "report",
        "ReportType": "report_type",
        "ConsumptionMethod": "consumption_method",
    }
    temp_df.rename(columns=column_renames, inplace=True)

    return Output(value=temp_df)


@asset(
    owners=["reem.bazz@gmrlgroup.com", "team:data-sci"],
    compute_kind="Pandas",
    group_name="Transform",
    io_manager_key="parquet_io_manager",
    tags={"dagster/storage_kind": "parquet"},
)
def transform_pbi_report_logs(get_pbi_report_logs,delete_pbi_report_logs):
    temp_df = get_pbi_report_logs
    filtered = temp_df[temp_df["UserType"] == 0]
    log.log_info(filtered.columns)
    new_df = filtered[FIELDS_TO_KEEP]

    new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], errors="coerce")
    new_df["provider"] = new_df["provider"].str.upper()
    new_df["email"] = new_df["email"].str.lower()
    new_df["username"] = new_df["email"]

    ordered_columns = [
        "timestamp", "operation", "provider", "email", "username",
        "ip_address", "workspace", "report", "report_type", "consumption_method"
    ]
    new_df = new_df[ordered_columns]

    log.log_info(f"✅ Saved {len(new_df)} refreshed/new rows → {FILE_PATH}")
    return Output(value=new_df)


@asset(
    owners=["reem.bazz@gmrlgroup.com", "team:data-sci"],
    compute_kind="duckdb",
    group_name="Load",
    io_manager_key="parquet_io_manager",
    tags={"dagster/storage_kind": "parquet"},
)
def load_pbi_report_logs(transform_pbi_report_logs, date_range_asset: tuple[str, str]):
    start_date, end_date = date_range_asset

    con = duckdb.connect(database=FILE_PATH, read_only=False)
    df = transform_pbi_report_logs
    con.register("df", df)
    log.log_info(end_date)

    #con.execute("CREATE OR REPLACE TEMP VIEW df AS SELECT * FROM df")
    end_date_exclusive = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    query = f"""
        INSERT INTO main.user_telemetry (
            timestamp,
            operation,
            provider,
            email,
            username,
            ip_address,
            workspace,
            report,
            report_type,
            consumption_method
        )
        SELECT *
        FROM df
        WHERE timestamp >= '{start_date}' AND timestamp <= '{end_date_exclusive}'
    """
    log.log_info(query)
    con.execute(query)

    query = "SELECT max(timestamp) FROM user_telemetry"
    max_timestamp = con.execute(query).fetchone()
    log.log_info(f"Max timestamp after insert: {max_timestamp}")
    con.close()
