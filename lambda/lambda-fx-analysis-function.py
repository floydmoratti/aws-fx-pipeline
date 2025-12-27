import json
import boto3
import time
import os
import logging
from datetime import datetime, timedelta


# ---------- Setup ----------
ATHENA_DATABASE = os.environ["ATHENA_DATABASE"]
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"]
ATHENA_TABLE = os.environ["ATHENA_TABLE"]
ATHENA_WORKGROUP = os.environ["ATHENA_WORKGROUP"]

PAIR = os.environ["PAIR"]
DEVIATION_METRIC = f"{PAIR}Deviation"
METRIC_NAMESPACE = "FX/Analysis"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client("athena")
cloudwatch = boto3.client("cloudwatch")


# ---------- Helper Functions ----------
def get_run_date(event):
    raw_run_date = event["run_date"]
    dt = datetime.fromisoformat(raw_run_date.replace("Z", "+00:00"))
    fx_dt = dt - timedelta(days=1)
    year = f"{fx_dt.year:04d}"
    month = f"{fx_dt.month:02d}"
    day = f"{fx_dt.day:02d}"

    return year, month, day, fx_dt


def get_yesterdays_date(fx_dt):
    dt_pd = fx_dt - timedelta(days=1)
    year_pd = f"{dt_pd.year:04d}"
    month_pd = f"{dt_pd.month:02d}"
    day_pd = f"{dt_pd.day:02d}"

    return year_pd, month_pd, day_pd


def is_weekend(fx_dt):
    # outputs true for weekend and false for weekdays
    return fx_dt.weekday() >= 5  # 5 = Saturday, 6 = Sunday


def start_query(query):

    logger.info(f"Query Info: Database={ATHENA_DATABASE}, Table={ATHENA_TABLE}, Workgroup={ATHENA_WORKGROUP}")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP
    )

    return response["QueryExecutionId"]


def wait_for_query(query_execution_id):
    while True:
        response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = response["QueryExecution"]["Status"]["State"]

        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if status != "SUCCEEDED":
                reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                logger.error(f"Athena query failed: {reason}")
            return status
        
        time.sleep(2)
        

def get_single_value(query_execution_id):
    results = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )

    rows = results["ResultSet"]["Rows"]
    # Row 0 = header, Row 1 = value

    return float(rows[1]["Data"][0]["VarCharValue"])


def get_todays_rate(year, month, day):

    logger.info("Initiating todays rate query")

    query = f"""
    SELECT rate
    FROM {ATHENA_TABLE}
    WHERE pair = '{PAIR}'
        AND market_open = true
        AND year = '{year}'
        AND month = '{month}'
        AND day = '{day}'
    """

    qid = start_query(query)
    status = wait_for_query(qid)

    if status != "SUCCEEDED":
        raise Exception("Athena query for todays rate failed")
    
    return get_single_value(qid)


def get_yesterdays_rate(year_pd, month_pd, day_pd):

    logger.info("Initiating daily average query")

    query = f"""
    SELECT rate
    FROM {ATHENA_TABLE}
    WHERE pair = '{PAIR}'
        AND market_open = true
        AND year = '{year_pd}'
        AND month = '{month_pd}'
        AND day = '{day_pd}'
    """

    qid = start_query(query)
    status = wait_for_query(qid)

    if status != "SUCCEEDED":
        raise Exception("Athena query for daily average failed")
    
    return get_single_value(qid)


def publish_metric(deviation):
    cloudwatch.put_metric_data(
        Namespace=METRIC_NAMESPACE,
        MetricData=[
            {
                "MetricName": DEVIATION_METRIC,
                "Value": deviation * 100,  # convert 0.02 → 2%
                "Unit": "Percent"
            }
        ]
    )


# ---------- Lambda Handler ----------
def lambda_handler(event, context):

    logger.info("Received event:")
    logger.info(json.dumps(event, indent=2, default=str))

    year, month, day, fx_dt = get_run_date(event)
    year_pd, month_pd, day_pd = get_yesterdays_date(fx_dt)

    # Guard: market closed
    if is_weekend(fx_dt):
        logging.info("Market closed (weekend). Skipping anomaly check.")
        return {"status": "skipped", "reason": "weekend"}

    todays_rate = get_todays_rate(year, month, day)
    yesterdays_rate = get_yesterdays_rate(year_pd, month_pd, day_pd)

    deviation = abs(todays_rate - yesterdays_rate) / yesterdays_rate

    logger.info(f"Todays rate: {todays_rate}")
    logger.info(f"Yesterdays rate: {yesterdays_rate}")
    logger.info(f"Deviation: {deviation}")

    publish_metric(deviation)

    return {
        "pair": PAIR,
        "todays_rate": todays_rate,
        "yesterdays_rate": yesterdays_rate,
        "deviation": deviation
    }