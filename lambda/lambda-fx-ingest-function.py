import json
import os
import logging
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import boto3


# ---------- Setup ----------
BUCKET_NAME = os.environ["BUCKET_NAME"]
BASE_CURRENCY = os.environ.get("BASE_CURRENCY", "USD")
TARGET_CURRENCIES = os.environ.get("TARGET_CURRENCIES", "JPY").split(",")  # convert string to a list
FX_API_URL = os.environ["FX_API_URL"]
TARGET_CURRENCY_CODES = [BASE_CURRENCY + c for c in TARGET_CURRENCIES]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")
s3 = boto3.client("s3")


# ---------- Helper Functions ----------
def get_api_key():
    logger.info("Retrieving API Key from Parameter Store")

    response = ssm.get_parameter(
        Name="/fx/api/access_key",
        WithDecryption=True
    )

    return response["Parameter"]["Value"]


def get_run_date(event):
    raw_run_date = event["run_date"]
    dt = datetime.fromisoformat(raw_run_date.replace("Z", "+00:00"))
    fx_dt = dt - timedelta(days=1)
    year = f"{fx_dt.year:04d}"
    month = f"{fx_dt.month:02d}"
    day = f"{fx_dt.day:02d}"

    return year, month, day


def fetch_fx_rates(api_key):
    # Fetches FX rates from public API

    params = {
        "access_key": api_key,
        "source": BASE_CURRENCY,
        "currencies": ",".join(TARGET_CURRENCIES)  # convert list into single string
    }

    url = FX_API_URL + "?" + urllib.parse.urlencode(params)
    logger.info(f"Requesting FX data from {url}")

    with urllib.request.urlopen(url, timeout=3) as response:
        if response.status != 200:
            raise RuntimeError(f"FX API returned status {response.status}")
        
        data = json.loads(response.read())
    
    return data


def validate_fx_data(data):
    # Basic validation to catch bad or partial responses

    required_fields = ["timestamp", "source", "quotes"]

    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
    
    for currency in TARGET_CURRENCY_CODES:
        if currency not in data["quotes"]:
            raise ValueError(f"Missing FX quote for: {currency}")
        

def filter_fx_data(data):
    # Drop unnessacery fields from data

    filtered_payload = {
        "timestamp": data.get("timestamp"),
        "source": data.get("source"),
        "quotes": data.get("quotes")
    }

    return filtered_payload


def build_s3_key(year, month, day):
    # Build partitioned S3 key

    return f"raw/year={year}/month={month}/day={day}/rates.json"


def write_to_s3(s3_key, data):
    # Write raw FX JSON to S3

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    logger.info("FX ingestion lambda started")
    logger.info("Received event:")
    logger.info(json.dumps(event, indent=2, default=str))

    try:
        api_key = get_api_key()
        fx_data = fetch_fx_rates(api_key)
        validate_fx_data(fx_data)
        filtered_data = filter_fx_data(fx_data)

        year, month, day = get_run_date(event)
        s3_key = build_s3_key(year, month, day)
        write_to_s3(s3_key, filtered_data)

        logger.info(f"FX data successfully written to s3://{BUCKET_NAME}/{s3_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "FX data ingested successfully",
                "s3_key": s3_key
            })
        }
    
    except Exception as e:
        logger.exception("FX ingestion failed")
        raise e