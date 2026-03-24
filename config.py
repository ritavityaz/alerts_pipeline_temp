import os
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "alerts-dashboard-data")
CLOUDFRONT_DISTRIBUTION_ID = os.environ.get("CF_DISTRIBUTION", "E28MP73WOLCLYQ")
OREF_ALERTS_HISTORY_URL = "https://alerts-history.oref.org.il/Shared/Ajax/GetAlarmsHistory.aspx"
ALERTS_START_DATE = datetime(2026, 2, 26, tzinfo=ZoneInfo("Asia/Jerusalem"))
MAX_CONCURRENT_CITY_FETCHES = 10
PIPELINE_RUN_INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "3600"))

s3_client = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-2"))
cf_client = boto3.client("cloudfront", region_name=os.environ.get("AWS_REGION", "us-east-2"))

S3_RAW_ALERTS_PREFIX = "raw/alerts/"
LOCAL_RAW_ALERTS_DIR = os.path.join(os.path.dirname(__file__), "raw_alerts")
