"""ETL script Template."""

import sys
from pathlib import Path

import boto3 as boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Initialize logging and context
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
logger = glue_ctx.get_logger()

# Evaluate parameters
parameters = getResolvedOptions(
    sys.argv,
    [
        "RAW_BUCKET_NAME",
        "ALERT_OBJ_KEY",
        "CLEAN_BUCKET_NAME",
    ],
)
raw_bucket = parameters["RAW_BUCKET_NAME"]
raw_key = parameters["ALERT_OBJ_KEY"]
clean_bucket = parameters["CLEAN_BUCKET_NAME"]

# Retrieve the raw file passed into the ETL script
# Fail nicely if there is an error and log it
s3_client = boto3.client("s3")
response = s3_client.get_object(Bucket=raw_bucket, Key=raw_key)
status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status != 200:
    err = (
        f"ERROR - Could not get object {raw_key} from bucket "
        f"{raw_bucket}. ETL Cannot continue."
    )
    logger.error(err)
    raise Exception(err)

logger.info(f"Obtained object {raw_key} from bucket {raw_bucket}.")

# `raw` has the contents of the raw file passed into the script
raw = response.get("Body")

# TODO: Here you want to clean the contents of the `raw` variable
# and produce the "cleaned" content to the `cleaned` variable
cleaned = None

# TODO: Specify the name of the new clean file
# We typically just want to replace the file extension with a new one
# Below is an example of this, update with the correct extension
clean_key = str(Path(raw_key).with_suffix(".csv"))

# Put the new cleaned object into the clean bucket
if cleaned is not None:
    response = s3_client.put_object(
        Bucket=clean_bucket, Key=clean_key, Body=cleaned
    )
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status != 200:
        err = (
            f"ERROR - Could not write transformed data object {clean_key} "
            f"to bucket {clean_bucket}. ETL Cannot continue."
        )
        logger.error(err)
        raise Exception(err)

    logger.info(
        f"Transformed {raw_bucket}/{raw_key} and wrote result "
        f"to {clean_bucket}/{clean_key}"
    )
