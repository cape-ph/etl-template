"""ETL script Template."""

import sys

import boto3 as boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# for our purposes here, the spark and glue context are only (currently) needed
# to get the logger.
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
logger = glue_ctx.get_logger()

# TODO:
#   - add error handling for the format of the document being incorrect
#   - figure out how we want to name and namespace clean files (e.g. will we
#     take the object key we're given, strip the extension and replace it with
#     one for the new format, or will we do something else)
#   - see what we can extract out of here to be useful for other ETLs. imagine
#     we'd have a few different things that could be made into a reusable
#     package

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

# NOTE: for now we'll take the alert object key and change out the file
#       extension for the clean data (leaving all namespacing and such). this
#       will probably need to change

# NOTE: May need some creds here
s3_client = boto3.client("s3")

# try to get the docx object from S3 and handle any error that would keep us
# from continuing.
response = s3_client.get_object(Bucket=raw_bucket, Key=raw_key)

status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

if status != 200:
    err = (
        f"ERROR - Could not get object {raw_key} from bucket "
        f"{raw_bucket}. ETL Cannot continue."
    )

    logger.error(err)

    # NOTE: need to properly handle exception stuff here, and we probably want
    #       this going somewhere very visible (e.g. SNS topic or a perpetual log
    #       as someone will need to be made aware)
    raise Exception(err)

logger.info(f"Obtained object {raw_key} from bucket {raw_bucket}.")

raw = response.get("Body")

# create the cleaned output as well as the name of the cleaned file
cleaned = None

clean_key = None
# We typically just want to replace the file extension with a new one
# Below is a commented out example of this
# clean_key = raw_key.replace(".xlsx", ".csv")

if cleaned is not None and clean_key is not None:
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

        # NOTE: need to properly handle exception stuff here, and we probably
        #       want this going somewhere very visible (e.g. SNS topic or a
        #       perpetual log as someone will need to be made aware)
        raise Exception(err)

    logger.info(
        f"Transformed {raw_bucket}/{raw_key} and wrote result "
        f"to {clean_bucket}/{clean_key}"
    )
