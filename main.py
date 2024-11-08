"""ETL script Template."""

from pathlib import Path

from capepy.aws.glue import EtlJob

etl_job = EtlJob()

# `raw` has the contents of the raw file passed into the script
raw = etl_job.get_raw_file()

# TODO: Here you want to clean the contents of the `raw` variable
# and produce the "cleaned" content to the `cleaned` variable
cleaned = None

# TODO: Specify the name of the new clean file
# We typically just want to replace the file extension with a new one
# Below is an example of this, update with the correct extension
clean_key = str(Path(etl_job.parameters["OBJECT_KEY"]).with_suffix(".csv"))

# Put the new cleaned object into the clean bucket
if cleaned is not None:
    etl_job.write_clean_file(cleaned, clean_key=clean_key)
