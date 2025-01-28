"""ETL script Template."""

from pathlib import Path

from capepy.aws.glue import EtlJob

etl_job = EtlJob()

# `src` has the contents of the source file passed into the script
src = etl_job.get_src_file()

# TODO: Here you want to clean the contents of the `src` variable
# and produce the "cleaned" content to the `sink` variable
sink = None

# TODO: Specify the name of the new sink file
# We typically just want to replace the file extension with a new one
# Below is an example of this, update with the correct extension
sink_key = str(Path(etl_job.parameters["OBJECT_KEY"]).with_suffix(".csv"))

# Put the new object into the sink bucket location
if sink is not None:
    etl_job.write_sink_file(sink, sink_key=sink_key)
