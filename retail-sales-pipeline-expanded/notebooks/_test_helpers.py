# helper used by tests to run ingest+transform in-process
import os, subprocess
def run_all(raw_input, silver_output):
    # run ingest (will write parquet to BRONZE_OUTPUT location)
    import notebooks._run_ingest_local as ingest
    ingest.run(raw_input, silver_output)
