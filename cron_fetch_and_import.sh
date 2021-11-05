#!/bin/bash

set -e

cd /backup/batch_jobs/bc_air_quality
source .venv/bin/activate

for data in PM10 PM25 ; do
    ./src/bcaq.py fetch --datatype $data
    ./src/bcaq.py load --datatype $data ./data/raw/${data}.csv
done
