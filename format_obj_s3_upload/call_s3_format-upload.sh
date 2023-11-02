#!/bin/bash

AWS_REGION="us-east-1" \
COLLECTION_CATEGORY="federated" \
COLLECTION_NAME="vtec" \
CSV_FULL_PATH="federated/vtec/vtec_flat/entomology2d_2024apr12_archive_metadata.csv" \
SRC="jennifer-vtec2d-formatting" \
SRC_IS_S3="true" \
SRC_ROOT="federated/vtec/vtec_flat" \
TARGET="jennifer-vtec2d-formatting" \
TARGET_IS_S3="true" \
TARGET_ROOT="federated/vtec/vtec_formatted/" \
DELETE_SRC="false" \
FILE_NAME_CONVENTION="dil" \
python3 s3_format-upload.py 