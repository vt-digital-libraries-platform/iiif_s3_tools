#!/bin/bash

AWS_REGION="us-east-1" \
COLLECTION_CATEGORY="federated" \
COLLECTION_NAME="nmcst" \
CSV_FULL_PATH="/Users/whunter/dev/dlp/assets/newman_maps/nmcst/nmcst_src_2/20231106_nmcst_archive_metadata.csv" \
SRC="/Users/whunter/dev/dlp/assets/" \
SRC_ROOT="newman_maps/nmcst/nmcst_src_2/" \
TARGET="/Users/whunter/dev/dlp/assets/" \
TARGET_ROOT="federated/nmcst_for_upload/" \
DELETE_SRC="false" \
FILE_NAME_CONVENTION="dil" \
python3 s3_format-upload.py 