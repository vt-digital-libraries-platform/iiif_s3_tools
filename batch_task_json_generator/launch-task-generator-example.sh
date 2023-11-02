#!/bin/bash 

JOB_QUEUE="IIIFS3JobQueue" \
JOB_DEFINITION="IIIFS3JobDefinition" \
AWS_REGION="us-east-1" \
SRC_PREFIX="<probably your collection category>" \
COLLECTION_IDENTIFIER="<collection identifier in csv>" \
AWS_SRC_BUCKET="<bucket w/ src images>" \
AWS_DEST_BUCKET="<bucket where the tiles etc should be written>" \
DEST_PREFIX="<probably your collection category again>" \
DEST_URL="<domain where resources will be hosted (cloudfront?)>" \
CSV_PATH="<path to directory containing your csv file in AWS_SRC_BUCKET w/o filename>" \
CSV_NAME="<your metadata filename>" \
python3 task-generator.py