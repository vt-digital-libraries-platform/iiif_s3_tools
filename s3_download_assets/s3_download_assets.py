#!/usr/bin/python3
import boto3, mimetypes, os
import pandas as pd

# Create an S3 client
s3 = boto3.client('s3')

def get_matching_s3_keys(bucket, prefix='', suffix=''):
    kwargs = {'Bucket': bucket}
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return
        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def is_img_file(file_name):
    mimetype = mimetypes.guess_type(file_name)[0]
    if mimetype is not None and mimetype.startswith('image/'):
      return True
    else:
      return False

def csv_to_dataframe(csv_path):
    df = pd.read_csv(
        csv_path,
        na_values='NaN',
        keep_default_na=False,
        encoding='utf-8',
        dtype={
            'Start Date': str,
            'End Date': str})
    return df

bucket = ""
prefix = "federated/nmcst"
local_dir = "/target/path"
csv_path = "/path/to/metadata.csv"
suffix = ".tif"

if not os.path.exists(local_dir):
    os.makedirs(local_dir)

for idx, record in csv_to_dataframe(csv_path).iterrows():
    recordPrefix = os.path.join(prefix, record.identifier)
    for key in get_matching_s3_keys(bucket, recordPrefix, suffix):
        local_file_path = os.path.join(local_dir, os.path.basename(key))
        print(f"Downloading {key} to {local_file_path}")
        s3.download_file(bucket, key, local_file_path)




