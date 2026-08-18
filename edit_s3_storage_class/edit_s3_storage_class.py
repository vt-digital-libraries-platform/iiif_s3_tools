import boto3, os
import pandas as pd

s3 = boto3.client('s3')

meta = "/path/to/metadata.csv"
bucket = "< my bucket >"
path = "path/to/collection"

def get_matching_s3_keys(bucket, prefix="", suffix=""):
    kwargs = {"Bucket": bucket}
    if isinstance(prefix, str):
        kwargs["Prefix"] = prefix

    while True:
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp["Contents"]
        except KeyError:
            return
        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                yield key
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break

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

df = csv_to_dataframe(meta)

for idx, row in df.iterrows():
    itemBasePath = os.path.join(path, row.identifier)

    for key in get_matching_s3_keys(bucket, itemBasePath):
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        print(copy_source)
        response = s3.copy(
            copy_source, bucket, key,
            ExtraArgs = {
                'StorageClass': 'STANDARD',
                'MetadataDirective': 'COPY'
            }
        )
        print(response)
        print()