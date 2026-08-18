import boto3, json, os
import pandas as pd
from datetime import datetime

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
s3_client = boto3.client("s3")

def get_matching_s3_keys(bucket, prefix="", suffix=""):
    s3 = boto3.client("s3")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    matches = []
    while True:
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp["Contents"]
        except KeyError:
            return
        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                matches.append(key)
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break
    return matches


bucket = "< my bucket >"
collection_path = os.path.join("federated", "testss")
collection_objects = get_matching_s3_keys(
    bucket, prefix=collection_path
)

for obj in collection_objects:
    filename = os.path.basename(obj)
    print(obj)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=obj)
        print(response)
        data = response["Body"].read()
        with open(filename, "wb") as f:
            f.write(data)
        print(f"Downloaded {filename} from {bucket}/{obj}")
    except Exception as e:
        print(e)

    print()