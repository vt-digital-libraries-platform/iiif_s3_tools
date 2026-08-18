#!/usr/bin/python3
import boto3, mimetypes, os, sys, shutil
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

bucket = 'img.cloud.lib.vt.edu'
src_path = os.path.join('federated')

# This is the collection you want to move tiles/manifests to.
parent_collection_identifier = '<collection-identifier>'
collection_url = f'https://img.cloud.lib.vt.edu/federated/{parent_collection_identifier}/'

# These are the sub-collections you want to move tiles/manifests from.
sub_collections = ['<sub-collection-identifier-1>', '<sub-collection-identifier-2>', '<sub-collection-identifier-3>']

for identifier in sub_collections:
    collection_path = os.path.join(src_path, identifier)
    print(f'Collection: {collection_path}')
    jsons = get_matching_s3_keys(bucket, prefix=collection_path, suffix='.json')
    for jsonFile in jsons:
        print(f'key: {jsonFile}')
        # download manifest
        fileName = os.path.basename(jsonFile)
        print(f'Filename: {fileName}')
        s3.download_file(bucket, jsonFile, fileName)
        # read json and replace urls line by line
        file_text = ''
        with open(fileName, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                edited_line = line.replace(f"https://img.cloud.lib.vt.edu/federated/{identifier}/", collection_url)
                file_text += edited_line
        
        
        print(file_text)
        print("==============================")
        # write edited manifest back to s3 with same name

        # uncomment this when you think it'll work.
        # s3.put_object(Bucket=bucket, Key=jsonFile, Body=file_text.encode('utf-8'))
        






