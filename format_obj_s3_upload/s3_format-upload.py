#!/usr/bin/python3
import json, boto3, mimetypes, os, sys, shutil, re
import pandas as pd
from lib_files.identifier_parser import get_item_data_by_convention

# Start borrowed code
# Code from https://github.com/alexwlchan/alexwlchan.net/tree/live/misc/matching_s3_objects
######
######
def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.
    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        try:
            contents = resp['Contents']
        except KeyError:
            return
        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
######
######
# End borrowed code


env = {}
env['aws_region'] = os.getenv('AWS_REGION')
env['collection_category'] = os.getenv('COLLECTION_CATEGORY')
env['collection_name'] = os.getenv('COLLECTION_NAME')
env['csv_full_path'] = os.getenv('CSV_FULL_PATH')
env['src'] = os.getenv('SRC')
env['src_root'] = os.getenv('SRC_ROOT')
env['target'] = os.getenv('TARGET')
env['target_root'] = os.getenv('TARGET_ROOT')
env['delete_src'] = os.getenv('DELETE_SRC')
env['file_name_convention'] = os.getenv('FILE_NAME_CONVENTION')

# src and target can be either s3 bucket uri or local path
# srcRoot and targetRoot should be the path from src or target to the collection directory
# items will be read from src/srcRoot/
# item directories will be written to target/targetRoot/collectionCategory/collectionName/
# there will be an "Access" directory in each item directory for now, just to make it work.

# Define paths
# Define source path
src_path = os.path.join(env['src'], env['src_root'])

# Define target path
collection_path = os.path.join(env['collection_category'], env['collection_name'])
target_path = os.path.join(env['target'], env['target_root'], env['collection_name'])

src_is_s3 = src_path.startswith('s3://')
target_is_s3 = target_path.startswith('s3://')
# Delete target if local and exists
if not target_is_s3 and os.path.exists(target_path) and os.path.isdir(target_path):
    shutil.rmtree(target_path)

# Create directory for storing the JSON files
os.makedirs(target_path)

def is_img_file(file_name):
    mimetype = mimetypes.guess_type(file_name)[0]
    if mimetype.startswith('image/'):
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

print("Processing files in src path -")
file_list = []
if not src_is_s3:
    for root, dirs, files in os.walk(src_path):
        for file in files:
            if is_img_file(file):
                file_list.append(os.path.join(root, file))
    #for j in get_matching_s3_keys(env['srcBucket'], os.path.dirname(i) + "/", '.tif'):
csv_data = csv_to_dataframe(env['csv_full_path']).itertuples()
for row in csv_data:
  item_data = get_item_data_by_convention(env['file_name_convention'], row.identifier)
  item_identifier = item_data['identifier']
  print(f"Looking for images that belong in: {item_identifier}")
  files_found = False
  for file in file_list:
      file_name = os.path.basename(file)
      if item_identifier in file_name:
        files_found = True
        item_path = os.path.join(target_path, item_identifier, "Access")
        print(f"Found {file_name} that should go in {item_identifier}")
        print(f"Moving/copying {file_name} to {item_path}")
        if not os.path.exists(item_path):
          os.makedirs(item_path)
        # Move or copy the image file to the item directory
        if env['delete_src'] == "true":
            shutil.move(file, os.path.join(item_path,str.lower(file_name.replace(' ', '_'))))
        else:
            shutil.copy(file, os.path.join(item_path,str.lower(file_name.replace(' ', '_'))))
  if not files_found:
    print(f"No images found for {item_identifier}")
  print("====================================")
sys.exit(0)
# Move the csv file to the collection directory
print("Moving/copying csv file -")
if os.path.exists(env['csv_full_path']) and os.path.isfile(env['csv_full_path']):
  csvFileName = os.path.basename(env['csv_full_path'])
  if env['delete_src'] == "true":  
    shutil.move(env['csv_full_path'], os.path.join(target_path, str.lower(csvFileName.replace(' ', '_'))))
  else:
    shutil.copy(env['csv_full_path'], os.path.join(target_path, str.lower(csvFileName.replace(' ', '_'))))

# Delete the src directory if local, exists and we're supposed to
if not src_is_s3 and os.path.exists(src_path) and os.path.isdir(src_path) and env['delete_src'] == "true":
    shutil.rmtree(src_path)

