#!/usr/bin/python3
import json, boto3, os, sys, shutil
from random import randrange

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
                yield obj['Key']

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

def generate_json(jobName, collectionName, csvName, csvPath, accessDir, srcBucket = "vtlib-store", destBucket = "img.cloud.lib.vt.edu", destUrl = "https://img.cloud.lib.vt.edu", destPrefix = "iawa", awsRegion="us-east-1", uploadBool="false"):
    return {
        "jobName": jobName,
        "jobQueue":"QueueForWDC",
        "jobDefinition":"IAWATileGenerationJob",
        "command":"./createiiif.sh",
        "environment":[
            {
                "name":"COLLECTION_NAME",
                "value": collectionName
            },
            {
                "name":"UPLOAD_BOOL",
                "value": uploadBool
            },
            {
                "name":"AWS_REGION",
                "value": awsRegion
            },
            {
                "name":"SRC_BUCKET",
                "value": srcBucket
            },
            {
                "name":"AWS_BUCKET_NAME",
                "value": destBucket
            },
            {
                "name":"CSV_NAME",
                "value": csvName
            },
            {
                "name":"ACCESS_DIR",
                "value": accessDir
            },
            {
                "name":"CSV_PATH",
                "value": csvPath
            },
            {
                "name":"DEST_BUCKET",
                "value": destBucket
            },
            {
                "name": "DEST_PREFIX",
                "value": destPrefix
            },
            {
                "name":"DEST_URL",
                "value": destUrl
            },
            {
                "name": "DIR_PREFIX",
                "value": "SpecScans/Women_of_Design"
            }
        ]
    }

collectionPrefix = sys.argv[1]
collectionName = sys.argv[2]

# Define path
collectionPath = collectionPrefix + collectionName

# Define json store
jsonStore = './json_files/' + collectionName
# Delete directory if it exists
if os.path.exists(jsonStore) and os.path.isdir(jsonStore):
    shutil.rmtree(jsonStore)

# Create directory for storing the JSON files
os.makedirs(jsonStore)

print("Processing directories -")

# Get all the CSV files
for i in get_matching_s3_keys('vtlib-store', collectionPath, '.csv'):
    # Get CSVfile.csv
    CSVFile = os.path.basename(i)
    # Just CSVfile
    CSVFileNoExt = os.path.splitext(CSVFile)[0]
    accessFolders = []
    # For each CSVfile
    print(collectionPath + "/" + CSVFileNoExt)
    for j in get_matching_s3_keys('vtlib-store', collectionPath + "/" + CSVFileNoExt + "/"):
        # Check if S3 object has "Access" in it
        position = j.find('/Access/')
        # If string contains Access and path already not in accessFolders
        if position != -1 and j[0:position+7] not in accessFolders:
                accessFolders.append(j[0:position+7])
                with open(jsonStore + '/' + 'A_' + accessFolders[-1].replace('/', '-')[-30:-7] + str(randrange(0, 100001, 2)) + '.json', 'w') as json_file:
                    json.dump(generate_json('A_' + accessFolders[-1].replace('/', '-')[-50:-7] + str(randrange(0, 100001, 2)), collectionName, CSVFile, os.path.dirname(i), accessFolders[-1].replace('SpecScans/Women_of_Design/','')), json_file)
