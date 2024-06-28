# #!/usr/bin/python3
# import json, boto3, os, sys, shutil, re
# from random import randrange


# # Start borrowed code
# # Code from https://github.com/alexwlchan/alexwlchan.net/tree/live/misc/matching_s3_objects
# def get_matching_s3_keys(bucket, prefix="", suffix=""):
#     """
#     Generate objects in an S3 bucket.
#     :param bucket: Name of the S3 bucket.
#     :param prefix: Only fetch objects whose key starts with
#         this prefix (optional).
#     :param suffix: Only fetch objects whose keys end with
#         this suffix (optional).
#     """
#     s3 = boto3.client("s3")
#     kwargs = {"Bucket": bucket}
#     # If the prefix is a single string (not a tuple of strings), we can
#     # do the filtering directly in the S3 API.
#     if isinstance(prefix, str):
#         kwargs["Prefix"] = prefix

#     while True:
#         # The S3 API response is a large blob of metadata.
#         # 'Contents' contains information about the listed objects.
#         resp = s3.list_objects_v2(**kwargs)
#         try:
#             contents = resp["Contents"]
#         except KeyError:
#             return
#         for obj in contents:
#             key = obj["Key"]
#             if key.startswith(prefix) and key.endswith(suffix):
#                 yield key

#         # The S3 API is paginated, returning up to 1000 keys at a time.
#         # Pass the continuation token into the next response, until we
#         # reach the final page (when this field is missing).
#         try:
#             kwargs["ContinuationToken"] = resp["NextContinuationToken"]
#         except KeyError:
#             break


# # End borrowed code


# def generate_json(
#     jobName,
#     jobQueue,
#     jobDefinition,
#     awsRegion,
#     collectionIdentifier,
#     accessDir,
#     awsSrcBucket,
#     awsDestBucket,
#     destPrefix,
#     destUrl,
#     csvPath,
#     csvName,
# ):
#     return {
#         "jobName": jobName,
#         "jobQueue": jobQueue,
#         "jobDefinition": jobDefinition,
#         "command": "./createiiif.sh",
#         "environment": [
#             {"name": "AWS_REGION", "value": awsRegion},
#             {"name": "COLLECTION_IDENTIFIER", "value": collectionIdentifier},
#             {"name": "ACCESS_DIR", "value": accessDir},
#             {"name": "AWS_SRC_BUCKET", "value": awsSrcBucket},
#             {"name": "AWS_DEST_BUCKET", "value": awsDestBucket},
#             {"name": "DEST_PREFIX", "value": destPrefix},
#             {"name": "DEST_URL", "value": destUrl},
#             {"name": "CSV_PATH", "value": csvPath},
#             {"name": "CSV_NAME", "value": csvName},
#         ],
#     }


# env = {}
# env["jobQueue"] = os.getenv("JOB_QUEUE")
# env["jobDefinition"] = os.getenv("JOB_DEFINITION")
# env["awsRegion"] = os.getenv("AWS_REGION")
# env["srcPrefix"] = os.getenv("SRC_PREFIX")
# env["collectionIdentifier"] = os.getenv("COLLECTION_IDENTIFIER")
# env["accessDir"] = os.getenv("ACCESS_DIR")
# env["awsSrcBucket"] = os.getenv("AWS_SRC_BUCKET")
# env["awsDestBucket"] = os.getenv("AWS_DEST_BUCKET")
# env["destPrefix"] = os.getenv("DEST_PREFIX")
# env["destUrl"] = os.getenv("DEST_URL")
# env["csvPath"] = os.getenv("CSV_PATH")
# env["csvName"] = os.getenv("CSV_NAME")


# # Define path
# collectionPath = os.path.join(env["srcPrefix"], env["collectionIdentifier"])
# # Define json store
# jsonStore = os.path.join(os.getcwd(), "json_files", env["collectionIdentifier"])
# # Delete directory if it exists
# if os.path.exists(jsonStore) and os.path.isdir(jsonStore):
#     shutil.rmtree(jsonStore)

# # Create directory for storing the JSON files
# os.makedirs(jsonStore)

# print("Processing directories -")

# # Get csvFile.csv
# csvFileWPath = os.path.join(env["csvPath"], env["csvName"])
# csvFile = os.path.basename(csvFileWPath)
# # Just csvFile
# csvFileNoExt = os.path.splitext(csvFile)[0]
# accessFolders = []

# # Generate jobs for each item directory in collection
# print("Generating jobs for " + csvFileWPath)
# for j in get_matching_s3_keys(env["awsSrcBucket"], os.path.join(collectionPath)):
#     print(j)
#     # Check if S3 object has "Access" in it
#     position = j.find("/Access/")
#     # If string contains Access and path already not in accessFolders
#     if position >= 0 and os.path.dirname(j) not in accessFolders:
#         print(f"Adding access directory: {os.path.dirname(j)}/")
#         accessFolders.append(os.path.dirname(j))
#         jobName = (
#             accessFolders[-1].replace("/", "-").replace(".", "_").replace("-Access", "")
#         )
#         accessDir = f"{accessFolders[-1]}/"
#         with open(jsonStore + "/" + jobName + ".json", "w") as json_file:
#             json.dump(
#                 generate_json(
#                     jobName,
#                     env["jobQueue"],
#                     env["jobDefinition"],
#                     env["awsRegion"],
#                     env["collectionIdentifier"],
#                     accessDir,
#                     env["awsSrcBucket"],
#                     env["awsDestBucket"],
#                     env["destPrefix"],
#                     env["destUrl"],
#                     env["csvPath"],
#                     csvFile,
#                 ),
#                 json_file,
#             )
import json
import boto3
import os
import shutil

class S3JsonJobGenerator:
    def __init__(self, env):
        self.s3 = boto3.client("s3")
        self.env = env
        self.collection_path = os.path.join(env["srcPrefix"], env["collectionIdentifier"])
        self.json_store = os.path.join(os.getcwd(), "json_files", env["collectionIdentifier"])
        self.access_folders = []

    def get_matching_s3_keys(self, bucket, prefix="", suffix=""):
        kwargs = {"Bucket": bucket}
        if isinstance(prefix, str):
            kwargs["Prefix"] = prefix

        while True:
            resp = self.s3.list_objects_v2(**kwargs)
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

    @staticmethod
    def generate_json(job_name, job_queue, job_definition, aws_region, collection_identifier, access_dir, aws_src_bucket, aws_dest_bucket, dest_prefix, dest_url, csv_path, csv_name):
        return {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "command": "./createiiif.sh",
            "environment": [
                {"name": "AWS_REGION", "value": aws_region},
                {"name": "COLLECTION_IDENTIFIER", "value": collection_identifier},
                {"name": "ACCESS_DIR", "value": access_dir},
                {"name": "AWS_SRC_BUCKET", "value": aws_src_bucket},
                {"name": "AWS_DEST_BUCKET", "value": aws_dest_bucket},
                {"name": "DEST_PREFIX", "value": dest_prefix},
                {"name": "DEST_URL", "value": dest_url},
                {"name": "CSV_PATH", "value": csv_path},
                {"name": "CSV_NAME", "value": csv_name},
            ],
        }

    def setup_directories(self):
        if os.path.exists(self.json_store) and os.path.isdir(self.json_store):
            shutil.rmtree(self.json_store)
        os.makedirs(self.json_store)

    def process_directories(self):
        csv_file_w_path = os.path.join(self.env["csvPath"], self.env["csvName"])
        csv_file = os.path.basename(csv_file_w_path)
        
        for key in self.get_matching_s3_keys(self.env["awsSrcBucket"], self.collection_path):
            position = key.find("/Access/")
            if position >= 0 and os.path.dirname(key) not in self.access_folders:
                self.access_folders.append(os.path.dirname(key))
                job_name = (
                    self.access_folders[-1].replace("/", "-").replace(".", "_").replace("-Access", "")
                )
                access_dir = f"{self.access_folders[-1]}/"
                with open(os.path.join(self.json_store, job_name + ".json"), "w") as json_file:
                    json.dump(
                        self.generate_json(
                            job_name,
                            self.env["jobQueue"],
                            self.env["jobDefinition"],
                            self.env["awsRegion"],
                            self.env["collectionIdentifier"],
                            access_dir,
                            self.env["awsSrcBucket"],
                            self.env["awsDestBucket"],
                            self.env["destPrefix"],
                            self.env["destUrl"],
                            self.env["csvPath"],
                            csv_file,
                        ),
                        json_file,
                    )

    def run(self):
        self.setup_directories()
        self.process_directories()

if __name__ == "__main__":
    env = {
        "jobQueue": os.getenv("JOB_QUEUE"),
        "jobDefinition": os.getenv("JOB_DEFINITION"),
        "awsRegion": os.getenv("AWS_REGION"),
        "srcPrefix": os.getenv("SRC_PREFIX"),
        "collectionIdentifier": os.getenv("COLLECTION_IDENTIFIER"),
        "accessDir": os.getenv("ACCESS_DIR"),
        "awsSrcBucket": os.getenv("AWS_SRC_BUCKET"),
        "awsDestBucket": os.getenv("AWS_DEST_BUCKET"),
        "destPrefix": os.getenv("DEST_PREFIX"),
        "destUrl": os.getenv("DEST_URL"),
        "csvPath": os.getenv("CSV_PATH"),
        "csvName": os.getenv("CSV_NAME"),
    }

    generator = S3JsonJobGenerator(env)
    generator.run()
