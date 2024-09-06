# #!/usr/bin/python3
# import boto3, mimetypes, os, sys, shutil
# import pandas as pd
# from lib_files.identifier_parser import get_item_data_by_convention

# # Create an S3 client
# s3 = boto3.client('s3')

# # Start borrowed code
# # Code from https://github.com/alexwlchan/alexwlchan.net/tree/live/misc/matching_s3_objects
# ######
# ######
# def get_matching_s3_keys(bucket, prefix='', suffix=''):
#     """
#     Generate objects in an S3 bucket.
#     :param bucket: Name of the S3 bucket.
#     :param prefix: Only fetch objects whose key starts with
#         this prefix (optional).
#     :param suffix: Only fetch objects whose keys end with
#         this suffix (optional).
#     """
#     kwargs = {'Bucket': bucket}
#     # If the prefix is a single string (not a tuple of strings), we can
#     # do the filtering directly in the S3 API.
#     if isinstance(prefix, str):
#         kwargs['Prefix'] = prefix

#     while True:
#         # The S3 API response is a large blob of metadata.
#         # 'Contents' contains information about the listed objects.
#         resp = s3.list_objects_v2(**kwargs)
#         try:
#             contents = resp['Contents']
#         except KeyError:
#             return
#         for obj in contents:
#             key = obj['Key']
#             if key.startswith(prefix) and key.endswith(suffix):
#                 yield key

#         # The S3 API is paginated, returning up to 1000 keys at a time.
#         # Pass the continuation token into the next response, until we
#         # reach the final page (when this field is missing).
#         try:
#             kwargs['ContinuationToken'] = resp['NextContinuationToken']
#         except KeyError:
#             break
# ######
# ######
# # End borrowed code

# def is_img_file(file_name):
#     mimetype = mimetypes.guess_type(file_name)[0]
#     if mimetype is not None and mimetype.startswith('image/'):
#       return True
#     else:
#       return False

# def csv_to_dataframe(csv_path):
#     df = pd.read_csv(
#         csv_path,
#         na_values='NaN',
#         keep_default_na=False,
#         encoding='utf-8',
#         dtype={
#             'Start Date': str,
#             'End Date': str})
#     return df


# env = {}
# env['aws_region'] = os.getenv('AWS_REGION')
# env['collection_category'] = os.getenv('COLLECTION_CATEGORY')
# env['collection_name'] = os.getenv('COLLECTION_NAME')
# env['csv_full_path'] = os.getenv('CSV_FULL_PATH')
# env['src'] = os.getenv('SRC')
# env['src_is_s3'] = os.getenv('SRC_IS_S3')
# env['src_root'] = os.getenv('SRC_ROOT')
# env['target'] = os.getenv('TARGET')
# env['target_is_s3'] = os.getenv('TARGET_IS_S3')
# env['target_root'] = os.getenv('TARGET_ROOT')
# env['delete_src'] = os.getenv('DELETE_SRC')
# env['file_name_convention'] = os.getenv('FILE_NAME_CONVENTION')

# # src and target can be either s3 bucket uri or local path
# # srcRoot and targetRoot should be the path from src or target to the collection directory
# # items will be read from src/srcRoot/
# # item directories will be written to target/targetRoot/collectionCategory/collectionName/
# # there will be an "Access" directory in each item directory for now, just to make it work.

# # Define paths
# # Define source path
# src_path = os.path.join(env['src'], env['src_root'])
# print("src_path: ", src_path)

# # Define collection/target paths
# collection_path = os.path.join(env['collection_category'], env['collection_name'])
# target_path = os.path.join(env['target_root'], env['collection_name'])

# src_is_s3 = env['src_is_s3'] is not None and env['src_is_s3'] == "true"
# target_is_s3 = env['target_is_s3'] is not None and env['target_is_s3'] == "true"

# if not target_is_s3:
#     target_path = os.path.join(env['target'], target_path)

# print("src_is_s3: ", src_is_s3)
# print("target_is_s3: ", target_is_s3)

# # Delete target if local and exists
# if not target_is_s3 and os.path.exists(target_path) and os.path.isdir(target_path):
#     shutil.rmtree(target_path)

# if not target_is_s3:
#     # If local target, Create directory for storing output files
#     os.makedirs(target_path)

# print("Processing files in src path -")
# file_list = []
# csv_data = None
# if src_is_s3:
#     try:
#         csv_response = s3.get_object(Bucket=env['src'], Key=env['csv_full_path'])
#         csv_data = csv_to_dataframe(csv_response['Body'] ).itertuples()
#     except Exception as e:
#         print(e)
#         sys.exit(1)

#     for i in get_matching_s3_keys(env['src'], env['src_root']):
#         if is_img_file(i):
#             file_list.append(i)
# else:
#     csv_data = csv_to_dataframe(env['csv_full_path']).itertuples()
#     for root, dirs, files in os.walk(src_path):
#         for file in files:
#             if is_img_file(file):
#                 file_list.append(os.path.join(root, file))

# for row in csv_data:
#   item_data = get_item_data_by_convention(env['file_name_convention'], row.identifier)
#   item_identifier = item_data['identifier']
#   print(f"Looking for images that belong in: {item_identifier}")
#   files_found = False
#   for file in file_list:
#       file_name = os.path.basename(file)
#       if item_identifier in file_name:
#         files_found = True
#         item_path = os.path.join(target_path, item_identifier, "Access")
#         print(f"Found {file_name} that should go in {item_identifier}")
#         print(f"Moving/copying {file_name} to {item_path}")
#         if target_is_s3:
#             try:
#                 print("s3 -> s3 copy")
#                 s3.copy_object(
#                     Bucket=env['target'],
#                     CopySource={'Bucket': env['src'], 'Key': file},
#                     Key=os.path.join(item_path, str.lower(file_name.replace(' ', '_'))))
#             except Exception as e:
#                 print(e)
#         else:
#             if not os.path.exists(item_path):
#                 os.makedirs(item_path)
#                 # Move or copy the image file to the item directory
#                 if env['delete_src'] == "true":
#                     shutil.move(file, os.path.join(item_path,str.lower(file_name.replace(' ', '_'))))
#                 else:
#                     shutil.copy(file, os.path.join(item_path,str.lower(file_name.replace(' ', '_'))))
#   if not files_found:
#     print(f"No images found for {item_identifier}")
#   print("====================================")

# # Move the csv file to the collection directory
# print("Moving/copying csv file -")
# if target_is_s3:
#     target_csv_key = os.path.join(target_path, str.lower(os.path.basename(env['csv_full_path']).replace(' ', '_')))
#     print(f"Copying csv to {target_csv_key}")
#     try:
#         s3.copy_object(
#             Bucket=env['target'],
#             CopySource={'Bucket': env['src'], 'Key': env['csv_full_path']},
#             Key=target_csv_key)
#     except Exception as e:
#         print(e)
# else:
#     if os.path.exists(env['csv_full_path']) and os.path.isfile(env['csv_full_path']):
#         csvFileName = os.path.basename(env['csv_full_path'])
#     if env['delete_src'] == "true":  
#         shutil.move(env['csv_full_path'], os.path.join(target_path, str.lower(csvFileName.replace(' ', '_'))))
#     else:
#         shutil.copy(env['csv_full_path'], os.path.join(target_path, str.lower(csvFileName.replace(' ', '_'))))

# # Delete the src directory if local, exists and we're supposed to
# if not src_is_s3 and os.path.exists(src_path) and os.path.isdir(src_path) and env['delete_src'] == "true":
#     shutil.rmtree(src_path)
import sys
import boto3
import mimetypes
import os
import shutil
import pandas as pd
from lib_files.identifier_parser import get_item_data_by_convention

class S3FormatUploadProcessor:
    def __init__(self, env):
        self.s3 = boto3.client('s3')
        self.env = env
        self.src_path = os.path.join(env['src'], env['src_root'])
        self.target_path = os.path.join(env['target_root'], env['collection_name'])
        self.collection_path = os.path.join(env['collection_category'], env['collection_name'])
        self.src_is_s3 = env['src_is_s3'] == "true"
        self.target_is_s3 = env['target_is_s3'] == "true"
        self.file_list = []
        self.csv_data = None
        
    def get_matching_s3_keys(self, bucket, prefix='', suffix=''):
        kwargs = {'Bucket': bucket}
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:
            resp = self.s3.list_objects_v2(**kwargs)
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

    @staticmethod
    def is_img_file(file_name):
        mimetype = mimetypes.guess_type(file_name)[0]
        return mimetype is not None and mimetype.startswith('image/')

    @staticmethod
    def csv_to_dataframe(csv_path):
        return pd.read_csv(
            csv_path,
            na_values='NaN',
            keep_default_na=False,
            encoding='utf-8',
            dtype={'Start Date': str, 'End Date': str}
        )

    def setup_directories(self):
        if not self.target_is_s3:
            if os.path.exists(self.target_path) and os.path.isdir(self.target_path):
                shutil.rmtree(self.target_path)
            os.makedirs(self.target_path)

    def fetch_csv_data(self):
        if self.src_is_s3:
            try:
                csv_response = self.s3.get_object(Bucket=self.env['src'], Key=self.env['csv_full_path'])
                self.csv_data = self.csv_to_dataframe(csv_response['Body']).itertuples()
            except Exception as e:
                print(e)
                sys.exit(1)
        else:
            self.csv_data = self.csv_to_dataframe(self.env['csv_full_path']).itertuples()

    def build_file_list(self):
        if self.src_is_s3:
            for key in self.get_matching_s3_keys(self.env['src'], self.env['src_root']):
                if self.is_img_file(key):
                    self.file_list.append(key)
        else:
            for root, _, files in os.walk(self.src_path):
                for file in files:
                    if self.is_img_file(file):
                        self.file_list.append(os.path.join(root, file))

    def process_files(self):
        for row in self.csv_data:
            item_data = get_item_data_by_convention(self.env['file_name_convention'], row.identifier)
            item_identifier = item_data['identifier']
            files_found = False

            for file in self.file_list:
                file_name = os.path.basename(file)
                if item_identifier in file_name:
                    files_found = True
                    item_path = os.path.join(self.target_path, item_identifier, "Access")

                    if self.target_is_s3:
                        try:
                            self.s3.copy_object(
                                Bucket=self.env['target'],
                                CopySource={'Bucket': self.env['src'], 'Key': file},
                                Key=os.path.join(item_path, str.lower(file_name.replace(' ', '_')))
                            )
                        except Exception as e:
                            print(e)
                    else:
                        if not os.path.exists(item_path):
                            os.makedirs(item_path)
                        if self.env['delete_src'] == "true":
                            shutil.move(file, os.path.join(item_path, str.lower(file_name.replace(' ', '_'))))
                        else:
                            shutil.copy(file, os.path.join(item_path, str.lower(file_name.replace(' ', '_'))))
            if not files_found:
                print(f"No images found for {item_identifier}")
            print("====================================")

    def move_csv_file(self):
        target_csv_key = os.path.join(self.target_path, str.lower(os.path.basename(self.env['csv_full_path']).replace(' ', '_')))
        if self.target_is_s3:
            try:
                self.s3.copy_object(
                    Bucket=self.env['target'],
                    CopySource={'Bucket': self.env['src'], 'Key': self.env['csv_full_path']},
                    Key=target_csv_key
                )
            except Exception as e:
                print(e)
        else:
            csv_file_name = os.path.basename(self.env['csv_full_path'])
            if self.env['delete_src'] == "true":
                shutil.move(self.env['csv_full_path'], os.path.join(self.target_path, str.lower(csv_file_name.replace(' ', '_'))))
            else:
                shutil.copy(self.env['csv_full_path'], os.path.join(self.target_path, str.lower(csv_file_name.replace(' ', '_'))))

    def clean_up_src(self):
        if not self.src_is_s3 and os.path.exists(self.src_path) and os.path.isdir(self.src_path) and self.env['delete_src'] == "true":
            shutil.rmtree(self.src_path)

    def run(self):
        self.setup_directories()
        self.fetch_csv_data()
        self.build_file_list()
        self.process_files()
        self.move_csv_file()
        self.clean_up_src()

if __name__ == "__main__":
    env = {
        'aws_region': os.getenv('AWS_REGION'),
        'collection_category': os.getenv('COLLECTION_CATEGORY'),
        'collection_name': os.getenv('COLLECTION_NAME'),
        'csv_full_path': os.getenv('CSV_FULL_PATH'),
        'src': os.getenv('SRC'),
        'src_is_s3': os.getenv('SRC_IS_S3'),
        'src_root': os.getenv('SRC_ROOT'),
        'target': os.getenv('TARGET'),
        'target_is_s3': os.getenv('TARGET_IS_S3'),
        'target_root': os.getenv('TARGET_ROOT'),
        'delete_src': os.getenv('DELETE_SRC'),
        'file_name_convention': os.getenv('FILE_NAME_CONVENTION')
    }

    processor = S3FormatUploadProcessor(env)
    processor.run()

