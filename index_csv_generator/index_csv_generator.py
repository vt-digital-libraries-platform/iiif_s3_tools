#!/usr/local/bin/python3

# Usage: python3 index_csv_generator.py <source s3 bucket> <source directory> <target s3 bucket> <target directory>

import boto3
import botocore
import csv
import io
import json
import os
import re
import sys


class IndexCSVGenerator(object):

    def __init__(self, source_bucket_name, source_dir, target_bucket_name, target_dir, metadata_csv):
        self.source_bucket_name = source_bucket_name
        self.source_dir = source_dir
        self.repository_type = self.get_repository_type(self.source_dir)
        self.target_bucket_name = target_bucket_name
        self.target_dir = target_dir
        self.metadata_csv = metadata_csv
        self.target_file_name = "index.csv"
        self.identifiers_found = []
        self.identifiers_not_found = []

        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.source_bucket = self.s3_resource.Bucket(self.source_bucket_name)
        self.target_bucket = self.s3_resource.Bucket(self.target_bucket_name)

        self.objects = None
        self.manifests = None
        self.headers = None
        self.metadata = None
        self.added_files = []

        self.objects = self.get_objects()


    def format(self):
        self.metadata = self.get_metadata()
        self.manifests = self.get_manifests(self.objects)
        self.write_csvs_to_subdirectories()
        self.write_added_files_to_local()
        

    def run(self):
        csvs = self.get_csvs(self.objects)
        output_string = ""
        print("Creating index.csv strings from subdirectory csvs and manifests")
        for csv_file in csvs:
            print("Adding " + csv_file + " to index.csv")
            output_string += csv_file.replace(self.repository_type + "/", "") + ","
            basepath = self.get_basepath(csv_file)
            manifest_string = self.get_manifest_string(basepath, self.objects)
            output_string += (manifest_string + "\n")
        local_temp_file_name = "output_temp.csv"
        self.write_local_file(output_string, local_temp_file_name)
        self.write_to_s3(local_temp_file_name)
        self.delete_local_file(local_temp_file_name)
        self.write_summary()


    def write_csvs_to_subdirectories(self):
        for manifest in self.manifests:
            path = manifest.replace("/manifest.json", "")
            identifier = path.replace(self.source_dir + "/", "")
            metadata_obj = io.StringIO()
            writer = csv.DictWriter(metadata_obj, self.headers)
            writer.writeheader()
            if identifier in self.metadata:
                writer.writerow(self.metadata[identifier])
                metadata_str = metadata_obj.getvalue()

                full_path = path + "/" + identifier + ".csv"
                print("Writing - " + full_path)
                s3_object = self.s3_resource.Object(self.target_bucket_name, full_path)
                s3_object.put(Body=metadata_str)
                self.added_files.append(full_path)
                self.added_files.append(path + "/index.csv")

                if full_path not in self.objects:
                    self.objects.append(full_path)
                self.identifiers_found.append(identifier)
            else:
                self.identifiers_not_found.append(identifier)


    def write_summary(self):
        output = "Records written for identifiers:\n"
        for identifier in self.identifiers_found:
            output = output + identifier + "\n"
        output = output + "Errors occured writing records for the following identifiers:\n"
        for identifier in self.identifiers_not_found:
            output = output + identifier + "\n"

        self.write_local_file(output, self.source_dir.replace("/","_") + "summary.txt")


    def write_added_files_to_local(self):
        print("Writing added files to local json for cleanup if necessary")
        self.write_local_file(json.dumps(self.added_files), self.source_dir.replace("/","_") + "_files_added.json" )


    def get_repository_type(self, source_dir):
        pattern = re.compile('^[a-zA-Z]+')
        root = pattern.match(source_dir)
        return root.group()


    def get_metadata(self):
        metadata_obj = {}
        self.get_metadata_file()

        with open(self.metadata_csv, 'r') as csv_file:
            print("reading metadata file: " + self.metadata_csv)
            reader = csv.DictReader(csv_file)
            self.headers = reader.fieldnames
            for row in reader:
                metadata_obj[row['Identifier']] = row
        os.remove(self.metadata_csv)

        return metadata_obj


    def get_metadata_file(self):
        try:
            metadata_file_path = self.source_dir + "/" + self.metadata_csv
            print("downloading metadata file: " + metadata_file_path)
            self.s3_resource.Bucket(self.source_bucket_name).download_file(metadata_file_path, self.metadata_csv)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise       
        

    def get_objects(self):
        print("Fetching objects from " + self.source_bucket_name + "/" + self.source_dir + ". This might take a while.")
        return [bucket_object.key for bucket_object in self.source_bucket.objects.filter(Prefix=self.source_dir)]


    def get_manifests(self, objects):
        manifests = []
        for obj in objects:
          if obj.endswith("manifest.json"):
              manifests.append(obj)
        return manifests


    def get_csvs(self, objects):
        csvs = []
        index_csv = self.source_dir + "/index.csv"
        uploaded_csv = self.source_dir + "/" + self.metadata_csv
        for obj in objects:
          if obj.endswith(".csv") and obj != index_csv and obj != uploaded_csv:
              csvs.append(obj)
        return csvs


    def get_basepath(self, file_path):
        return os.path.dirname(file_path)


    def get_manifest_string(self, basepath, objects):
        manifest_path_list = []
        for file in objects:
            if file.startswith(basepath) and file.endswith('manifest.json'):
                manifest_path_list.append(self.get_basepath(file).replace(self.repository_type + "/", ""))
        return (',').join(manifest_path_list)


    def write_local_file(self, output_string, file_name):
        f = open(file_name, "w")
        f.write(output_string)
        f.close


    def delete_local_file(self, file_name):
        os.remove(file_name)


    def write_to_s3(self, local_file):
        print("Writing " + self.target_file_name + " to s3")
        key = os.path.join(self.target_dir, self.target_file_name)
        self.s3_client.upload_file(local_file, self.target_bucket_name, key)

# end class IndexCSVGenerator

if __name__ == '__main__':

    if len(sys.argv) < 5:
        print("Usage: python3 index_csv_generator.py <source s3 bucket> <source directory> <target s3 bucket> <target directory> <metadata_csv>")
        sys.exit(1)
    else:
        source_bucket = "".join(sys.argv[1])
        source_dir = "".join(sys.argv[2])
        target_bucket = "".join(sys.argv[3])
        target_dir = "".join(sys.argv[4])
        metadata_csv = None
        try:
            metadata_csv = "".join(sys.argv[5])
        except:
            pass
        

    generator = IndexCSVGenerator(source_bucket, source_dir, target_bucket, target_dir, metadata_csv)
    if metadata_csv is not None:
        print(metadata_csv + " specified. Generating metadata files for Collection items." )
        generator.format()
    else:
        print("Metadata CSV not given. Proceeding with index generation.")
    
    generator.run()
