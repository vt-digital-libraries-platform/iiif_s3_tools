#!/usr/local/bin/python3

# Usage: python3 index_csv_generator.py <source s3 bucket> <source directory> <target s3 bucket> <target directory>

import boto3
import os
import sys

class IndexCSVGenerator(object):

    def __init__(self, repository_type, source_bucket_name, source_dir, target_bucket_name, target_dir):
        self.repository_type = repository_type
        self.source_bucket_name = source_bucket_name
        self.source_dir = source_dir
        self.target_bucket_name = target_bucket_name
        self.target_dir = target_dir
        self.target_file_name = "index.csv"

        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.source_bucket = self.s3_resource.Bucket(self.source_bucket_name)
        self.target_bucket = self.s3_resource.Bucket(self.target_bucket_name)

    def run(self):
        objects = self.get_objects()
        csvs = self.get_csvs(objects)
        output_string = ""
        for csv_file in csvs:
            output_string += csv_file.strip(self.repository_type + "/") + ","
            basepath = self.get_basepath(csv_file)
            manifest_string = self.get_manifest_string(basepath, objects)
            output_string += (manifest_string + "\n")
        local_temp_file_name = "output_temp.csv"
        self.write_local_temp(output_string, local_temp_file_name)
        self.write_to_s3(local_temp_file_name)
        self.delete_local_temp(local_temp_file_name)

    def get_objects(self):
        return [bucket_object.key for bucket_object in self.source_bucket.objects.filter(Prefix=self.source_dir)]

    def get_csvs(self, objects):
        csvs = []
        index_csv = self.source_dir + "/index.csv"
        for obj in objects:
          if obj.endswith(".csv") and obj != index_csv:
              csvs.append(obj)
        return csvs

    def get_basepath(self, file_path):
        return os.path.dirname(file_path)

    def get_manifest_string(self, basepath, objects):
        manifest_path_list = []
        for file in objects:
            if file.startswith(basepath) and file.endswith('manifest.json'):
                manifest_path_list.append(self.get_basepath(file).strip(self.repository_type + "/"))
        return (',').join(manifest_path_list)

    def write_local_temp(self, output_string, file_name):
        f = open(file_name, "w")
        f.write(output_string)
        f.close

    def delete_local_temp(self, file_name):
        os.remove(file_name)

    def write_to_s3(self, local_file):
        key = os.path.join(self.target_dir, self.target_file_name)
        self.s3_client.upload_file(local_file, self.target_bucket_name, key)

if __name__ == '__main__':

    if len(sys.argv) < 6:
        print("Usage: python3 index_csv_generator.py <repository_type> <source s3 bucket> <source directory> <target s3 bucket> <target directory>")
        sys.exit(1)
    else:
        repository_type = "".join(sys.argv[1])
        source_bucket = "".join(sys.argv[2])
        source_dir = "".join(sys.argv[3])
        target_bucket = "".join(sys.argv[4])
        target_dir = "".join(sys.argv[5])

    generator = IndexCSVGenerator(repository_type, source_bucket, source_dir, target_bucket, target_dir)
    generator.run()
