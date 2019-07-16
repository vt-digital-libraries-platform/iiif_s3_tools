#!/usr/local/bin/python3

# python3 index_csv_generator.py sources3folder, collectionname, targets3bucket

import boto3
import os
import sys

class IndexCSVGenerator(object):

    def __init__(self, source, name, target_bucket):
        self.source = source
        self.name = name
        self.target_bucket = target_bucket
        self.target_file_name = "index.csv"

        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket = self.s3_resource.Bucket(self.target_bucket)

    def run(self):
        objects = self.get_objects()
        csvs = self.get_csvs(objects)
        output_string = ""
        for csv_file in csvs:
            output_string += csv_file.strip("iawa/") + ","
            basepath = self.get_basepath(csv_file)
            manifest_string = self.get_manifest_string(basepath, objects)
            output_string += (manifest_string + "\n")
        local_temp_file_name = "output_temp.csv"
        self.write_local_temp(output_string, local_temp_file_name)
        self.write_to_s3(local_temp_file_name)
        self.delete_local_temp(local_temp_file_name)

    def get_objects(self):
        return [bucket_object.key for bucket_object in self.bucket.objects.filter(Prefix=self.source)]

    def get_csvs(self, objects):
        csvs = []
        for obj in objects:
          if obj.endswith(".csv"):
              csvs.append(obj)
        return csvs

    def get_basepath(self, file_path):
        return os.path.dirname(file_path)

    def get_manifest_string(self, basepath, objects):
        manifest_path_list = []
        for file in objects:
            if file.startswith(basepath) and file.endswith('manifest.json'):
                manifest_path_list.append(self.get_basepath(file).strip("iawa/"))
        return (',').join(manifest_path_list)

    def write_local_temp(self, output_string, file_name):
        f = open(file_name, "w")
        f.write(output_string)
        f.close

    def delete_local_temp(self, file_name):
        os.remove(file_name)

    def write_to_s3(self, local_file):
        key = self.name + "." + self.target_file_name
        self.s3_client.upload_file(local_file, self.target_bucket, key)

if __name__ == '__main__':

    if len(sys.argv) < 4:
        print("Usage: python3 index_csv_generator.py <source directory> <name> <target s3 bucket>")
        sys.exit(1)
    else:
        source_dir = "".join(sys.argv[1])
        name = "".join(sys.argv[2])
        target_dir = "".join(sys.argv[3])

    generator = IndexCSVGenerator(source_dir, name, target_dir)
    generator.run()
