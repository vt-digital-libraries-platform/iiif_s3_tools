import boto3, os, pathlib, shutil, sys
import pandas as pd

s3 = boto3.client('s3')
collection_identifier = 'nmcst'
bucket = ''
results_path = "/path/to/batch_task_json_generator/results_files"
results_filename = f"{collection_identifier}_tiling_results_20241210.csv"
results_full_path = os.path.join(results_path, results_filename)
src_dir = f"/path/to/batch_task_json_generator/json_files/{collection_identifier}"
target_dir = f"{src_dir}_2"
job_file_prefix = f"federated-{collection_identifier}-"
metadata_path = f"/path/to/dev/dlp/assets/{collection_identifier}/meta"
metadata_filename = "20250218_fixerrors_part3_archive_metadata.csv"
job_files_not_found = []

def get_job_file_src_path(identifier):
    job_file =  f"{job_file_prefix}{identifier}.json"
    return os.path.join(src_dir, job_file)

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

if not os.path.exists(src_dir):
    print("Error: no src dir")
    sys.exit(1)

pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

# based on results file output
# df = csv_to_dataframe(results_full_path)
# for idx, row in df.iterrows():
#     if not row.succeeded:
#         job_file = get_job_file_src_path(row.identifier)
#         shutil.copyfile(job_file, os.path.join(target_dir, os.path.basename(job_file)))

# Based on presence of manifest.json file in <s3-bucket-hostname>
df = csv_to_dataframe(os.path.join(metadata_path, metadata_filename))
not_found = 1
for idx, row in df.iterrows():
    
    target_key = f"federated/{collection_identifier}/{row.identifier}/manifest.json"
    response = None
    try:
        response = s3.head_object(Bucket=bucket, Key=target_key)
    except:
        pass
    if not response:
        not_found += 1
        print(f"NOT FOUND {target_key}. copying job file")
        job_file = get_job_file_src_path(row.identifier)
        try:
            shutil.copyfile(job_file, os.path.join(target_dir, os.path.basename(job_file)))
        except FileNotFoundError:
            print("====================================")
            print()
            print()
            print(f"Error: {job_file} not found")
            print()
            print()
            print("====================================")
            job_files_not_found.append(job_file)
print("====================================")
print()
print("job files not found")
print(job_files_not_found)
  