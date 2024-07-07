import os, pathlib, shutil, sys
import pandas as pd

results_path = "/home/wlh/dev/dlp/ingest/metadata/dlp-ingest/results_files"
results_filename = "sfd_ingest_results_20240704204723.csv"
results_full_path = os.path.join(results_path, results_filename)
src_dir = "/home/wlh/dev/dlp/ingest/iiif_s3_tools/batch_task_json_generator/json_files/sfd"
target_dir = f"{src_dir}_bombs"
job_file_prefix = "federated-sfd-"

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

df = csv_to_dataframe(results_full_path)

for idx, row in df.iterrows():
    if not row.succeeded:
        job_file = get_job_file_src_path(row.identifier)
        shutil.copyfile(job_file, os.path.join(target_dir, os.path.basename(job_file)))