import pandas as pd

first = "/path/to/dynamo/export/file.csv"
second = "/path/to/another/dynamo/export/file.csv"

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

first_df = csv_to_dataframe(first)
second_df = csv_to_dataframe(second)
second_identifiers = second_df['identifier'].tolist()

print(f"First file has {len(first_df)} rows")
print(f"Second file has {len(second_df)} rows")
print("Identifiers from first file also found in second file:")
for idx, row in first_df.iterrows():
    if row['identifier'] in second_identifiers:
        print(row['identifier'])

print()
print("-----------------------------------------------------")
print()
print("Identifiers from first file not found in second file:")
for idx, row in first_df.iterrows():
    if row['identifier'] not in second_identifiers:
        print(row['identifier'])
        