import boto3, os
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd

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


def query_by_index(table, index_name, value):
        index_key = index_name.lower()
        ret_val = None
        try:
            if str(index_key) == "id":
                response = table.query(
                    KeyConditionExpression=Key(index_key).eq(value), Limit=1
                )
            else:
                response = table.query(
                    IndexName=index_name,
                    KeyConditionExpression=Key(index_key).eq(value),
                    Limit=1,
                )
            if "Items" in response and len(response["Items"]) == 1:
                ret_val = response["Items"][0]
        except Exception as e:
            pass
        return ret_val

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
archiveTable = dyndb.Table("")
metaCsvs = [
     "/Users/whunter/dev/dlp/assets/hokies/meta/20250117_hokst-test_archive_metadata.csv",
     "/Users/whunter/dev/dlp/assets/hokies/meta/20250117_pprd_hokst-test_archive_metadata.csv",
     "/Users/whunter/dev/dlp/assets/hokies/meta/20250117_comsub_add_archive_metadata.csv",
     "/Users/whunter/dev/dlp/assets/hokies/meta/20250117_nonvtsrc_add_archive_metadata.csv",
     "/Users/whunter/dev/dlp/assets/hokies/meta/20250117_univpub_add_archive_metadata.csv"
]
dynamoCsv = "/Users/whunter/dev/dlp/assets/hokies/dynamo_records/20250117_Ms2020-004_results.csv"

db = csv_to_dataframe(dynamoCsv)

dbIdentifiers = db['identifier'].tolist()
modifiedIdentifiers = {}
found = []
allMetaIdentifiers = {}



# print(metaIdentifiers)
# print("--------------------------------------")
# print(dbIdentifiers)
# item = query_by_index(archiveTable, "Identifier", identifier)

for identifier in dbIdentifiers:
    modified = identifier.replace("Ms2020-004_hokst", "hokst").replace("Ms2020-004_ons", "hokst").replace("Ms2020-004_vtn", "hokst").replace("Ms2020-004_vtd", "hokst")
    modifiedIdentifiers[modified] = identifier
    
for csv in metaCsvs:
    metaFile = os.path.basename(csv)
    metaIdentifiers = csv_to_dataframe(csv)
    print(f"{csv} identifiers in dynamo:")
    for idx, row in metaIdentifiers.iterrows():
        identifier = row.identifier
        filename = row.filename
        # create a list of identifiers for each meta file if it doesn't exist
        if metaFile not in allMetaIdentifiers:
            allMetaIdentifiers[metaFile] = []

        allMetaIdentifiers[metaFile].append(identifier)

        if identifier in modifiedIdentifiers.keys():
            if identifier not in found:
                found.append(identifier)
                print(f"found {identifier}")
            else:
                print(f"already found {identifier}")
    

# print("======================================")
# print()
# print(f"found {len(found)}")
# print("======================================")
print(found)

# print("======================================")
# print(allMetaIdentifiers)
notFound = []
for identifier in modifiedIdentifiers.keys():
    if identifier not in found:
       notFound.append(identifier)

notFound = list(set(notFound))
print(f"all db identifiers {len(modifiedIdentifiers)}")
print(f"not found {len(notFound)}")
print(f"found {len(found)}")

print("======================================")
print("removing found from db")
for identifier in found:
    item = query_by_index(archiveTable, "Identifier", modifiedIdentifiers[identifier])
    response = archiveTable.delete_item(
        Key={"id": item["id"]}
    )
    print(response)
    print()

