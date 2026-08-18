import boto3, os
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
archiveTable = dyndb.Table("your-dynamodb-table-name")
metadataCSV = "/path/to/your/archive_metadata.csv"
visibility = True

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



records = csv_to_dataframe(metadataCSV)
    
for idx, record in records.iterrows():
    item = query_by_index(archiveTable, "Identifier", record.identifier)
    archiveTable.update_item(
        Key={
            "id": item["id"]
        },
        UpdateExpression="SET visibility = :true",
        ExpressionAttributeValues={
            ":true": visibility
        }
    )
