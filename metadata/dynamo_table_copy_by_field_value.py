import boto3, os
from boto3.dynamodb.conditions import Key, Attr


dyndb = boto3.resource("dynamodb", region_name="us-east-1")
srcTable = dyndb.Table("")
destTable = dyndb.Table("")
tableKey = "id"
tableField = "item_category"
value = "federated"

def getRecords():
        scan_kwargs = {
            "FilterExpression": Attr(tableField).eq(value)
        }
        table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = srcTable.scan(**scan_kwargs)
                table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e

        return table_items


def copyRecord(record):
    try:
        response = destTable.put_item(Item=record)
        print(f"Copied {record[tableKey]}")
        # print(record)
        print()
        
    except Exception as e:
        print(f"{str(record[tableKey])}: error occurred: {str(e)}")



records = getRecords()
print()
print(f"There are/were {len(records)} records")
print("======================================================")
for record in records:
    copyRecord(record)
print("======================================================")
if len(records) == 0:
    print("No more records :(")