import boto3, os
from boto3.dynamodb.conditions import Key, Attr

# Hopefully I don't have to tell you that you should use this with caution
# Doesn't ask for confirmation, doesn't exit on exceptions, just deletes errthing
# Just put the table name in tableThatNeedsCleanin


dyndb = boto3.resource("dynamodb", region_name="us-east-1")
tableThatNeedsCleanin = dyndb.Table("")
tableKey = "id"
tableField = "collection_category"
value = "federated"

def getRecords():
        scan_kwargs = {
            "FilterExpression": Attr(tableField).eq(value),
            "ProjectionExpression": f"#{tableKey}",
            "ExpressionAttributeNames": {f"#{tableKey}": tableKey}
        }
        table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = tableThatNeedsCleanin.scan(**scan_kwargs)
                table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e

        return table_items


def getRidOfRecord(record):
    try:
        response = tableThatNeedsCleanin.delete_item(
            Key={
                tableKey: record[tableKey]
            }
        )
        print(f"Deleted {record[tableKey]}")
        print(response)
        print()
        
    except Exception as e:
        print(f"{str(record[tableKey])}: error occurred: {str(e)}")



records = getRecords()
print()
print(f"There are/were {len(records)} records")
print("======================================================")
for record in records:
    # print(record)
    getRidOfRecord(record)
print("======================================================")
if len(records) == 0:
    print("No more records :(")