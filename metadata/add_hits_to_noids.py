import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
mintTable = dyndb.Table("")

def get_noids_wo_hits():
        scan_kwargs = {
            "FilterExpression": Attr('hits').not_exists(),
            "ProjectionExpression": "short_id",
        }
        table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = mintTable.scan(**scan_kwargs)
                table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return table_items

noHits = get_noids_wo_hits()
print("Number of noids without hits: ", len(noHits))

for record in noHits:
    mintTable.update_item(
        Key={
            "short_id": record["short_id"]
        },
        UpdateExpression="SET hits = :hits",
        ExpressionAttributeValues={
            ":hits": 0
        }
    )
    print(record)


    




