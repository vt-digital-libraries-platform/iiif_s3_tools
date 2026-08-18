import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
table = dyndb.Table("")
field = "__typename"
value = "Archive"

def get_table_items(table):
        scan_kwargs = {
            "FilterExpression": Attr("identifier").exists(),
            "ProjectionExpression": "#id",
            "ExpressionAttributeNames": {"#id": "id"},
        }
        table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = table.scan(**scan_kwargs)
                table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return table_items


    
for record in get_table_items(table):
    resp = table.update_item(
        Key={
            "id": record["id"]
        },
        UpdateExpression=f"SET #f = :value",
        ExpressionAttributeNames={
            "#f": field
        },
        ExpressionAttributeValues={
            ":value": value
        }
    )
    print(resp)
    print()
