import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
table = dyndb.Table("")
field = "archived"
value = True

def get_table_items(table, field):
    scan_kwargs = {
        "FilterExpression": Attr(field).not_exists(),
        "ProjectionExpression": f"#id, {field}",
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


    
for record in get_table_items(table, field):
    resp = table.update_item(
        Key={
            "id": record["id"]
        },
        UpdateExpression=f"SET {field} = :field",
        ExpressionAttributeValues={
            ":field": value
        }
    )
    print(resp)
    print()
