import boto3, os
from boto3.dynamodb.conditions import Key, Attr

def get_table_items(table, srcField):
        scan_kwargs = {
            "FilterExpression": Attr(srcField).exists(),
            "ProjectionExpression": f"#id, {srcField}",
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


dyndb = boto3.resource("dynamodb", region_name="us-east-1")
archiveTable = dyndb.Table("")
src_field = "item_category"
dest_field = "category"


# copy values to new attribute    
for record in get_table_items(archiveTable, src_field):
    resp = archiveTable.update_item(
        Key={
            "id": record["id"]
        },
        UpdateExpression=f"SET {dest_field} = :src_field",
        ExpressionAttributeValues={
            ":src_field": record[src_field]
        }
    )
    print(resp)
    print()

# delete old attribute values
for record in get_table_items(archiveTable, src_field):
    resp = archiveTable.update_item(
        Key={
            "id": record["id"]
        },
        UpdateExpression="remove #c",
        ExpressionAttributeNames={ '#c': src_field }
    )
    print(resp)
    print()