import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
collectionMaps = dyndb.Table("")

def get_maps():
        scan_kwargs = {
            "FilterExpression": Attr('collection_id').exists(),
            "ProjectionExpression": "#id, collection_id",
            "ExpressionAttributeNames": {"#id": "id"}
        }
        table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = collectionMaps.scan(**scan_kwargs)
                table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return table_items

for record in get_maps():
    collectionMaps.update_item(
        Key={
            "id": record["id"]
        },
        UpdateExpression="SET collectionmapCollectionId = :collection_id",
        ExpressionAttributeValues={
            ":collection_id": record["collection_id"]
        }
    )
    print(record)


    




