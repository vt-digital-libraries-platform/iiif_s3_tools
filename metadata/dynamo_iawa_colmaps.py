import boto3, json, os
from boto3.dynamodb.conditions import Key, Attr
dyndb = boto3.resource("dynamodb", region_name="us-east-1")

# --------------------------------------------------------------
# These need to be set appropriately
# examples: *EnvString = "-j5rxpkb73zewthwrmyirtfbw6r-env"
# category = "iawa" | "federated"
prodEnvString = ""
category = "iawa"
# --------------------------------------------------------------

collectionTable = dyndb.Table("Collection" + prodEnvString)
mapTable = dyndb.Table("Collectionmap" + prodEnvString)



print("\n-------------------\n")
print(f"Category: {category}")
print("\n-------------------\n")

def getCollectionRecords(table):
        print(f"Scanning {table.name} for collections...")
        scan_kwargs = {
            "FilterExpression": Attr("collection_category").eq(category) & Attr("visibility").eq(True)
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


def getMapRecord(table, collection_id):
        scan_kwargs = {
            "FilterExpression": Attr("collection_id").eq(collection_id)
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

def update_map_record(table, id, map_object):
    try:
        response = table.update_item(
            Key={
                "id": id
            },
            UpdateExpression="set map_object = :m",
            ExpressionAttributeValues={
                ":m": map_object
            },
            ReturnValues="UPDATED_NEW"
        )
        print(f"Update response: {response}")
    except Exception as e:
        print(f"An error occurred while updating: {str(e)}")
        raise e


collections = getCollectionRecords(collectionTable)
print(f"Total Collections: {len(collections)}")

records = []
for collection in collections:
    collection_id = collection["id"]
    map_record = getMapRecord(mapTable, collection_id)
    
    for map in map_record:
        records.append(map)
        # print(f"Collection: {collection_id}")
        # print(f"Map ID: {map['id']}")
        map_object = json.loads(map["map_object"])
        if "children" in map_object:
            del map_object["children"]
            print(f"map_object after deleting children: {map_object}")
            map["map_object"] = json.dumps(map_object)
            # update_map_record(mapTable, map["id"], map["map_object"])
        # print("\n-------------------\n")
print(records)
print(f"Total Map Records: {len(records)}")
