import boto3, os
from boto3.dynamodb.conditions import Key, Attr
dyndb = boto3.resource("dynamodb", region_name="us-east-1")

# --------------------------------------------------------------
# These need to be set appropriately
# examples: *EnvString = "-j5rxpkb73zewthwrmyirtfbw6r-env"
# category = "iawa" | "federated"
prodEnvString = ""
testEnvString = ""
category = ""
# --------------------------------------------------------------

prodCollectionTable = dyndb.Table("Collection" + prodEnvString)
testCollectionTable = dyndb.Table("Collection" + testEnvString)
prodArchiveTable = dyndb.Table("Archive" + prodEnvString)
testArchiveTable = dyndb.Table("Archive" + testEnvString)


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


def getArchiveRecords(table, collection_id):
        # print(f"Scanning {table.name} for archives in collection {collection_id}...")
        scan_kwargs = {
            "FilterExpression": Attr("item_category").eq(category) & Attr("heirarchy_path").contains(collection_id) & Attr("visibility").eq(True)
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


results = {
    "prod": {
        "num_total_collections": 0,
        "num_top_level_collections": 0,
        "top_level_collections": {},
    },
    "test": {
        "num_total_collections": 0,
        "num_top_level_collections": 0,
        "top_level_collections": {},
    }
}
collectionMismatches = []
archiveCountMismatches = []

prodCollections = getCollectionRecords(prodCollectionTable)
testCollections = getCollectionRecords(testCollectionTable)
results["prod"]["num_total_collections"] = len(prodCollections)
results["test"]["num_total_collections"] = len(testCollections)
for collection in prodCollections:
     if "parent_collection" not in collection or not collection["parent_collection"]:
        results["prod"]["num_top_level_collections"] += 1
        results["prod"]["top_level_collections"][collection["identifier"]] = len(getArchiveRecords(prodArchiveTable, collection["id"]))

for collection in testCollections:
     if "parent_collection" not in collection or not collection["parent_collection"]:
        results["test"]["num_top_level_collections"] += 1
        results["test"]["top_level_collections"][collection["identifier"]] = len(getArchiveRecords(testArchiveTable, collection["id"]))
        
print("iterating prod")
prod_archive_total = 0
for collection_identifier in results["prod"]["top_level_collections"]:
    prod_archive_total += results["prod"]["top_level_collections"][collection_identifier]
    print(f"{collection_identifier}: Prod - {results['prod']['top_level_collections'][collection_identifier]}, Test - {results['test']['top_level_collections'].get(collection_identifier, 'N/A')}")
    if collection_identifier not in results["test"]["top_level_collections"]:
        if collection_identifier not in collectionMismatches:
            collectionMismatches.append(collection_identifier)
    elif results["prod"]["top_level_collections"][collection_identifier] != results["test"]["top_level_collections"][collection_identifier]:
        if collection_identifier not in archiveCountMismatches:
            archiveCountMismatches.append(collection_identifier)

print("\n-------------------\n")
print("iterating test")
test_archive_total = 0
for collection_identifier in results["test"]["top_level_collections"]:
    test_archive_total += results["test"]["top_level_collections"][collection_identifier]
    print(f"{collection_identifier}: Test - {results['test']['top_level_collections'][collection_identifier]}, Prod - {results['prod']['top_level_collections'].get(collection_identifier, 'N/A')}")
    if collection_identifier not in results["prod"]["top_level_collections"]:
        if collection_identifier not in collectionMismatches:
            collectionMismatches.append(collection_identifier)
    elif results["prod"]["top_level_collections"][collection_identifier] != results["test"]["top_level_collections"][collection_identifier]:
        if collection_identifier not in archiveCountMismatches:
            archiveCountMismatches.append(collection_identifier)

print("\n-------------------\n")
print(f"Collections: prod - {results['prod']['num_total_collections']}, test - {results['test']['num_total_collections']}")
print(f"Top Level Collections: prod - {results['prod']['num_top_level_collections']}, test - {results['test']['num_top_level_collections']}")
print("\n-------------------\n")
print(f"Top Level Collection Mismatches: {collectionMismatches or 'None'}")
print(f"Archive Count Mismatches: {archiveCountMismatches or 'None'}")
print(f"Total Archives: prod - {prod_archive_total}, test - {test_archive_total}")