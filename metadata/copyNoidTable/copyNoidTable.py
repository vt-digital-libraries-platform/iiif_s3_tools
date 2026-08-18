import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
src = dyndb.Table("")
target = dyndb.Table("")

def getRecordsToCopy():
        scan_kwargs = {
            "FilterExpression": Attr('long_url').contains("https://<old-domain>"),
            "ProjectionExpression": "created_at, hits, long_url, short_id, short_url, #ttl",
            "ExpressionAttributeNames": {"#ttl": "ttl"}
        }
        tableItems = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = src.scan(**scan_kwargs)
                tableItems.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return tableItems

records = getRecordsToCopy()
print()
print("======================================================")
print("Number of records: ", len(records))

# https://federated-pprd.<amplify-app-id>.amplifyapp.com/
for record in records:
    record['long_url'] = record['long_url'].replace("https://<old-domain>", "https://federated-pprd.<amplify-app-id>.amplifyapp.com")
    record['hits'] = 0
    newNoidResponse = target.put_item(Item=record)
    success = (newNoidResponse["ResponseMetadata"]["HTTPStatusCode"] == 200)
    if success:
        print("Success: ", record['short_id'])
    else:
        print("Failed: ", record['short_id'])