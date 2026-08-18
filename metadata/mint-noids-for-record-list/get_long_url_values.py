import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
mintTable = dyndb.Table("")

urlValues = []

def get_records_for_minting():
        scan_kwargs = {
            "FilterExpression": Attr('long_url').contains("https://<old-domain>"),
            "ProjectionExpression": "short_id, long_url",
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

records = get_records_for_minting()
print()
print("======================================================")
print("Number of records: ", len(records))

for record in records: 
    baseURL = ("/").join(record['long_url'].split('/')[0:(len(record['long_url'].split('/')) - 2)])
    if baseURL not in urlValues:
        urlValues.append(baseURL)

print(urlValues)
print("======================================================")
print()