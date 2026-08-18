import boto3, os
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
mintTable = dyndb.Table("")
tableKey = "id"
site = "iawa"


urlValues = []

def get_records_for_minting(): 
        scan_kwargs = {
            "FilterExpression": Attr(f"{category}_category").eq(site),
            "ProjectionExpression": f"#{tableKey}, custom_key, identifier",
            "ExpressionAttributeNames": {f"#{tableKey}": f"{tableKey}" }
        }
        table_items = []
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = recordTable.scan(**scan_kwargs)
                table_items.extend(response["Items"])
                start_key = response.get("LastEvaluatedKey", None)
                done = start_key is None
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            raise e
        return table_items

types = ["Archive", "Collection"]
for type in types:
    recordTable = dyndb.Table(f"{type}-77eik3yv7rbdbjhjemas6h7dmi-vtdlppprd")
    category = "item" if type == "Archive" else "collection"
    long_url = f"https://{site}-pprd.d3reyduta3lkkz.amplifyapp.com/{type.lower()}/"
    records = get_records_for_minting()
    numCreated = 0
    print(f"{len(records)} records found for {type} in {site}")
    print("======================================================")
    for record in records: 
        now = datetime.now()
        insert_date_str = now.strftime("%Y-%m-%dT%H:%M:%S")
        short_id = record['custom_key'].replace("ark:/53696/","")
        noidRecord = {
            "short_id": short_id,
            "long_url": os.path.join(long_url, short_id),
            "short_url": os.path.join("https://065ykk5p42.execute-api.us-east-1.amazonaws.com/Prod/ark:/53696", short_id),
            "created_at": insert_date_str,
            "hits": 0,
            "ttl": 4129578000,
            "type": type,
            "site": site,
            "identifier": record["identifier"]
        }
        mintTable.put_item(Item=noidRecord)
        numCreated += 1
        print(noidRecord)
        print("Success: ", short_id)
        print("======================================================")
    print(f"{site}: {type}")
    print("Number of records created: ", numCreated)
     
        
