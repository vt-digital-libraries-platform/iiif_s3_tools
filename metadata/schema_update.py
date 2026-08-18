import boto3, os
from boto3.dynamodb.conditions import Key, Attr

dyndb = boto3.resource("dynamodb", region_name="us-east-1")
archiveTable = dyndb.Table("")
collectionTable = dyndb.Table("")

def get_table_items(table):
        scan_kwargs = {
            "FilterExpression": Attr("identifier").exists(),
            "ProjectionExpression": f"#id, identifier, manifest_url, archiveOptions, item_category, parent_collection_identifier",
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

def handle_item_category(record):
    if "item_category" in record:
        record["site_category"] = record["item_category"]
    return record

def handle_manifest_url(record):
    type = None
    assetUrls = {}
    try:
        type = record['manifest_url'].split('.')[-1]
    except KeyError:
        print(f"KeyError: 'manifest_url' not found in record {record['id']}")
    
    match type:
        case "json":
            assetUrls["iiif_manifest"] = record["manifest_url"]
        case "pdf":
            assetUrls["pdf"] = record["manifest_url"]
        case "mp3":
            assetUrls["audio"] = record["manifest_url"]
        case "mp4":
            assetUrls["video"] = record["manifest_url"]

    record["asset_urls"] = assetUrls

    return record


def handle_thumbnail_path(record):
    if "thumbnail_path" in record:
        if "asset_urls" not in record:
            record["asset_urls"] = {}
        record["asset_urls"]["thumbnail"] = record["thumbnail_path"]
    return record


def handle_archiveOptions(record):
    archive_options = {}
    if "archiveOptions" in record:
        archive_options = record["archiveOptions"]
    if "asset_urls" not in record:
        record["asset_urls"] = {}

    if "assets" in archive_options:
        assets = archive_options["assets"]
        if "iiif_manifest" in assets:
            record["asset_urls"]["iiif_manifest"] = assets["iiif_manifest"]
        if "x3d_config" in assets:
            record["asset_urls"]["x3d_config"] = assets["x3d_config"]
        if "x3d_src_img" in assets:
            record["asset_urls"]["x3d_src_img"] = assets["x3d_src_img"]
        if "gltf_config" in assets:
            record["asset_urls"]["gltf_obj"] = assets["gltf_config"]
        if "env_config" in assets:
            record["asset_urls"]["threeD_env"] = assets["env_config"]
        if "threeD_skybox" in assets:
            record["asset_urls"]["threeD_skybox"] = assets["threeD_skybox"]
        if "morpho_thumb" in assets:
            record["asset_urls"]["thumbnail"] = assets["morpho_thumb"]
        if "media_type" in assets:
            record["archiveOptions"]["media_type"] = assets["media_type"]
        
        del record["archiveOptions"]["assets"]
    return record


def update_record(table, record):
    update_expression = "SET "
    expression_attribute_values = {}
    expression_attribute_names = {}

    for key, value in record.items():
        if key == "id":
            continue
        update_expression += f"#{key} = :{key}, "
        expression_attribute_values[f":{key}"] = value
        expression_attribute_names[f"#{key}"] = key

    update_expression = update_expression.rstrip(", ")

    table.update_item(
        Key={"id": record["id"]},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_names
    )


def remove_old_fields_from_record(table, fields_to_remove):
    update_expression = "REMOVE "
    expression_attribute_names = {}
    for field in fields_to_remove:
        update_expression += f"#{field}, "
        expression_attribute_names[f"#{field}"] = field

    update_expression = update_expression.rstrip(", ")
    try:
        table.update_item(
            Key={"id": record["id"]},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names
        )
    except Exception as e:
        print(f"Error removing field {field} from record {record['id']}: {str(e)}")


# item_category => site_category
# collection_category => site_category
# manifest_url => asset_urls{}
# add embargo fields to archive|collection
# add collection_map to collection


### relationships
# add sub_collections to collection
# add archives to collection
# add parent_collection_identifier to archive|collection
archive_fields_to_remove = [
    "item_category",
    "manifest_url",
    "thumbnail_path"
]

for record in get_table_items(archiveTable):
    record = handle_item_category(record)
    record = handle_manifest_url(record)
    record = handle_archiveOptions(record)
    record = handle_thumbnail_path(record)
    print(record)
    update_record(archiveTable, record)

    # remove_old_fields_from_record(archiveTable, archive_fields_to_remove)











# "assets": {
#     "iiif_manifest": "https://d21nnzi4oh5qvs.cloudfront.net/federated/vtec/VTEC000007235/manifest.json",
#     "media_type": "3d_2diiif",
#     "x3d_config": "https://d21nnzi4oh5qvs.cloudfront.net/federated/vtec/VTEC000007235/3d/LowRes_VTEC000007235_X3D.x3d",
#     "x3d_src_img": "https://d21nnzi4oh5qvs.cloudfront.net/federated/vtec/VTEC000007235/3d/LowRes_VTEC000007235_X3D.png"

#      "env_config": "https://d21nnzi4oh5qvs.cloudfront.net/federated/3d/gltf/studio.env",
#    "gltf_config": "https://d21nnzi4oh5qvs.cloudfront.net/federated/3d/gltf/Egg1GlbTest.glb",
#    "media_type": "3d-model/gltf"
# }


    # resp = archiveTable.update_item(
    #     Key={
    #         "id": record["id"]
    #     },
    #     UpdateExpression="remove #c",
    #     ExpressionAttributeNames={ '#c': src_field }
    # )

    # altered_record = archiveTable.get_item(
    #     Key={
    #         "id": record["id"]
    #     }
    # )["Item"]
    # try:
    #     print(f"{altered_record['id']} - {altered_record[dest_field]}")
    #     print()
    # except KeyError:
    #     print(f"Error: {altered_record['id']} does not have the field {dest_field}")
    