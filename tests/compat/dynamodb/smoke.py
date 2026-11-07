"""DynamoDB-compatibility smoke test with the real AWS SDK (boto3).

    python tests/compat/dynamodb/smoke.py http://127.0.0.1:8481
"""
import sys, time
import boto3
from botocore.exceptions import ClientError

url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8481"
ddb = boto3.client("dynamodb", endpoint_url=url, region_name="us-east-1",
                   aws_access_key_id="x", aws_secret_access_key="x")
streams = boto3.client("dynamodbstreams", endpoint_url=url, region_name="us-east-1",
                       aws_access_key_id="x", aws_secret_access_key="x")
ok = lambda m: print("ok  ", m)

def code(f):
    try:
        f()
    except ClientError as e:
        return e.response["Error"]["Code"]
    return None

name = f"orders_{int(time.time())}"
ddb.create_table(
    TableName=name,
    AttributeDefinitions=[{"AttributeName": a, "AttributeType": t} for a, t in
                          [("customer", "S"), ("order_id", "S"), ("status", "S"), ("total", "N")]],
    KeySchema=[{"AttributeName": "customer", "KeyType": "HASH"}, {"AttributeName": "order_id", "KeyType": "RANGE"}],
    GlobalSecondaryIndexes=[{"IndexName": "by_status", "KeySchema": [
        {"AttributeName": "status", "KeyType": "HASH"}, {"AttributeName": "total", "KeyType": "RANGE"}],
        "Projection": {"ProjectionType": "ALL"}}],
    BillingMode="PAY_PER_REQUEST",
    StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"})
assert name in ddb.list_tables()["TableNames"]
desc = ddb.describe_table(TableName=name)["Table"]
assert desc["GlobalSecondaryIndexes"][0]["IndexName"] == "by_status"
ok("create / list / describe table")

for i, (cust, st, total) in enumerate([("acme", "paid", 120), ("acme", "open", 40), ("acme", "paid", 15), ("globex", "paid", 99), ("globex", "open", 7)]):
    ddb.put_item(TableName=name, Item={"customer": {"S": cust}, "order_id": {"S": f"o{i:03}"}, "status": {"S": st},
                                       "total": {"N": str(total)}, "tags": {"SS": ["new"]}},
                 ConditionExpression="attribute_not_exists(order_id)")
assert code(lambda: ddb.put_item(TableName=name, Item={"customer": {"S": "acme"}, "order_id": {"S": "o000"}},
                                 ConditionExpression="attribute_not_exists(order_id)")) == "ConditionalCheckFailedException"
ok("conditional put (and its failure)")

item = ddb.get_item(TableName=name, Key={"customer": {"S": "acme"}, "order_id": {"S": "o000"}})["Item"]
assert item["total"] == {"N": "120"}
r = ddb.update_item(TableName=name, Key={"customer": {"S": "acme"}, "order_id": {"S": "o000"}},
                    UpdateExpression="SET total = total + :d, #s = :s ADD tags :t REMOVE gone",
                    ConditionExpression="#s = :paid", ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":d": {"N": "-20"}, ":s": {"S": "shipped"}, ":t": {"SS": ["vip"]}, ":paid": {"S": "paid"}},
                    ReturnValues="ALL_NEW")["Attributes"]
assert r["total"] == {"N": "100"} and r["status"] == {"S": "shipped"} and sorted(r["tags"]["SS"]) == ["new", "vip"], r
ok("get / update with SET, ADD, REMOVE, a condition, ReturnValues=ALL_NEW")

q = ddb.query(TableName=name, KeyConditionExpression="customer = :c AND begins_with(order_id, :p)",
              ExpressionAttributeValues={":c": {"S": "acme"}, ":p": {"S": "o00"}})
assert q["Count"] == 3, q
q = ddb.query(TableName=name, KeyConditionExpression="customer = :c AND order_id BETWEEN :a AND :b",
              FilterExpression="total > :min", ExpressionAttributeValues={":c": {"S": "acme"}, ":a": {"S": "o000"}, ":b": {"S": "o001"}, ":min": {"N": "50"}})
assert [i["order_id"]["S"] for i in q["Items"]] == ["o000"] and q["ScannedCount"] == 2, q
q = ddb.query(TableName=name, KeyConditionExpression="customer = :c", ScanIndexForward=False, Limit=1,
              ExpressionAttributeValues={":c": {"S": "acme"}})
assert q["Items"][0]["order_id"]["S"] == "o002", q
ok("query: begins_with, BETWEEN + filter, descending")

seen, start = [], None
while True:
    kw = {"ExclusiveStartKey": start} if start else {}
    page = ddb.scan(TableName=name, Limit=2, **kw)
    seen += page["Items"]
    start = page.get("LastEvaluatedKey")
    if not start:
        break
assert len(seen) == 5, len(seen)
ok("scan with pagination (Limit=2)")

ddb.transact_write_items(TransactItems=[
    {"Update": {"TableName": name, "Key": {"customer": {"S": "acme"}, "order_id": {"S": "o001"}},
                "UpdateExpression": "SET #s = :p", "ExpressionAttributeNames": {"#s": "status"}, "ExpressionAttributeValues": {":p": {"S": "paid"}}}},
    {"Put": {"TableName": name, "Item": {"customer": {"S": "initech"}, "order_id": {"S": "o100"}, "status": {"S": "paid"}, "total": {"N": "5"}}}},
])
try:
    ddb.transact_write_items(TransactItems=[
        {"Put": {"TableName": name, "Item": {"customer": {"S": "initech"}, "order_id": {"S": "o101"}}}},
        {"ConditionCheck": {"TableName": name, "Key": {"customer": {"S": "acme"}, "order_id": {"S": "o001"}},
                            "ConditionExpression": "#s = :o", "ExpressionAttributeNames": {"#s": "status"}, "ExpressionAttributeValues": {":o": {"S": "open"}}}},
    ])
    raise AssertionError("expected TransactionCanceledException")
except ClientError as e:
    assert e.response["Error"]["Code"] == "TransactionCanceledException", e.response
    assert [r["Code"] for r in e.response["CancellationReasons"]] == ["None", "ConditionalCheckFailed"], e.response
assert "Item" not in ddb.get_item(TableName=name, Key={"customer": {"S": "initech"}, "order_id": {"S": "o101"}})
ok("transact write: commit, and cancel with reasons (nothing applied)")

ddb.batch_write_item(RequestItems={name: [{"PutRequest": {"Item": {"customer": {"S": "zeta"}, "order_id": {"S": f"b{i}"}}}} for i in range(3)]})
got = ddb.batch_get_item(RequestItems={name: {"Keys": [{"customer": {"S": "zeta"}, "order_id": {"S": f"b{i}"}} for i in range(3)]}})
assert len(got["Responses"][name]) == 3
ok("batch write / batch get")

deadline = time.time() + 10
while True:
    g = ddb.query(TableName=name, IndexName="by_status", KeyConditionExpression="#s = :p AND total >= :m",
                  ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":p": {"S": "paid"}, ":m": {"N": "10"}})
    totals = [int(i["total"]["N"]) for i in g["Items"]]
    if totals == [15, 40, 99] or time.time() > deadline:
        break
    time.sleep(0.2)
assert totals == [15, 40, 99], totals
ok(f"GSI query by status and total (eventually consistent) → {totals}")

arn = desc["LatestStreamArn"]
shard = streams.describe_stream(StreamArn=arn)["StreamDescription"]["Shards"][0]["ShardId"]
it = streams.get_shard_iterator(StreamArn=arn, ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
recs = streams.get_records(ShardIterator=it)["Records"]
kinds = [r["eventName"] for r in recs]
assert kinds.count("INSERT") >= 9 and "MODIFY" in kinds, kinds
mod = next(r for r in recs if r["eventName"] == "MODIFY")
assert "OldImage" in mod["dynamodb"] and "NewImage" in mod["dynamodb"]
ok(f"stream: {len(recs)} records, {kinds.count('INSERT')} INSERT, {kinds.count('MODIFY')} MODIFY, with old and new images")

ddb.delete_item(TableName=name, Key={"customer": {"S": "zeta"}, "order_id": {"S": "b0"}})
assert code(lambda: ddb.get_item(TableName="nope", Key={"k": {"S": "x"}})) == "ResourceNotFoundException"
assert code(lambda: ddb.put_item(TableName=name, Item={"customer": {"S": "x"}})) == "ValidationException"
ok("delete item; ResourceNotFound and Validation errors")
ddb.delete_table(TableName=name)
assert name not in ddb.list_tables()["TableNames"]
ok("delete table")
print("all passed")
