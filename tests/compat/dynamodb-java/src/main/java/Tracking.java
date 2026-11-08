// Replays what Conveyor's tracking-svc does with DynamoDB (TrackingTables,
// EventStore, ShipmentStore), with the same SDK and the async client, against
// a Stowage node. Expressions are copied verbatim from those classes.
//
//   mvn -q compile exec:java -Dexec.args=http://127.0.0.1:8481
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class Tracking {
  static DynamoDbAsyncClient dynamo;
  static int checks = 0;

  static AttributeValue s(String v) { return AttributeValue.fromS(v); }
  static AttributeValue n(Number v) { return AttributeValue.fromN(v.toString()); }

  static void check(boolean ok, String what) {
    if (!ok) throw new AssertionError(what);
    checks++;
    System.out.println("ok   " + what);
  }

  /** ShipmentStore.conditional: true if written, false if the condition failed. */
  static boolean conditional(CompletableFuture<?> f) {
    try {
      f.join();
      return true;
    } catch (CompletionException e) {
      if (e.getCause() instanceof ConditionalCheckFailedException) return false;
      throw e;
    }
  }

  static void create(String table, boolean withSortKey) {
    dynamo.createTable(r -> {
      r.tableName(table).billingMode(BillingMode.PAY_PER_REQUEST);
      if (withSortKey) {
        r.attributeDefinitions(
                AttributeDefinition.builder().attributeName("shipmentId").attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName("seq").attributeType(ScalarAttributeType.N).build())
            .keySchema(
                KeySchemaElement.builder().attributeName("shipmentId").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName("seq").keyType(KeyType.RANGE).build());
      } else {
        r.attributeDefinitions(AttributeDefinition.builder().attributeName("shipmentId").attributeType(ScalarAttributeType.S).build())
            .keySchema(KeySchemaElement.builder().attributeName("shipmentId").keyType(KeyType.HASH).build());
      }
    }).join();
  }

  static boolean recordEvent(String shipment, long seq, String scanType, long expiresAt) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("shipmentId", s(shipment));
    item.put("seq", n(seq));
    item.put("courierId", s("courier-7"));
    item.put("recordedAt", s(Instant.now().toString()));
    item.put("expiresAt", n(expiresAt));
    item.put("scanType", s(scanType));
    return conditional(dynamo.putItem(p -> p.tableName("tracking_events").item(item)
        .conditionExpression("attribute_not_exists(seq)")));
  }

  static boolean movePosition(String id, long seq, double lat, double lon) {
    return conditional(dynamo.updateItem(u -> u.tableName("shipments")
        .key(Map.of("shipmentId", s(id)))
        .updateExpression("SET lat = :lat, lon = :lon, positionSeq = :seq, positionAt = :at, courierId = :c")
        .conditionExpression("attribute_not_exists(positionSeq) OR positionSeq < :seq")
        .expressionAttributeValues(Map.of(":lat", n(lat), ":lon", n(lon), ":seq", n(seq),
            ":at", s(Instant.now().toString()), ":c", s("courier-7")))));
  }

  static boolean applyScan(String id, long seq, String scanType) {
    return conditional(dynamo.updateItem(u -> u.tableName("shipments")
        .key(Map.of("shipmentId", s(id)))
        .updateExpression("SET #st = :st, statusSeq = :seq, statusAt = :at, courierId = :c")
        .conditionExpression("(attribute_not_exists(statusSeq) OR statusSeq < :seq) AND "
            + "(attribute_not_exists(#st) OR #st <> :cancelled)")
        .expressionAttributeNames(Map.of("#st", "status"))
        .expressionAttributeValues(Map.of(":st", s(scanType), ":seq", n(seq),
            ":at", s(Instant.now().toString()), ":c", s("courier-7"), ":cancelled", s("CANCELLED")))));
  }

  static boolean settleOrder(String id, String status, Instant promised) {
    Map<String, AttributeValue> values = new HashMap<>();
    values.put(":to", s(status));
    values.put(":from", s("ORDERED"));
    String update = "SET #st = :to";
    if (promised != null) {
      update += ", promisedDelivery = :p";
      values.put(":p", s(promised.toString()));
    }
    String expression = update;
    return conditional(dynamo.updateItem(u -> u.tableName("shipments")
        .key(Map.of("shipmentId", s(id)))
        .updateExpression(expression)
        .conditionExpression("#st = :from")
        .expressionAttributeNames(Map.of("#st", "status"))
        .expressionAttributeValues(values)));
  }

  static boolean createShipment(String id) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("shipmentId", s(id));
    item.put("userId", s("user-1"));
    item.put("region", s("us-east"));
    item.put("status", s("ORDERED"));
    item.put("statusSeq", n(0L));
    item.put("positionSeq", n(0L));
    return conditional(dynamo.putItem(p -> p.tableName("shipments").item(item)
        .conditionExpression("attribute_not_exists(shipmentId)")));
  }

  static Map<String, AttributeValue> find(String id) {
    return dynamo.getItem(g -> g.tableName("shipments").key(Map.of("shipmentId", s(id))).consistentRead(true))
        .join().item();
  }

  public static void main(String[] args) throws Exception {
    dynamo = DynamoDbAsyncClient.builder()
        .endpointOverride(URI.create(args[0]))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
        .build();

    // TrackingTables.afterPropertiesSet
    create("tracking_events", true);
    create("shipments", false);
    dynamo.updateTimeToLive(r -> r.tableName("tracking_events")
        .timeToLiveSpecification(t -> t.enabled(true).attributeName("expiresAt"))).join();
    try {
      dynamo.updateTimeToLive(r -> r.tableName("tracking_events")
          .timeToLiveSpecification(t -> t.enabled(true).attributeName("expiresAt"))).join();
      check(false, "second UpdateTimeToLive fails");
    } catch (CompletionException e) {
      check(e.getCause() instanceof DynamoDbException, "second UpdateTimeToLive fails, as TrackingTables expects");
    }
    var ttl = dynamo.describeTimeToLive(r -> r.tableName("tracking_events")).join().timeToLiveDescription();
    check(ttl.timeToLiveStatus() == TimeToLiveStatus.ENABLED && "expiresAt".equals(ttl.attributeName()), "DescribeTimeToLive: ENABLED on expiresAt");

    // EventStore: idempotent append, then a paginated history read.
    long later = Instant.now().getEpochSecond() + 7 * 86400;
    for (long seq = 1; seq <= 25; seq++) recordEvent("shp-1", seq, "IN_TRANSIT", later);
    check(!recordEvent("shp-1", 7, "IN_TRANSIT", later), "re-delivered event is a ConditionalCheckFailed no-op");
    recordEvent("shp-1", 26, "DELIVERED", Instant.now().getEpochSecond() - 5); // already expired
    QueryRequest query = QueryRequest.builder().tableName("tracking_events")
        .keyConditionExpression("shipmentId = :id AND seq > :after")
        .expressionAttributeValues(Map.of(":id", s("shp-1"), ":after", n(10L)))
        .limit(4) // force several pages
        .build();
    List<Long> seqs = new java.util.ArrayList<>();
    var pages = new java.util.concurrent.atomic.AtomicInteger();
    dynamo.queryPaginator(query).subscribe(page -> {
      pages.incrementAndGet();
      page.items().forEach(i -> seqs.add(Long.parseLong(i.get("seq").n())));
    }).join();
    check(seqs.size() >= 15 && seqs.get(0) == 11 && seqs.get(14) == 25 && pages.get() >= 4,
        "queryPaginator: seq 11..25 in order over " + pages.get() + " pages");

    // ShipmentStore: the order lifecycle and the out-of-order guards.
    check(createShipment("shp-1"), "create shipment");
    check(!createShipment("shp-1"), "second create is refused (attribute_not_exists)");
    check(settleOrder("shp-1", "READY", Instant.parse("2026-11-10T17:00:00Z")), "settle ORDERED -> READY");
    check(!settleOrder("shp-1", "CANCELLED", null), "settle again is refused (#st = :from)");
    check(movePosition("shp-1", 5, 40.71, -74.0), "position seq 5");
    check(!movePosition("shp-1", 3, 40.0, -73.0), "older position seq 3 is ignored");
    check(applyScan("shp-1", 6, "OUT_FOR_DELIVERY"), "scan seq 6");
    check(!applyScan("shp-1", 4, "IN_TRANSIT"), "older scan seq 4 is ignored");
    var item = find("shp-1");
    check("OUT_FOR_DELIVERY".equals(item.get("status").s()) && "5".equals(item.get("positionSeq").n())
        && "40.71".equals(item.get("lat").n()) && item.containsKey("promisedDelivery"),
        "GetItem (consistent) sees the latest status and position");
    check(createShipment("shp-2") && settleOrder("shp-2", "CANCELLED", null) && !applyScan("shp-2", 1, "IN_TRANSIT"),
        "a cancelled order ignores scans");

    // TTL: the expired event disappears in the background.
    long deadline = System.currentTimeMillis() + 15_000;
    boolean gone = false;
    while (!gone && System.currentTimeMillis() < deadline) {
      var r = dynamo.getItem(g -> g.tableName("tracking_events")
          .key(Map.of("shipmentId", s("shp-1"), "seq", n(26L))).consistentRead(true)).join();
      gone = !r.hasItem() || r.item().isEmpty();
      if (!gone) Thread.sleep(250);
    }
    check(gone, "the expired event (expiresAt in the past) is deleted by TTL");
    System.out.println(checks + " checks passed");
    dynamo.close();
  }
}
