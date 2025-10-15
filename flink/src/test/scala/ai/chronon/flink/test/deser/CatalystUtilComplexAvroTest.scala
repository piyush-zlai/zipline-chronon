package ai.chronon.flink.test.deser

import ai.chronon.api.{Accuracy, Builders, GroupBy}
import ai.chronon.flink.deser.{DeserializationSchemaBuilder, ProjectedEvent, SourceProjectionDeserializationSchema}
import ai.chronon.online.serde.{AvroCodec, AvroSerDe}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.flink.api.common.functions.util.ListCollector
import org.scalatest.flatspec.AnyFlatSpec

import java.io.ByteArrayOutputStream
import java.time.Instant
import java.util
import java.util.{Base64, UUID, HashMap => JHashMap}
import scala.collection.JavaConverters._

// This test sanity checks large schemas that have a few hundred fields and confirms
// that we are able to run the catalyst expression through them without issues
class CatalystUtilComplexAvroTest extends AnyFlatSpec {
  import LargeEventPayloadGenerator._

  val testGroupBy: GroupBy = {
    val selects = Map(
      "favorite" -> "IF(event = 'favorite', 1, 0)",
      "item_id" -> "EXPLODE(TRANSFORM(SPLIT(COALESCE(attributes.sold_item_ids, attributes.item_id), ','), e -> CAST(e AS LONG)))",
      "add_cart" -> "IF(event = 'add_cart', 1, 0)",
      "purchase" -> "IF(event = 'payment', 1, 0)",
      "view" -> "IF(event = 'view',  1, 0)"
    )

    val wheres: Seq[String] = Seq(
      "event in ('add_cart', 'view', 'payment', 'favorite')",
      "( (attributes.code in ('1', '3') AND attributes.test_code in ('1', '3')) OR ((NOT attributes.code IS NOT NULL) AND " +
        "(NOT attributes.test_code IS NOT NULL) AND attributes.region in ('US', 'CA', 'AU', 'MX', 'JP', 'NZ', 'BR', 'CN') AND logger = 'native' AND src in ('ios', 'android')) )",
      "attributes.canary IS NULL OR attributes.canary != 'true'",
      "attributes.support IS NULL OR attributes.support != 'true'"
    )

    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_item_data",
          topic = "my_item_data",
          query = Builders.Query(
            selects = selects,
            wheres = wheres,
            timeColumn = "unix_millis(TIMESTAMP(timestamp))",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("item_id"),
      aggregations = Seq.empty,
      metaData = Builders.MetaData(
        name = "item_event_group_by"
      ),
      accuracy = Accuracy.TEMPORAL
    )
  }

  def processEvent(base64Payload: String): Seq[Map[String, Any]] = {
    val payloadBytes = java.util.Base64.getDecoder.decode(base64Payload)
    val avroSchema = AvroCodec.of(testSchema.toString).schema
    val avroSerDe = new AvroSerDe(avroSchema)

    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(avroSerDe, testGroupBy)
    deserSchema.open(new DummyInitializationContext)
    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(payloadBytes, listCollector)

    resultList.asScala.map(_.fields)
  }

  private def validateQueryResults(result: Seq[Map[String, Any]],
                                   isFavorite: Boolean,
                                   isAddCart: Boolean,
                                   isPurchase: Boolean,
                                   isView: Boolean): Unit = {
    assert(result.size == 2)
    assert(result.map(r => r("item_id")).toSet == Set(123456L, 789012L))
    assert(result.map(r => r("favorite")).toSet == Set(if (isFavorite) 1 else 0))
    assert(result.map(r => r("add_cart")).toSet == Set(if (isAddCart) 1 else 0))
    assert(result.map(r => r("purchase")).toSet == Set(if (isPurchase) 1 else 0))
    assert(result.map(r => r("view")).toSet == Set(if (isView) 1 else 0))
    assert(result.map(r => r("ts")).toSet == Set(fixedTimestamp, fixedTimestamp))
  }

  it should "match event condition (add_cart)" in {
    val addToCartEvent = createModifiedEvent(
      Map(
        "event" -> "add_cart",
        "logger" -> "native",
        "src" -> "ios",
        "attributes.region" -> "US"
      ))
    val payloadBase64 = serializeToBase64(addToCartEvent)
    val result = processEvent(payloadBase64)
    // expect 2 rows for each of the listings, we check those
    validateQueryResults(result, isFavorite = false, isAddCart = true, isPurchase = false, isView = false)
  }

  it should "match event condition (view) with code indicating consent" in {
    val viewListingWithMatchingCode = createModifiedEvent(
      Map(
        "event" -> "view",
        "attributes.code" -> "1",
        "attributes.test_code" -> "1"
      ))
    val payloadBase64 = serializeToBase64(viewListingWithMatchingCode)
    val result = processEvent(payloadBase64)
    // expect 2 rows for each of the listings, we check those
    validateQueryResults(result, isFavorite = false, isAddCart = false, isPurchase = false, isView = true)
  }

  it should "match event condition (payment) with regional condition" in {
    val purchaseEventRegional = createModifiedEvent(
      Map(
        "event" -> "payment",
        "logger" -> "native",
        "src" -> "android",
        "attributes.region" -> "CA"
      ))
    val payloadBase64 = serializeToBase64(purchaseEventRegional)
    val result = processEvent(payloadBase64)
    // expect 2 rows for each of the listings, we check those
    validateQueryResults(result, isFavorite = false, isAddCart = false, isPurchase = true, isView = false)
  }

  it should "match event condition (favorite) with listing IDs" in {
    val favoriteEvent = createModifiedEvent(
      Map(
        "event" -> "favorite",
        "logger" -> "native",
        "src" -> "ios",
        "attributes.region" -> "JP",
        "attributes.item_id" -> "789012,456789"
      ))
    val payloadBase64 = serializeToBase64(favoriteEvent)
    val result = processEvent(payloadBase64)
    // expect 2 rows for each of the listings, we check those
    assert(result.size == 2)
    assert(result.map(r => r("item_id")).map(_.toString).toSet == Set("456789", "789012"))
    assert(result.map(r => r("favorite")).toSet == Set(1))
  }

  it should "NOT match (bot flag is true)" in {
    val botEvent = createModifiedEvent(
      Map(
        "event" -> "view",
        "logger" -> "native",
        "src" -> "android",
        "attributes.region" -> "US",
        "attributes.canary" -> "true"
      ))
    val payloadBase64 = serializeToBase64(botEvent)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }

  it should "NOT match (support login is true)" in {
    val supportLoginEvent = createModifiedEvent(
      Map(
        "event" -> "add_cart",
        "logger" -> "native",
        "src" -> "ios",
        "attributes.region" -> "MX",
        "attributes.support" -> "true"
      ))
    val payloadBase64 = serializeToBase64(supportLoginEvent)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }

  it should "NOT match (wrong event)" in {
    val wrongEventName = createModifiedEvent(
      Map(
        "event" -> "search",
        "logger" -> "native",
        "src" -> "ios",
        "attributes.region" -> "US"
      ))
    val payloadBase64 = serializeToBase64(wrongEventName)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }

  it should "NOT match (incompatible region & not matching code)" in {
    val wrongRegion = createModifiedEvent(
      Map(
        "event" -> "view",
        "logger" -> "native",
        "src" -> "ios",
        "attributes.region" -> "UK"
      ))
    val payloadBase64 = serializeToBase64(wrongRegion)
    val result = processEvent(payloadBase64)
    assert(result.isEmpty) // expect no results here
  }
}

object LargeEventPayloadGenerator {
  val testSchema: Schema = new Schema.Parser().parse(
    """{"type":"record","name":"LargeEvent","namespace":"com.customer","fields":[{"name":"event","type":"string"},{ "name": "timestamp", "type": ["null", { "type": "long", "logicalType": "timestamp-millis" }], "default": null },{"name":"browser","type":["null","string"],"default":null},{"name":"primary","type":"boolean"},{"name":"id","type":"string"},{"name":"page_id","type":"string"},{"name":"logger","type":"string"},{"name":"src","type":"string"},{"name":"ip","type":"string"},{"name":"user_agent","type":"string"},{"name":"loc","type":"string"},{"name":"ref","type":"string"},{"name":"cookie_map","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"ab","type":["null",{"type":"map","values":{"type":"array","items":"string"}}],"default":null},{"name":"user","type":["null","long"],"default":null},{"name":"mobile_request","type":["null","boolean"],"default":null},{"name":"mobile_device","type":["null","boolean"],"default":null},{"name":"mobile_template","type":["null","boolean"],"default":null},{"name":"currency","type":["null","string"],"default":null},{"name":"language","type":["null","string"],"default":null},{"name":"region","type":["null","string"],"default":null},{"name":"item_ids","type":["null",{"type":"array","items":"long"}],"default":null},{"name":"event_timestamp","type":["null","long"],"default":null},{"name":"attrs","type":["null",{"type":"map","values":"string"}],"default":null},{"name":"attributes","type":{"type":"record","name":"Attributes","fields":[{"name":"code","type":["null","string"],"default":null},{"name":"test_code","type":["null","string"],"default":null},{"name":"region","type":["null","string"],"default":null},{"name":"canary","type":["null","string"],"default":null},{"name":"support","type":["null","string"],"default":null},{"name":"item_id","type":["null","string"],"default":null},{"name":"sold_item_ids","type":["null","string"],"default":null}]}}]}"""
  )

  // Create writer for serializing records
  val writer = new SpecificDatumWriter[GenericRecord](testSchema)

  val fixedTimestamp: Long = Instant.parse("2025-06-06T00:00:00Z").toEpochMilli

  // Function to create a base event record
  def createBaseEvent(): GenericRecord = {
    val record = new GenericData.Record(testSchema)

    // Set default values
    record.put("event", "view")
    record.put("timestamp", fixedTimestamp)
    record.put("browser", "test-browser-id")
    record.put("primary", true)
    record.put("id", s"test-id-${UUID.randomUUID().toString.take(8)}")
    record.put("page_id", s"test-page-id-${UUID.randomUUID().toString.take(8)}")
    record.put("logger", "web")
    record.put("src", "web")
    record.put("ip", "127.0.0.1")
    record.put("user_agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    record.put("loc", "https://customer.com/item_id/123456")
    record.put("ref", "")

    // Create cookies map
    val cookies = new JHashMap[String, String]()
    cookies.put("test-cookie", "test-value")
    record.put("cookie_map", cookies)

    // Create A/B test map
    val ab = new JHashMap[String, java.util.List[String]]()
    ab.put("test-experiment", List("variant_a").asJava)
    record.put("ab", ab)

    record.put("user", 12345L)
    record.put("mobile_request", false)
    record.put("mobile_device", false)
    record.put("mobile_template", false)
    record.put("currency", "USD")
    record.put("language", "en-US")
    record.put("region", "US")

    // Create item_ids array
    val itemIds = new GenericData.Array[Long](2, testSchema.getField("item_ids").schema().getTypes.get(1))
    itemIds.add(123456L)
    itemIds.add(789012L)
    record.put("item_ids", itemIds)

    record.put("event_timestamp", Instant.now().toEpochMilli)

    // Create attributes map
    val attrs = new JHashMap[String, String]()
    attrs.put("test-prop", "test-value")
    record.put("attrs", attrs)

    // Create attributes record
    record.put("attributes", createDefaultAttributes())

    record
  }

  // Function to create default attributes record
  def createDefaultAttributes(): GenericRecord = {
    val attributesSchema = testSchema.getField("attributes").schema()
    val attrs = new GenericData.Record(attributesSchema)

    // Set all fields to null by default
    attrs.put("code", null)
    attrs.put("test_code", null)
    attrs.put("region", "US")

    attrs.put("canary", null)
    attrs.put("support", null)

    attrs.put("item_id", "123456,789012")
    attrs.put("sold_item_ids", null)

    attrs
  }

  // Create a modified event with specific property changes
  def createModifiedEvent(modifications: Map[String, Any]): GenericRecord = {
    val record = createBaseEvent()

    modifications.foreach { case (key, value) =>
      if (key.contains('.')) {
        // Handle nested properties
        val Array(parent, child) = key.split('.')
        val parentRecord = record.get(parent).asInstanceOf[GenericRecord]
        parentRecord.put(child, value)
      } else {
        record.put(key, value)
      }
    }

    record
  }

  // Serialize a record to Avro binary and encode as Base64
  def serializeToBase64(record: GenericRecord): String = {
    val baos = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)

    writer.write(record, encoder)
    encoder.flush()

    Base64.getEncoder.encodeToString(baos.toByteArray)
  }
}
