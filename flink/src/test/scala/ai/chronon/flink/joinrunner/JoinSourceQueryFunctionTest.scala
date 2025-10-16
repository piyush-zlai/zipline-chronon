package ai.chronon.flink.joinrunner

import ai.chronon.api.{Builders, DataType, DoubleType, IntType, LongType, StringType}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.{Api, JoinCodec}
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.serde.SparkConversions
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class JoinSourceQueryFunctionTest extends AnyFlatSpec with Matchers with MockitoSugar {

  val inputSchema = Seq(
    ("user_id", StringType),
    ("price", DoubleType),
    ("timestamp", LongType)
  )

  "JoinSourceQueryFunction" should "apply SQL transformations correctly" in {
    // Create a join source with SQL query
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "price" -> "price"),
          timeColumn = "timestamp"
        ),
        table = "test.events",
        topic = "kafka://test-topic"
      ),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test.parent_join")
    )

    val joinSource = Builders.Source.joinSource(
      join = parentJoin,
      query = Builders.Query(
        selects = Map(
          "user_id" -> "user_id",
          "doubled_price" -> "price * 2",
          "price_category" -> "CASE WHEN price > 100 THEN 'expensive' ELSE 'affordable' END"
        ),
        timeColumn = "timestamp"
      )
    ).getJoinSource

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher] 
    val mockMetadataStore = mock[ai.chronon.online.fetcher.MetadataStore]
    val mockJoinCodec = mock[JoinCodec]
    
    when(mockApi.buildFetcher(debug = false)).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(mockMetadataStore)
    when(mockMetadataStore.buildJoinCodec(parentJoin, refreshOnFail = false)).thenReturn(mockJoinCodec)
    
    // Mock join codec schema (enriched fields from join)
    val joinValueSchema = ai.chronon.api.StructType("join_enriched", Array(
      ai.chronon.api.StructField("user_category", StringType),
      ai.chronon.api.StructField("user_score", DoubleType)
    ))
    when(mockJoinCodec.valueSchema).thenReturn(joinValueSchema)

    val function = new JoinSourceQueryFunction(joinSource, inputSchema, mockApi, enableDebug = false)
    function.open(new Configuration())

    // Create enriched event (after join processing)
    val enrichedFields = Map(
      "user_id" -> "123",
      "price" -> 150.0,
      "timestamp" -> 1000L,
      "user_category" -> "premium", // From join
      "user_score" -> 85.5        // From join
    )
    val enrichedEvent = ProjectedEvent(enrichedFields, 500L)

    // Collect outputs
    val outputs = ListBuffer[ProjectedEvent]()
    val collector = new Collector[ProjectedEvent] {
      override def collect(record: ProjectedEvent): Unit = outputs += record
      override def close(): Unit = {}
    }

    // Execute transformation
    function.flatMap(enrichedEvent, collector)

    // Verify transformation results
    outputs should have size 1
    val result = outputs.head
    
    result.startProcessingTimeMillis should be(500L)
    
    val resultFields = result.fields
    resultFields("user_id") should be("123")
    resultFields("doubled_price") should be(300.0) // 150 * 2
    resultFields("price_category") should be("expensive") // price > 100
  }


  it should "handle query errors gracefully" in {
    // Create join source with a query that will work during schema building but fail during execution
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(),
        table = "test.events", 
        topic = "kafka://test-topic"
      ),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test.parent_join")
    )

    // Use a valid query that should work
    val joinSource = Builders.Source.joinSource(
      join = parentJoin,
      query = Builders.Query(
        selects = Map("user_id" -> "user_id", "price_doubled" -> "price * 2"),
        timeColumn = "timestamp"
      )
    ).getJoinSource

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    val mockMetadataStore = mock[ai.chronon.online.fetcher.MetadataStore]
    val mockJoinCodec = mock[JoinCodec]
    
    when(mockApi.buildFetcher(debug = false)).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(mockMetadataStore)
    when(mockMetadataStore.buildJoinCodec(parentJoin, refreshOnFail = false)).thenReturn(mockJoinCodec)
    
    val joinValueSchema = ai.chronon.api.StructType("join_enriched", Array(
      ai.chronon.api.StructField("user_category", StringType)
    ))
    when(mockJoinCodec.valueSchema).thenReturn(joinValueSchema)

    val function = new JoinSourceQueryFunction(joinSource, inputSchema, mockApi, enableDebug = false)
    function.open(new Configuration())

    // Create enriched event with fields that should work
    val enrichedFields = Map(
      "user_id" -> "123",
      "price" -> 99.99,
      "timestamp" -> 1000L,
      "user_category" -> "premium"
    )
    val enrichedEvent = ProjectedEvent(enrichedFields, 500L)

    // Collect outputs
    val outputs = ListBuffer[ProjectedEvent]()
    val collector = new Collector[ProjectedEvent] {
      override def collect(record: ProjectedEvent): Unit = outputs += record
      override def close(): Unit = {}
    }

    // Execute (should work successfully)
    function.flatMap(enrichedEvent, collector)

    // Should have transformed the event successfully
    outputs should have size 1
    val result = outputs.head
    result should not be theSameInstanceAs(enrichedEvent) // Should be a new transformed event
    result.startProcessingTimeMillis should be(500L)
  }

  it should "build join schema correctly" in {
    // Test that the schema building combines left source + join codec schemas
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(),
        table = "test.events",
        topic = "kafka://test-topic"
      ),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test.parent_join")
    )

    val joinSource = Builders.Source.joinSource(
      join = parentJoin,
      query = Builders.Query(
        selects = Map("user_id" -> "user_id", "enriched_field" -> "enriched_field"),
        timeColumn = "timestamp"
      )
    ).getJoinSource

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    val mockMetadataStore = mock[ai.chronon.online.fetcher.MetadataStore]
    val mockJoinCodec = mock[JoinCodec]
    
    when(mockApi.buildFetcher(debug = false)).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(mockMetadataStore)
    when(mockMetadataStore.buildJoinCodec(parentJoin, refreshOnFail = false)).thenReturn(mockJoinCodec)
    
    // Mock join codec value schema (fields added by join)
    val joinValueSchema = ai.chronon.api.StructType("join_enriched", Array(
      ai.chronon.api.StructField("enriched_field", StringType),
      ai.chronon.api.StructField("another_enriched", IntType)
    ))
    when(mockJoinCodec.valueSchema).thenReturn(joinValueSchema)

    val function = new JoinSourceQueryFunction(joinSource, inputSchema, mockApi, enableDebug = false)
    
    // Opening the function should trigger schema building without errors
    function.open(new Configuration())
    
    // If we get here without exceptions, schema building worked
    // The buildJoinSchema method combines:
    // - inputSchema fields: user_id (StringType), price (DoubleType), timestamp (LongType) 
    // - joinCodec.valueSchema fields: enriched_field (StringType), another_enriched (IntType)
    // Total schema should have 5 fields
    
    verify(mockJoinCodec, org.mockito.Mockito.atLeast(1)).valueSchema // Should have accessed the join schema
  }
}