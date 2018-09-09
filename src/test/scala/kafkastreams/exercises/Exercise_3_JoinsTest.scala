package kafkastreams.exercises

import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import com.madewithtea.mockedstreams.MockedStreams
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TimestampExtractor
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.immutable

class Exercise_3_JoinsTest extends ExerciseBase {


  val strings = Serdes.String()
  val ints = Serdes.Integer()
  val longs = Serdes.Long()
  val json = new JsonNodeSerde


  val mapper = new ObjectMapper

  /**
    *
    * These first set of cases are all non-windowed KStream-KTable joins and therefore can ignore the time aspect.
    * The timeline follows the KStream with one KTable lookup per KStream element.
    *
    */


  // Account state as an account changelog stream of the backend account system
  val accountChangeLog = List(
    //(UserId,AccountState)
    ("Alice", "free"),
    ("Alice", "paying"),
    ("Alice", "closed"),
    ("Bob", "free"),
    ("Bob", "paying"),
    ("Bob", "free"),
    ("Mallory", "free"),
    ("Mallory", "banned")
  )

  // User event is a record stream of user actions
  val userEvent = List(
    //(UserId,PageID)
    ("Alice", "pageA"),
    ("Bob", "pageA"),
    ("Mallory", "pageA"),
    ("Alice", "pageB"),
    ("Bob", "pageB"),
    ("Mallory", "pageC")
  )


  test("Enrich user event with account state (free/paying/closed/banned)") {

    // We expect the output to have the user name as key, and the value be a string  <page name>-<account state>
    val expected = List(
      "Alice" -> "pageA-closed",
      "Bob" -> "pageA-free",
      "Mallory" -> "pageA-banned",
      "Alice" -> "pageB-closed",
      "Bob" -> "pageB-free",
      "Mallory" -> "pageC-banned"

    )

    val result = MockedStreams()
      .topology(builder => exercise3.accountStateJoin(builder))
      .input("account-state-changelog", strings, strings, accountChangeLog)
      .input("user-events", strings, strings, userEvent)
      .output("user-events-with-accountstate", strings, strings, expected.length)

    result should be(expected)

  }

  test("Emit an event stream of account closures, what page user visited last") {

    val expected = List(
      "Alice" -> "pageB"
    )

    val result = MockedStreams()
      .topology(builder => exercise3.accountCancellationLastVisitedPage(builder))
      .input("user-events", strings, strings, userEvent)
      .input("account-state-changelog", strings, strings, accountChangeLog)
      .output("account-cancellation-last-visited-page", strings, strings, expected.length)

    result should be(expected)

  }


  /**
    *
    * Now we add a time aspect to the events
    *
    */
  // Account state as an account changelog stream of the backend account system
  val timestampedAccountChangeLog: immutable.Seq[(String, JsonNode)] = List(
    //(UserId,AccountState)
    ("Alice",   """{ "timestamp": 1000000 , "state": "free" }    """),
    ("Alice",   """{ "timestamp": 1001000 , "state": "paying" }  """),
    ("Alice",   """{ "timestamp": 1003000 , "state": "closed" }  """),
    ("Bob",     """{ "timestamp": 1001000 , "state": "free" }    """),
    ("Bob",     """{ "timestamp": 1002000 , "state": "paying" }  """),
    ("Bob",     """{ "timestamp": 1003000 , "state": "free" }    """),
    ("Mallory", """{ "timestamp": 1002000 , "state": "free" }    """),
    ("Mallory", """{ "timestamp": 1003000 , "state": "banned" }  """)
  ).map { case (k: String,v: String) => (k,mapper.readTree(v)) }

  // User event is a record stream of user actions
  val timestampedUserEvent: immutable.Seq[(String, JsonNode)] = List(
    //(UserId,PageID)
    ("Alice",   """{ "timestamp": 1000000 , "page": "pageA" } """),
    ("Alice",   """{ "timestamp": 1002000 , "page": "pageB" } """),
    ("Bob",     """{ "timestamp": 1001000 , "page": "pageA" } """),
    ("Bob",     """{ "timestamp": 1004000 , "page": "pageB" } """),
    ("Mallory", """{ "timestamp": 1002000 , "page": "pageA" } """),
    ("Mallory", """{ "timestamp": 1005000 , "page": "pageC" } """)
  ).map { case (k: String,v: String) => (k,mapper.readTree(v)) }

  test("Emit an event stream of account state changes paired with each pageviews happening within 1000 ms of the state change") {
    val expected = List(
      "Alice" ->    """{ "pageTimestamp" :1000000, "stateTimestamp": 1000000, "page": "pageA", "state": "free" }""",
      "Alice" ->    """{ "pageTimestamp" :1000000, "stateTimestamp": 1001000, "page": "pageA", "state": "paying" }""",
      "Alice" ->    """{ "pageTimestamp" :1002000, "stateTimestamp": 1001000, "page": "pageB", "state": "paying" }""",
      "Alice" ->    """{ "pageTimestamp" :1002000, "stateTimestamp": 1003000, "page": "pageB", "state": "closed" }""",
      "Bob" ->      """{ "pageTimestamp" :1001000, "stateTimestamp": 1001000, "page": "pageA", "state": "free" }""",
      "Bob" ->      """{ "pageTimestamp" :1001000, "stateTimestamp": 1002000, "page": "pageA", "state": "paying" }""",
      "Bob" ->      """{ "pageTimestamp" :1004000, "stateTimestamp": 1003000, "page": "pageB", "state": "free" }""",
      "Mallory" ->  """{ "pageTimestamp" :1002000, "stateTimestamp": 1002000, "page": "pageA", "state": "free" }""",
      "Mallory" ->  """{ "pageTimestamp" :1002000, "stateTimestamp": 1003000, "page": "pageA", "state": "banned" }"""
    ).map { case (k: String,v: String) => (k,mapper.readTree(v)) }

    val conf = new Properties
    conf.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[TimestampFieldExtractor].getName)

    val result = MockedStreams()
      .config(conf)
      .topology(builder => exercise3.pageViewsWithAccountStateChange(builder))
      .input("user-events", strings, json, timestampedUserEvent)
      .input("account-state-changelog", strings, json, timestampedAccountChangeLog)
      .output("account-state-coinciding-pageview", strings, json, expected.length)

    result should be (expected)

  }

  test("Emit an event stream of account state changes paired with a *list of* pageviews happening within 1000 ms of the state change") {

    // You need to perform a join followed by an aggregation

    val expected = List(
      "Alice-free-1000000"      -> """{ "state":"free",   "stateTimestamp":1000000, "pages":[{"id":"pageA","timestamp":1000000}]}""",
      "Alice-paying-1001000"    -> """{ "state":"paying", "stateTimestamp":1001000, "pages":[{"id":"pageA","timestamp":1000000}]}""",
      "Alice-paying-1001000"    -> """{ "state":"paying", "stateTimestamp":1001000, "pages":[{"id":"pageA","timestamp":1000000}, {"id":"pageB","timestamp":1002000}]}""",
      "Alice-closed-1003000"    -> """{ "state":"closed", "stateTimestamp":1003000, "pages":[{"id":"pageB","timestamp":1002000}]}""",
      "Bob-free-1001000"        -> """{ "state":"free",   "stateTimestamp":1001000, "pages":[{"id":"pageA","timestamp":1001000}]}""",
      "Bob-paying-1002000"      -> """{ "state":"paying", "stateTimestamp":1002000, "pages":[{"id":"pageA","timestamp":1001000}]}""",
      "Bob-free-1003000"        -> """{ "state":"free",   "stateTimestamp":1003000, "pages":[{"id":"pageB","timestamp":1004000}]}""",
      "Mallory-free-1002000"    -> """{ "state":"free",   "stateTimestamp":1002000, "pages":[{"id":"pageA","timestamp":1002000}]}""",
      "Mallory-banned-1003000"  -> """{ "state":"banned", "stateTimestamp":1003000, "pages":[{"id":"pageA","timestamp":1002000}]}"""
    ).map { case (k: String, v: String) => (k, mapper.readTree(v)) }

    val conf = new Properties
    conf.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[TimestampFieldExtractor].getName)
    conf.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0") // To prevent caching for consistent results
    conf.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0")
    conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[JsonNodeSerde])

    val result = MockedStreams()
      .config(conf)
      .topology(builder => exercise3.pageViewsWithListOfAccountStateChange(builder))
      .input("user-events", strings, json, timestampedUserEvent)
      .input("account-state-changelog", strings, json, timestampedAccountChangeLog)
      .output("account-state-coinciding-pageslist", strings, json, expected.length*100)

    result should be (expected)

  }

}

class TimestampFieldExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    val event = record.value().asInstanceOf[JsonNode]
    val timestamp = event.path("timestamp").longValue()
    timestamp
  }
}