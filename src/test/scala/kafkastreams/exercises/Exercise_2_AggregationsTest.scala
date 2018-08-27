package kafkastreams.exercises

import java.time.{Instant, ZonedDateTime}
import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import com.madewithtea.mockedstreams.MockedStreams
import kafkastreams.exercises.ClickEvents.clickEvents
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TimestampExtractor

class Exercise_2_AggregationsTest extends ExerciseBase {

  val strings = Serdes.String()
  val ints = Serdes.Integer()
  val longs = Serdes.Long()
  val json = new JsonNodeSerde

  test("Count number of occurrences of each color") {
    val colors = List("red", "blue", "pink", "red", "green", "pink", "yellow").map((null, _))
    val expected = List(
      "red" -> 1,
      "blue" -> 1,
      "pink" -> 1,
      "red" -> 2,
      "green" -> 1,
      "pink" -> 2,
      "yellow" -> 1
    )

    val result = MockedStreams()
      .topology(builder => exercise2.countColorOccurrences(toBuilder(builder)))
      .input("colors", strings, strings, colors)
      .output("color-counts", strings, longs, expected.size)

    result shouldBe expected
  }

  test("Count the number of occurrences of each word") {
    val hamlet = List(
      "To be or not to be",
      "that is the question"
    ).map((null, _))

    val expected = List(
      "to" -> 1,
      "be" -> 1,
      "or" -> 1,
      "not" -> 1,
      "to" -> 2,
      "be" -> 2,
      "that" -> 1,
      "is" -> 1,
      "the" -> 1,
      "question" -> 1
    )

    val result = MockedStreams()
      .topology(builder => exercise2.countWordOccurrences(toBuilder(builder)))
      .input("hamlet", strings, strings, hamlet)
      .output("word-counts", strings, longs, expected.length)

    result shouldBe expected
  }

  test("Count clicks per site") {
    val expected = List(
      "sdrn:schibsted:client:finn" -> 1,
      "sdrn:schibsted:client:aftenposten" -> 1,
      "sdrn:schibsted:client:blocket" -> 1,
      "sdrn:schibsted:client:leboncoin" -> 1,
      "sdrn:schibsted:client:aftonbladet" -> 1,
      "sdrn:schibsted:client:finn" -> 2,
      "sdrn:schibsted:client:finn" -> 3
    )

    val result = MockedStreams()
      .topology(builder => exercise2.clicksPerSite(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("clicks-per-site", strings, longs, expected.length)

    result shouldBe expected
  }

  test("Total price of classified ads per site") {
    val expected = List(
      "sdrn:schibsted:client:finn" -> 1500,
      "sdrn:schibsted:client:blocket" -> 23000,
      "sdrn:schibsted:client:leboncoin" -> 1,
      "sdrn:schibsted:client:finn" -> 1698,
      "sdrn:schibsted:client:finn" -> 2198
    )

    val result = MockedStreams()
      .topology(builder => exercise2.totalClassifiedsPricePerSite(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .output("total-classifieds-price-per-site", strings, ints, expected.length)

    result shouldBe expected
  }

  def toTimestamps(entry: (String, Int)) = Instant.from(ZonedDateTime.parse(entry._1)).toEpochMilli -> entry._2

  test("Clicks per hour") {
    val conf = new Properties
    conf.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[PublishedTimestampExtractor].getName)

    val stream = MockedStreams()
      .topology(builder => exercise2.clicksPerHour(toBuilder(builder)))
      .input("click-events", strings, json, clickEvents)
      .stores(Seq("clicks-per-hour"))
      .config(conf)

    stream.windowStateTable("clicks-per-hour", "sdrn:schibsted:client:finn") shouldEqual Map(
      "2017-11-26T09:00:00+00:00" -> 2,
      "2017-11-26T10:00:00+00:00" -> 1
    ).map(toTimestamps)

    stream.windowStateTable("clicks-per-hour", "sdrn:schibsted:client:aftenposten") shouldEqual Map(
      "2017-11-26T09:00:00+00:00" -> 1
    ).map(toTimestamps)

    stream.windowStateTable("clicks-per-hour", "sdrn:schibsted:client:aftonbladet") shouldEqual Map(
      "2017-11-26T09:00:00+00:00" -> 1
    ).map(toTimestamps)
  }

}

class PublishedTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    val event = record.value().asInstanceOf[JsonNode]
    val timestamp = event.path("published").textValue()
    Instant.from(ZonedDateTime.parse(timestamp)).toEpochMilli
  }
}
