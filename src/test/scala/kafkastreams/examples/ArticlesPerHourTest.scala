package kafkastreams.examples

import java.time.{Instant, ZonedDateTime}
import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import com.madewithtea.mockedstreams.MockedStreams
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.TimestampExtractor

import kafkastreams.javaexamples.ArticlesPerHourExample
//import kafkastreams.scalaexamples.ArticlesPerHourExample
import org.scalatest.{FlatSpec, Matchers}

class ArticlesPerHourTest extends FlatSpec with Matchers with ArticlesTestBase {

  "Articles per hour example" should "count articles per hour" in {
    val config = new Properties()
    config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[PublishedTimestampExtractor].getName)

    val mstreams = MockedStreams()
      .topology(new ArticlesPerHourExample().createTopology)
      .input("Articles", strings, json, articles)
      .stores(Seq("articles-per-hour"))
      .config(config)

    mstreams.windowStateTable("articles-per-hour", "bbc") shouldEqual Map(
      "2018-01-30T17:00:00Z" -> 1,
      "2018-01-30T21:00:00Z" -> 1,
      "2018-01-30T23:00:00Z" -> 1
    ).map(toTimestamps)

    mstreams.windowStateTable("articles-per-hour", "cnn") shouldEqual Map(
      "2018-01-30T11:00:00Z" -> 1,
      "2018-01-30T17:00:00Z" -> 1,
      "2018-01-30T21:00:00Z" -> 1
    ).map(toTimestamps)

    mstreams.windowStateTable("articles-per-hour", "foxnews") shouldEqual Map(
      "2018-01-30T15:00:00Z" -> 1,
      "2018-01-30T22:00:00Z" -> 1
    ).map(toTimestamps)
  }

  def toTimestamps(entry: (String, Int)) = Instant.from(ZonedDateTime.parse(entry._1)).toEpochMilli -> entry._2
}

class PublishedTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], previousTimestamp: Long): Long = {
    val event = record.value().asInstanceOf[JsonNode]
    val timestamp = event.path("published").textValue()
    Instant.from(ZonedDateTime.parse(timestamp)).toEpochMilli
  }
}
