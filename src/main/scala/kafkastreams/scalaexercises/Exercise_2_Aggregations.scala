package kafkastreams.scalaexercises

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.kstream.{Materialized, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.{Integer, Long, String}
import org.apache.kafka.streams.scala.StreamsBuilder

class Exercise_2_Aggregations {

  private implicit val json = new JsonNodeSerde

  /**
    * Read the topic 'colors' and count the number of occurrences of
    * each color *key*. Write the result to the topic 'color-counts'.
    */
  def countColorKeyOccurrences(builder: StreamsBuilder): Unit = {
    builder.stream[String, String]("colors")
      .groupByKey
      .count
      .toStream
      .to("color-counts")
  }

  /**
    * Read the topic 'colors' and count the number of occurrences of
    * each color *value*. Write the result to the topic 'color-counts'.
    */
  def countColorValueOccurrences(builder: StreamsBuilder): Unit = {
    builder.stream[String, String]("colors")
      .groupBy((key: String, color: String) => color)
      .count
      .toStream
      .to("color-counts")

    /* Alternatively

    builder.stream("colors")
      .map((key, color) => (color, 1))
      .groupByKey
      .count
      .toStream
      .to("color-counts")
     */
  }

  /**
    * Read the topic 'hamlet' and count the number of occurrences
    * of each word in the text. Write the result to the topic
    * 'word-counts'.
    */
  def countWordOccurrences(builder: StreamsBuilder): Unit = {
    builder.stream[String, String]("hamlet")
      .flatMapValues(line => line.split(" "))
      .mapValues(_.toLowerCase)
      .groupBy((key, word) => word)
      .count
      .toStream
      .to("word-counts")

    /* Alternatively

    builder.stream("hamlet")
      .flatMapValues(line => line.split(" "))
      .map((key, word) => (word.toLowerCase, 1))
      .groupByKey
      .count
      .toStream
      .to("word-counts")
     */
  }

  /**
    * Read the topic 'click-events' and count the number of events
    * per site (field 'provider.@id'). Write the results to the topic
    * 'clicks-per-site'.
    */
  def clicksPerSite(builder: StreamsBuilder): Unit = {
    builder.stream[String, JsonNode]("click-events")
      .selectKey((key, json) => json.path("provider").path("@id").asText())
      .groupByKey
      .count
      .toStream
      .to("clicks-per-site")

    /* Alternatively

    builder.stream("click-events")
      .map((key, json) => (json.path("provider").path("@id").asText, 1))
      .groupByKey
      .count
      .toStream
      .to("clicks-per-site")
     */
  }

  /**
    * Read the topic 'prices' and compute the total price per site (key).
    * Write the results to the topic 'total-price-per-site'.
    *
    * Hint: Use method 'reduce' on the grouped stream.
    */
  def totalPricePerSite(builder: StreamsBuilder): Unit = {
    builder.stream[String, Int]("prices")
      .groupByKey
      .reduce((a, b) => a + b)
      .toStream
      .to("total-price-per-site")
  }

  /**
    * Read the topic 'click-events' and compute the total value
    * (field 'object.price') of the classified ads per site. Write
    * the results to the topic 'total-classifieds-price-per-site'.
    */
  def totalClassifiedsPricePerSite(builder: StreamsBuilder): Unit = {
    builder.stream[String, JsonNode]("click-events")
      .filter(objectType("ClassifiedAd"))
      .map((key, json) => (
        json.path("provider").path("@id").asText,
        json.path("object").path("price").asInt)
      )
      .groupByKey
      .reduce((a, b) => a + b)
      .toStream
      .to("total-classifieds-price-per-site")
  }

  def objectType(`type`: String) =
    (key: String, json: JsonNode) => json.path("object").path("@type").asText == `type`

  /**
    * Read the topic 'pulse-events' and count the number of events
    * per site (field 'provider.@id') per hour. Write the results to
    * the state store 'clicks-per-hour'.
    */
  def clicksPerHour(builder: StreamsBuilder): Unit = {
    builder.stream[String, JsonNode]("click-events")
      .selectKey((key, json) => json.path("provider").path("@id").asText)
      .groupByKey
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
      .count()(Materialized.as("clicks-per-hour"))
  }

}
