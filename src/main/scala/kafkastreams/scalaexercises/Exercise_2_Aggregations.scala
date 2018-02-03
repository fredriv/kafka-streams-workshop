package kafkastreams.scalaexercises

import java.util
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Materialized, Predicate, Produced, Serialized, TimeWindows}
import org.apache.kafka.streams.{Consumed, KeyValue, StreamsBuilder}

class Exercise_2_Aggregations {

  private val strings = Serdes.String
  private val ints = Serdes.Integer
  private val longs = Serdes.Long
  private val json = new JsonNodeSerde

  /**
    * Read the topic 'colors' and count the number of occurrences of
    * each color. Write the result to the topic 'color-counts'.
    */
  def countColorOccurrences(builder: StreamsBuilder): Unit = {
    builder.stream("colors", Consumed.`with`(strings, strings))
      .groupBy((key: String, color: String) => color, Serialized.`with`(strings, strings))
//      .map[String, Integer]((key, color) => KeyValue.pair(color, 1))
//      .groupByKey(Serialized.`with`(strings, ints))
      .count
      .toStream
      .to("color-counts", Produced.`with`(strings, longs))
  }

  /**
    * Read the topic 'hamlet' and count the number of occurrences
    * of each word in the text. Write the result to the topic
    * 'word-counts'.
    */
  def countWordOccurrences(builder: StreamsBuilder): Unit = {
    builder.stream("hamlet", Consumed.`with`(strings, strings))
      .flatMapValues[String](line => util.Arrays.asList(line.split(" "): _*))
      .mapValues[String](_.toLowerCase)
      .groupBy((key: String, word: String) => word, Serialized.`with`(strings, strings))
//      .map[String, Integer]((key: String, word: String) => KeyValue.pair(word.toLowerCase, 1))
//      .groupByKey(Serialized.`with`(strings, ints))
      .count
      .toStream
      .to("word-counts", Produced.`with`(strings, longs))
  }

  /**
    * Read the topic 'click-events' and count the number of events
    * per site (field 'provider.@id'). Write the results to the topic
    * 'clicks-per-site'.
    */
  def clicksPerSite(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .selectKey[String]((key: String, json: JsonNode) => json.path("provider").path("@id").asText())
      .groupByKey(Serialized.`with`(strings, json))
//      .map[String, Integer]((key: String, json: JsonNode) => KeyValue.pair(json.path("provider").path("@id").asText, 1))
//      .groupByKey(Serialized.`with`(strings, ints))
      .count
      .toStream
      .to("clicks-per-site", Produced.`with`(strings, longs))
  }

  /**
    * Read the topic 'click-events' and compute the total value
    * (field 'object.price') of the classified ads per site. Write
    * the results to the topic 'total-classifieds-price-per-site'.
    *
    * Hint: Use method 'reduce' on the grouped stream.
    */
  def totalClassifiedsPricePerSite(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .filter(objectType("ClassifiedAd"))
      .map[String, Integer]((key: String, json: JsonNode) => KeyValue.pair(
        json.path("provider").path("@id").asText,
        json.path("object").path("price").asInt)
      )
      .groupByKey(Serialized.`with`(strings, ints))
      .reduce((a: Integer, b: Integer) => a + b)
      .toStream
      .to("total-classifieds-price-per-site", Produced.`with`(strings, ints))
  }

  def objectType(`type`: String): Predicate[String, JsonNode] =
    (key, json) => json.path("object").path("@type").asText == `type`

  /**
    * Read the topic 'pulse-events' and count the number of events
    * per site (field 'provider.@id') per hour. Write the results to
    * the state store 'clicks-per-hour'.
    */
  def clicksPerHour(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .selectKey[String]((key: String, json: JsonNode) => json.path("provider").path("@id").asText)
      .groupByKey(Serialized.`with`(strings, json))
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
      .count(Materialized.as("clicks-per-hour"))
  }

}
