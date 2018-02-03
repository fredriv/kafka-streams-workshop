package kafkastreams.scalaexercises

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.IntSerde
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Materialized, Predicate, Produced, TimeWindows}
import org.apache.kafka.streams.{Consumed, StreamsBuilder}

class Exercise_2_Aggregations {

  private implicit val strings = Serdes.String
  private implicit val ints = Serdes.Integer
  private implicit val scalaInts = new IntSerde
  private implicit val longs = Serdes.Long
  private implicit val json = new JsonNodeSerde

  /**
    * Read the topic 'colors' and count the number of occurrences of
    * each color. Write the result to the topic 'color-counts'.
    */
  def countColorOccurrences(builder: StreamsBuilder): Unit = {
    builder.stream("colors", Consumed.`with`(strings, strings))
      .groupByS(value)
      .count
      .toStream
      .toS("color-counts")

    /* Alternatively

    builder.stream("colors", Consumed.`with`(strings, strings))
      .mapS((key, color) => (color, 1))
      .groupByKeyS
      .count
      .toStream
      .toS("color-counts")
     */
  }

  def value[K, V](key: K, value: V) = value

  /**
    * Read the topic 'hamlet' and count the number of occurrences
    * of each word in the text. Write the result to the topic
    * 'word-counts'.
    */
  def countWordOccurrences(builder: StreamsBuilder): Unit = {
    val lines = builder.stream("hamlet", Consumed.`with`(strings, strings))
    val words = lines ~>> toWords ~> (_.toLowerCase)

    words.groupByS(value)
      .count
      .toStream
      .toS("word-counts")

    /* Alternatively

    words.mapS((key, word) => (word, 1))
      .groupByKeyS
      .count
      .toStream
      .toS("word-counts")
    */
  }

  def toWords(line: String): Seq[String] = line.split(" ")

  /**
    * Read the topic 'click-events' and count the number of events
    * per site (field 'provider.@id'). Write the results to the topic
    * 'clicks-per-site'.
    */
  def clicksPerSite(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .groupByS((key, event) => event("provider")("@id").asText)
      .count
      .toStream
      .toS("clicks-per-site")

    /* Alternatively

    builder.stream("click-events", Consumed.`with`(strings, json))
      .mapS((key, event) => (event("provider")("@id").asText, 1))
      .groupByKeyS
      .count
      .toStream
      .toS("clicks-per-site")
     */
  }

  /**
    * Read the topic 'click-events' and compute the total value
    * (field 'object.price') of the classified ads per site. Write
    * the results to the topic 'total-classifieds-price-per-site'.
    *
    * Hint: Use method 'reduce' on the grouped stream.
    */
  def totalClassifiedsPricePerSite(builder: StreamsBuilder): Unit = {
    val priceBySite = (key: String, ad: JsonNode) => (
      ad("provider")("@id").asText,
      ad("object")("price").asInt
    )

    val clickEvents = builder.stream("click-events", Consumed.`with`(strings, json))
    val pricesBySite = clickEvents \ objectType("ClassifiedAd") ~> priceBySite

    pricesBySite
      .groupByKeyS
      .reduce((a, b) => a + b)
      .toStream
      .toS("total-classifieds-price-per-site")
  }

  def objectType(`type`: String) =
    (key: String, event: JsonNode) => event("object")("@type").asText == `type`

  /**
    * Read the topic 'pulse-events' and count the number of events
    * per site (field 'provider.@id') per hour. Write the results to
    * the state store 'clicks-per-hour'.
    */
  def clicksPerHour(builder: StreamsBuilder): Unit = {
    builder.stream("click-events", Consumed.`with`(strings, json))
      .selectKey[String]((key, event) => event("provider")("@id").asText)
      .groupByKeyS
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
      .count(Materialized.as("clicks-per-hour"))
  }

}
