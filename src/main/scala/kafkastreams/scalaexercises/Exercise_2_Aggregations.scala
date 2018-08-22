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
    * each color. Write the result to the topic 'color-counts'.
    */
  def countColorOccurrences(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the topic 'hamlet' and count the number of occurrences
    * of each word in the text. Write the result to the topic
    * 'word-counts'.
    */
  def countWordOccurrences(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the topic 'click-events' and count the number of events
    * per site (field 'provider.@id'). Write the results to the topic
    * 'clicks-per-site'.
    */
  def clicksPerSite(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the topic 'click-events' and compute the total value
    * (field 'object.price') of the classified ads per site. Write
    * the results to the topic 'total-classifieds-price-per-site'.
    *
    * Hint: Use method 'reduce' on the grouped stream.
    */
  def totalClassifiedsPricePerSite(builder: StreamsBuilder): Unit = {

  }

  /**
    * Read the topic 'pulse-events' and count the number of events
    * per site (field 'provider.@id') per hour. Write the results to
    * the state store 'clicks-per-hour'.
    */
  def clicksPerHour(builder: StreamsBuilder): Unit = {

  }

}
