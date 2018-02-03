package kafkastreams.scalaexamples

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Materialized, Serialized, TimeWindows}
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

class ArticlesPerHourExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new Serdes.StringSerde
    implicit val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val articlesPerHour = articles
      .groupBy(extractSite, Serialized.`with`(strings, json))
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
      .count(Materialized.as("articles-per-hour"))

    /* Alternatively, using KafkaStreamsDSL:

    val articlesPerHour = articles
      .groupByS(extractSite)
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
      .count(Materialized.as("articles-per-hour"))
     */

    return builder.build
  }

  private def extractSite(key: String, article: JsonNode) = article("site").asText
}


