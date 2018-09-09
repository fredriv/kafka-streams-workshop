package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.StreamsBuilder

object FilterTransformExample extends App {
  new FilterTransformExample().start("filter-transform-example")
}

class FilterTransformExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val json = new JsonNodeSerde

    val articles = builder.stream[String, JsonNode]("Articles")

    val bbcArticles = articles.filter((key, article) => article("site").asText == "bbc")
    val bbcTitles = bbcArticles.mapValues(article => article("title").asText)

    bbcArticles.to("BBC-Articles")
    bbcTitles.to("BBC-Titles")

    /* Alternatively, with KafkaStreamsDSL:

    articles \
      (article => article("site").asText == "bbc") ~>
      (article => article("title").asText) ~>
      "BBC-Titles"
     */

    builder.build()
  }
}
