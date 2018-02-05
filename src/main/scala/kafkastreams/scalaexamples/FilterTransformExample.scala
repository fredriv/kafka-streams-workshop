package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

object FilterTransformExample extends App {
  new FilterTransformExample().start("filter-transform-example")
}

class FilterTransformExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new Serdes.StringSerde
    implicit val json = new JsonNodeSerde

    val articles = builder.streamS[String, JsonNode]("Articles")

    val bbcArticles = articles \ (article => article("site").asText == "bbc")
    bbcArticles ~> "BBC-Articles"

    bbcArticles ~> (article => article("title").asText) ~> "BBC-Titles"

    return builder.build
  }
}
