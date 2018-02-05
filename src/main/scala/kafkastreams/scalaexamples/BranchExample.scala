package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

class BranchExample extends KafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new Serdes.StringSerde
    implicit val json = new JsonNodeSerde

    val articles = builder.streamS[String, JsonNode]("Articles")

    val articlesPerSite = articles.branch(
      (key, article) => article("site").asText == "bbc",
      (key, article) => article("site").asText == "cnn",
      (key, article) => article("site").asText == "foxnews",
      (key, article) => true // catch remaining events
    )

    val topics = List("BBC-Articles", "CNN-Articles", "FoxNews-Articles", "Other-Articles")

    /*
    for ((stream, topic) <- articlesPerSite.zip(topics))
      stream.toS(topic)
     */

    for ((stream, topic) <- articlesPerSite.zip(topics))
      stream ~> topic

    builder.build()
  }
}

object BranchExample extends App {
  new BranchExample().start("branch-app")
}