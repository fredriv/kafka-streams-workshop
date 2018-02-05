package kafkastreams.scalaexamples

import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

class BranchExample extends KafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new Serdes.StringSerde
    implicit val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val articlesPerSite = articles.branch(
      (key, article) => article("site").asText == "bbc",
      (key, article) => article("site").asText == "cnn",
      (key, article) => article("site").asText == "foxnews",
      (key, article) => true // catch remaining events
    )

    val topics = List("BBC-Articles", "CNN-Articles", "FoxNews-Articles", "Other-Articles")

    for ((stream, topic) <- articlesPerSite.zip(topics))
      stream.to(topic, Produced.`with`(strings, json))

    /* Alternatively, using KafkaStreamsSDL

    val articles = builder.streamS[String, JsonNode]("Articles")

    ...

    for ((stream, topic) <- articlesPerSite.zip(topics))
      stream.toS(topic)
     */

    builder.build()
  }
}

object BranchExample extends App {
  new BranchExample().start("branch-app")
}