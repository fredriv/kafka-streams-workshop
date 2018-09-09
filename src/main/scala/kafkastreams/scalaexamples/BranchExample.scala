package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.StreamsBuilder

class BranchExample extends KafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val json = new JsonNodeSerde

    val articles = builder.stream[String, JsonNode]("Articles")

    val articlesPerSite = articles.branch(
      (key, article) => article("site").asText == "bbc",
      (key, article) => article("site").asText == "cnn",
      (key, article) => article("site").asText == "foxnews",
      (key, article) => true // catch remaining events
    )

    val topics = List("BBC-Articles", "CNN-Articles", "FoxNews-Articles", "Other-Articles")

    for ((stream, topic) <- articlesPerSite.zip(topics))
      stream.to(topic)

    /* Alternatively, using new 'to' method:

    articles.to((key, article, recordContext) =>
      article("site").asText match {
        case "bbc" => "BBC-Articles"
        case "cnn" => "CNN-Articles"
        case "foxnews" => "FoxNews-Articles"
        case _ => "Other-Articles"
      }
    )
     */

    builder.build()
  }
}

object BranchExample extends App {
  new BranchExample().start("branch-app")
}