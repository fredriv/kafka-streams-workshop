package kafkastreams.scalaexamples
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

class BranchExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    val strings = new Serdes.StringSerde
    val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val articlesPerSite = articles.branch(
      (key, value) => value.path("site").asText == "bbc",
      (key, value) => value.path("site").asText == "cnn",
      (key, value) => value.path("site").asText == "foxnews",
      (key, value) => true // catch remaining events
    ).toList

    val topics = List("BBC-Articles", "CNN-Articles", "FoxNews-Articles", "Other-Articles")

    articlesPerSite.zip(topics)
      .foreach { case (stream, topic) => stream.to(topic, Produced.`with`(strings, json)) }

    builder.build()
  }
}

object BranchExample extends App {
  new BranchExample().start("branch-app")
}