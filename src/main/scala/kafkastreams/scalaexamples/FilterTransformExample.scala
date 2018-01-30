package kafkastreams.scalaexamples

import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.Produced

class FilterTransformExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    val strings = new Serdes.StringSerde
    val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val bbcArticles = articles.filter((key, value) => "bbc" == value.path("site").asText)

    val bbcTitles = bbcArticles.mapValues[String](value => value.path("title").asText)

    bbcArticles.to("BBC-Articles", Produced.`with`(strings, json))
    bbcTitles.to("BBC-Titles", Produced.`with`(strings, strings))

    return builder.build
  }
}

object FilterTransformExample extends App {
  new FilterTransformExample().start("filter-transform-example")
}
