package kafkastreams.scalaexamples

import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

object FilterTransformExample extends App {
  new FilterTransformExample().start("filter-transform-example")
}

class FilterTransformExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new Serdes.StringSerde
    implicit val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val bbcArticles = articles.filter((key, article) => article("site").asText == "bbc")

    val bbcTitles = bbcArticles.mapValues[String](article => article("title").asText)

    bbcArticles.to("BBC-Articles", Produced.`with`(strings, json))
    bbcTitles.to("BBC-Titles", Produced.`with`(strings, strings))

    /* Alternatively, using KafkaStreamsDSL

    val bbcTitles = bbcArticles.mapValuesS(article => article("title").asText)

    bbcArticles.toS("BBC-Articles")
    bbcTitles.toS("BBC-Titles")
     */

    return builder.build
  }
}
