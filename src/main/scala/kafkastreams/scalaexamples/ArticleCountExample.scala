package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.scalautils.KafkaStreamsDSL._
import org.apache.kafka.streams.kstream.{Produced, Serialized}
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

object ArticleCountExample extends App {
  new ArticleCountExample().start("article-count-app")
}

class ArticleCountExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new Serdes.StringSerde
    implicit val longs = new Serdes.LongSerde
    implicit val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val articlesPerSite = articles
      .groupBy(extractSite, Serialized.`with`(strings, json))
      .count()

    articlesPerSite.toStream().to("ArticleCounts", Produced.`with`(strings, longs))

    /* Alternatively, using KafkaStreamsDSL:

    val articles = builder.streamS[String, JsonNode]("Articles")

    val articlesPerSite = articles
      .groupByS(extractSite)
      .count()

    articlesPerSite.toStream().toS("ArticleCounts")
     */

    return builder.build
  }

  private def extractSite(key: String, article: JsonNode) = article("site").asText
}
