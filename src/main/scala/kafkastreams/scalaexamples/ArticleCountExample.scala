package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Produced, Serialized}
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

class ArticleCountExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    val strings = new Serdes.StringSerde
    val longs = new Serdes.LongSerde
    val json = new JsonNodeSerde

    val articles = builder.stream("Articles", Consumed.`with`(strings, json))

    val articlesPerSite = articles
      .groupBy(extractSite, Serialized.`with`(strings, json))
      .count()

    articlesPerSite.toStream().to("ArticleCounts", Produced.`with`(strings, longs))

    return builder.build
  }

  private def extractSite(key: String, value: JsonNode) = value.path("site").asText()
}

object ArticleCountExample extends App {
  new ArticleCountExample().start("article-count-app")
}
