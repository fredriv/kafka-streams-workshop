package kafkastreams.scalaexamples

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.{Long, String}
import org.apache.kafka.streams.scala.StreamsBuilder

object ArticleCountExample extends App {
  new ArticleCountExample().start("article-count-app")
}

class ArticleCountExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val json = new JsonNodeSerde

    val articles = builder.stream[String, JsonNode]("Articles")

    val articlesPerSite = articles
      .groupBy(extractSite)
      .count()

    articlesPerSite.toStream.to("ArticleCounts")

    builder.build()
  }

  private def extractSite(key: String, article: JsonNode) = article("site").asText
}
