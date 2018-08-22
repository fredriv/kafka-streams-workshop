package kafkastreams.scalaexamples

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.JsonNode
import kafkastreams.scalautils.JacksonDSL._
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.{Materialized, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.QueryableStoreTypes

object ArticlesPerHourExample extends App {
  val streams = new ArticlesPerHourExample().start("articles-per-hour-app")

  Thread.sleep(1000)

  val clicksPerHour = streams.store("articles-per-hour", QueryableStoreTypes.windowStore[String, Long])

  val from = 0L // Jan 1st 1970
  val to = System.currentTimeMillis
  val articles = clicksPerHour.fetch("bbc", from, to)

  articles.forEachRemaining { kv =>
    val timestamp = Instant.ofEpochMilli(kv.key)
    println(s"BBC published ${kv.value} articles in hour $timestamp")
  }
}

class ArticlesPerHourExample extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val json = new JsonNodeSerde

    val articles = builder.stream[String, JsonNode]("Articles")

    val articlesPerHour = articles
      .groupBy(extractSite)
      .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))

    articlesPerHour.count()(Materialized.as("articles-per-hour"))

    builder.build()
  }

  private def extractSite(key: String, article: JsonNode) = article("site").asText
}


