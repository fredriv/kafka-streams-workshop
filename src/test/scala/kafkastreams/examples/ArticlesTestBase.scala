package kafkastreams.examples

import com.fasterxml.jackson.databind.ObjectMapper
import kafkastreams.serdes.JsonNodeSerde
import org.apache.kafka.common.serialization.Serdes.{LongSerde, StringSerde}

import scala.io.Source

trait ArticlesTestBase {

  val mapper = new ObjectMapper()

  val strings = new StringSerde
  val longs = new LongSerde
  val json = new JsonNodeSerde

  val articles = Source.fromResource("articles.txt")
    .getLines
    .map(line => line.split("::").toList)
    .map { case List(key, value) => (key, mapper.readTree(value)) }
    .toList

}
