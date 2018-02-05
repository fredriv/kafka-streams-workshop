package kafkastreams.scalaexamples

import kafkastreams.scalautils.KafkaStreamsDSL._
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

object HelloKafkaStreams extends App {
  new HelloKafkaStreams().start("hello-kafka-streams")
}

class HelloKafkaStreams extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new StringSerde

    val names = builder.streamS[String, String]("names")
    names ~> (name => s"Hello, $name!") ~> "hello"

    builder.build()
  }
}
