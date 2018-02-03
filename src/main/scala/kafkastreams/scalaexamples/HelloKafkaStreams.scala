package kafkastreams.scalaexamples

import kafkastreams.scalautils.KafkaStreamsDSL._
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

class HelloKafkaStreams extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new StringSerde

    val names = builder.stream("names", Consumed.`with`(strings, strings))
    names ~> (name => s"Hello, $name!") ~> "hello"

    builder.build()
  }
}

object HelloKafkaStreams extends App {
  new HelloKafkaStreams().start("hello-kafka-streams")
}
