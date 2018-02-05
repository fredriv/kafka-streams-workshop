package kafkastreams.scalaexamples

import kafkastreams.scalautils.KafkaStreamsDSL._
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}

object HelloKafkaStreams extends App {
  new HelloKafkaStreams().start("hello-kafka-streams")
}

class HelloKafkaStreams extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    implicit val strings = new StringSerde

    builder.stream("names", Consumed.`with`(strings, strings))
      .mapValues[String](name => s"Hello, $name!")
      .to("hello", Produced.`with`(strings, strings))

    /* Alternatively, using KafkaStreamsDSL

    builder.streamS[String, String]("names")
      .mapValuesS(name => s"Hello, $name!")
      .toS("hello")
     */

    builder.build()
  }
}
