package kafkastreams.scalaexamples

import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.{Consumed, StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.Produced

class HelloKafkaStreams extends KafkaStreamsApp {
  def createTopology(builder: StreamsBuilder): Topology = {
    val strings = new StringSerde

    builder.stream("names", Consumed.`with`(strings, strings))
      .mapValues[String](name => s"Hello, $name!")
      .to("hello", Produced.`with`(strings, strings))

    builder.build()
  }
}

object HelloKafkaStreams {
  def main(args: Array[String]): Unit = {
    new HelloKafkaStreams().start("hello-kafka-streams")
  }
}
