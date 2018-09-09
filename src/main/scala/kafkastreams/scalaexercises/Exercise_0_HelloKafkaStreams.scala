package kafkastreams.scalaexercises

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes.String
import org.apache.kafka.streams.scala.StreamsBuilder

class Exercise_0_HelloKafkaStreams {

  /**
    * Read the Kafka topic 'text' and send the contents directly to
    * the new topic 'pass-through'
    */
  def passEventsThroughDirectly(builder: StreamsBuilder): Unit = {
    val stream = builder.stream[String, String]("text")
    stream.to("pass-through")
  }

}
