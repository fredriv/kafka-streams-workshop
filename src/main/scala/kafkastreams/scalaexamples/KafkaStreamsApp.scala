package kafkastreams.scalaexamples

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

trait KafkaStreamsApp {

  def createTopology(builder: StreamsBuilder): Topology

  def start(applicationId: String): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")

    val builder = new StreamsBuilder
    val topology = createTopology(builder)

    val streams = new KafkaStreams(topology, config)
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() =>
      streams.close(10, TimeUnit.SECONDS)
    ))
  }
}
