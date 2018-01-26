package kafkastreams.javaexamples;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

abstract class KafkaStreamsApp {

    public abstract Topology createTopology(StreamsBuilder builder);

    public void start(String applicationId) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = createTopology(builder);

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                streams.close(10, TimeUnit.SECONDS)
        ));
    }
}
