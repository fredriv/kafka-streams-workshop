package kafkastreams.javaexamples;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

abstract class KafkaStreamsApp {

    public abstract Topology createTopology(StreamsBuilder builder);

    public KafkaStreams start(String applicationId) {
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

        return streams;
    }
}
