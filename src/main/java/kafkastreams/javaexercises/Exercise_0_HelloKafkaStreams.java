package kafkastreams.javaexercises;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class Exercise_0_HelloKafkaStreams {

    private static Serde<String> strings = Serdes.String();

    /**
     * Read the Kafka topic 'text' and send the contents directly to
     * the new topic 'pass-through'
     */
    public void passEventsThroughDirectly(StreamsBuilder builder) {

    }

}
