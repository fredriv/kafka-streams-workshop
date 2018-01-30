package kafkastreams.javaexamples;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.concurrent.TimeUnit;

public class ArticlesPerHourExample extends KafkaStreamsApp {

    public static void main(String[] args) {
        new BranchExample().start("articles-per-hour-app");
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = new Serdes.StringSerde();
        JsonNodeSerde json = new JsonNodeSerde();

        KStream<String, JsonNode> articles = builder.stream("Articles", Consumed.with(strings, json));

        KTable<Windowed<String>, Long> articlesPerHour = articles
                .groupBy((key, value) -> value.path("site").asText(), Serialized.with(strings, json))
                .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
                .count(Materialized.as("articles-per-hour"));

        return builder.build();
    }
}
