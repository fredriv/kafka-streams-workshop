package kafkastreams.javaexamples;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class ArticlesPerHourExample extends KafkaStreamsApp {

    public static void main(String[] args) throws InterruptedException {
        KafkaStreams streams = new ArticlesPerHourExample().start("articles-per-hour-app");

        Thread.sleep(1000);

        ReadOnlyWindowStore<String, Long> articlesPerHour =
                streams.store("articles-per-hour", QueryableStoreTypes.windowStore());

        long from = 0L; // Jan 1st 1970
        long to = System.currentTimeMillis();
        WindowStoreIterator<Long> articles = articlesPerHour.fetch("bbc", from, to);

        articles.forEachRemaining(kv -> {
            Instant timestamp = Instant.ofEpochMilli(kv.key);
            System.out.println("BBC published " + kv.value +
                    " articles in hour " + timestamp);
        });
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = Serdes.String();
        JsonNodeSerde json = new JsonNodeSerde();

        KStream<String, JsonNode> articles = builder.stream("Articles", Consumed.with(strings, json));

        KGroupedStream<String, JsonNode> grouped = articles
                .groupBy((key, value) -> value.path("site").asText(),
                        Serialized.with(strings, json));

        KTable<Windowed<String>, Long> articlesPerHour = grouped
                .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
                .count(Materialized.as("articles-per-hour"));

        return builder.build();
    }
}
