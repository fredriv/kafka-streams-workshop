package kafkastreams.javaexamples;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class ArticleCountExample extends KafkaStreamsApp {

    public static void main(String[] args) {
        new ArticleCountExample().start("article-count-app");
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = Serdes.String();
        Serde<Long> longs = Serdes.Long();
        JsonNodeSerde json = new JsonNodeSerde();

        KStream<String, JsonNode> articles = builder.stream("Articles", Consumed.with(strings, json));

        KGroupedStream<String, JsonNode> grouped = articles
                .groupBy((key, value) -> value.path("site").asText(),
                        Serialized.with(strings, json));

        KTable<String, Long> counts = grouped.count();

        counts.toStream().to("ArticleCounts", Produced.with(strings, longs));

        return builder.build();
    }
}
