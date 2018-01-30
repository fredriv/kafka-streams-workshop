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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

public class ArticleCountExample extends KafkaStreamsApp {

    public static void main(String[] args) {
        new BranchExample().start("article-count-app");
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = new Serdes.StringSerde();
        Serde<Long> longs = new Serdes.LongSerde();
        JsonNodeSerde json = new JsonNodeSerde();

        KStream<String, JsonNode> articles = builder.stream("Articles", Consumed.with(strings, json));

        KTable<String, Long> counts = articles
                .groupBy((key, value) -> value.path("site").asText(), Serialized.with(strings, json))
                .count();

        counts.toStream().to("ArticleCounts", Produced.with(strings, longs));

        return builder.build();
    }
}
