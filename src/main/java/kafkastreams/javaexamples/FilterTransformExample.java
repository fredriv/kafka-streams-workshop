package kafkastreams.javaexamples;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class FilterTransformExample extends KafkaStreamsApp {

    public static void main(String[] args) {
        new FilterTransformExample().start("filter-transform-app");
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = Serdes.String();
        JsonNodeSerde json = new JsonNodeSerde();

        KStream<String, JsonNode> articles = builder.stream("Articles", Consumed.with(strings, json));

        KStream<String, JsonNode> bbcArticles = articles
                .filter((key, value) -> "bbc".equals(value.path("site").asText()));

        KStream<String, String> bbcTitles = bbcArticles
                .mapValues(value -> value.path("title").asText());

        bbcArticles.to("BBC-Articles", Produced.with(strings, json));
        bbcTitles.to("BBC-Titles", Produced.with(strings, strings));

        return builder.build();
    }
}
