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

public class BranchExample extends KafkaStreamsApp {

    public static void main(String[] args) {
        new BranchExample().start("branch-app");
    }

    public Topology createTopology(StreamsBuilder builder) {
        Serde<String> strings = Serdes.String();
        JsonNodeSerde json = new JsonNodeSerde();

        KStream<String, JsonNode> articles = builder.stream("Articles", Consumed.with(strings, json));

        KStream<String, JsonNode>[] articlesPerSite = articles.branch(
                (key, value) -> "bbc".equals(value.path("site").asText()),
                (key, value) -> "cnn".equals(value.path("site").asText()),
                (key, value) -> "foxnews".equals(value.path("site").asText()),
                (key, value) -> true // catch remaining events
        );

        articlesPerSite[0].to("BBC-Articles", Produced.with(strings, json));
        articlesPerSite[1].to("CNN-Articles", Produced.with(strings, json));
        articlesPerSite[2].to("FoxNews-Articles", Produced.with(strings, json));
        articlesPerSite[3].to("Other-Articles", Produced.with(strings, json));

        /* Alternatively, using new 'to' method:

        articles.to((key, value, recordContext) -> {
            switch (value.path("site").asText()) {
                case "bbc": return "BBC-Articles";
                case "cnn": return "BBC-Articles";
                case "foxnews": return "FoxNews-Articles";
                default: return "Other-Articles";
            }
        });
         */

        return builder.build();
    }
}
