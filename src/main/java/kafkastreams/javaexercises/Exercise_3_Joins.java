package kafkastreams.javaexercises;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;


public class Exercise_3_Joins {

    private Serde<String> strings = Serdes.String();
    private Serde<Integer> ints = Serdes.Integer();
    private Serde<Long> longs = Serdes.Long();
    private Serde<JsonNode> json = new JsonNodeSerde();

    public void accountStateJoin(StreamsBuilder builder) {

        KTable<String, String> accountStateKTable = builder.table("account-state-changelog", Consumed.with(strings, strings));
        KStream<String, String> userEventStream = builder.stream("user-events", Consumed.with(strings, strings));

        userEventStream
                .join(accountStateKTable, (value1, value2) -> value1 + "-" + value2)
                .to("user-events-with-accountstate", Produced.with(strings, strings));

    }

    public void accountCancellationLastVisitedPage(StreamsBuilder builder) {
        KStream<String, String> accountState = builder.stream("account-state-changelog", Consumed.with(strings, strings));
        KTable<String, String> userEventStream = builder.table("user-events", Consumed.with(strings, strings));

        accountState
                .filter((key, value) -> value.equals("closed"))
                .join(userEventStream, (value1, value2) -> value2)
                .to("account-cancellation-last-visited-page", Produced.with(strings, strings));


    }


    public void pageViewsWithAccountStateChange(StreamsBuilder builder) {
        KStream<String, JsonNode> accountStateStream = builder.stream("account-state-changelog", Consumed.with(strings, json));
        KStream<String, JsonNode> userEventStream = builder.stream("user-events", Consumed.with(strings, json));

        ObjectMapper mapper = new ObjectMapper();

        accountStateStream
                .join(userEventStream,
                        (JsonNode accountState, JsonNode pageView) -> {
                            ObjectNode node = mapper.createObjectNode();

                            node.set("pageTimestamp", pageView.path("timestamp"));
                            node.set("stateTimestamp", accountState.path("timestamp"));
                            node.set("page", pageView.path("page"));
                            node.set("state", accountState.path("state"));
                            return (JsonNode) node;
                        },

                        JoinWindows.of(1000),
                        Joined.with(strings, json, json))
                .to("account-state-coinciding-pageview", Produced.with(strings, json));


    }

    public void pageViewsWithListOfAccountStateChange(StreamsBuilder builder) {
        KStream<String, JsonNode> accountStateStream = builder.stream("account-state-changelog", Consumed.with(strings, json));
        KStream<String, JsonNode> userEventStream = builder.stream("user-events", Consumed.with(strings, json));

        ObjectMapper mapper = new ObjectMapper();


        Aggregator<String, JsonNode, JsonNode> aggregator = (key, value, aggregate) -> {
            ObjectNode page = mapper.createObjectNode();
            page.set("id",value.path("page"));
            page.set("timestamp",value.path("pageTimestamp"));
            ((ArrayNode)aggregate.path("pages")).add(page);
            ((ObjectNode)aggregate).set("state",value.path("state"));
            ((ObjectNode)aggregate).set("stateTimestamp",value.path("stateTimestamp"));
            return aggregate;
        };

        Initializer<JsonNode> initializer = () -> {
            ObjectNode result = mapper.createObjectNode();
            return result.set("pages", mapper.createArrayNode());
        };

        accountStateStream
                .join(userEventStream,
                        (JsonNode accountState, JsonNode pageView) -> {
                            ObjectNode node = mapper.createObjectNode();
                            node.set("pageTimestamp", pageView.path("timestamp"));
                            node.set("stateTimestamp", accountState.path("timestamp"));
                            node.set("page", pageView.path("page"));
                            node.set("state", accountState.path("state"));
                            return (JsonNode) node;
                        },
                        JoinWindows.of(1000),
                        Joined.with(strings, json, json))
                .groupBy((key, value) -> key+"-"+value.path("state").asText()+"-"+value.path("stateTimestamp").toString()) // Gotcha - what happens if you use .textValue here?
                .aggregate(initializer, aggregator)
                .toStream()
                .to("account-state-coinciding-pageslist", Produced.with(strings, json));

    }


}
