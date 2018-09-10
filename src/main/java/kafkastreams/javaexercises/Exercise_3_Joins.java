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

    /**
     * Join user events with account state based on user id, producing values of the form
     * <p>
     * pageId-accountState - e.g. 'pageA-closed'
     * <p>
     * For input and output topic names, see Exercise_3_JoinsTest
     */
    public void accountStateJoin(StreamsBuilder builder) {

    }

    /**
     * Join user events with account state, outputing the last visited page
     * when an account is cancelled/closed.
     */
    public void accountCancellationLastVisitedPage(StreamsBuilder builder) {

    }

    private ValueJoiner<JsonNode, JsonNode, JsonNode> createPageViewRelatedToAccountChangeJoiner(ObjectMapper mapper) {
        return (JsonNode accountState, JsonNode pageView) ->
                getPageViewRelatedToAccountChange(mapper, accountState, pageView);
    }

    private JsonNode getPageViewRelatedToAccountChange(ObjectMapper mapper, JsonNode accountState, JsonNode pageView) {
        ObjectNode node = mapper.createObjectNode();

        // TODO populate the JSON object with the expected fields. See Exercise_3_JoinsTest for details.

        return node;
    }

    /**
     * Emit an event stream of account state changes paired with
     * each pageview happening within 1000 ms of the state change.
     *
     * You can fill in and use the ValueJoiner helper methods above.
     */
    public void pageViewsWithAccountStateChange(StreamsBuilder builder) {

    }

    /**
     * Emit an event stream of account state changes paired with
     * a *list of* pageviews happening within 1000 ms of the state change.
     */
    public void pageViewsWithListOfAccountStateChange(StreamsBuilder builder) {

    }

}
