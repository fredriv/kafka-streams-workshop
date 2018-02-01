package kafkastreams.javaexercises;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Collections;

public class Exercise_1_FilterAndTransform {

    private static Serde<String> strings = Serdes.String();
    private static Serde<Integer> ints = Serdes.Integer();
    private static Serde<JsonNode> json = new JsonNodeSerde();

    /**
     * Read the Kafka topic 'text' and send the contents directly to
     * the new topic 'pass-through'
     */
    public static void passEventsThroughDirectly(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("text", Consumed.with(strings, strings));
        stream.to("pass-through", Produced.with(strings, strings));
    }

    /**
     * Read the Kafka topic 'text', convert each line of text to the
     * length of that text and send it to the topic 'line-lengths'
     * as a stream of ints
     */
    public static void lineLengths(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'text', count the number of words in
     * each line and send that to the topic 'words-per-line' as a
     * stream of ints
     */
    public static void wordsPerLine(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'text', find the lines containing the
     * word 'conference' and send them to the topic
     * 'contains-conference'
     */
    public static void linesContainingData(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'text', split each line into words and
     * send them individually to the topic 'all-the-words'
     */
    public static void allTheWords(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'click-events' as json, get the object URL
     * (see 'ClickEvents' class in the 'testdata' package for details)
     * and send the URL as a string to the topic 'urls-visited'
     */
    public static void urlsVisited(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'click-events' as json, find the events
     * that are for objects of @type 'Article' (see 'ClickEvents'
     * class in the 'testdata' package for details) and send the
     * events unmodified to the topic 'articles' as json
     */
    public static void articles(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'click-events' as json, find the events
     * that are for objects of @type 'Article' and send the object
     * URLs to the topic 'article-urls' as strings
     */
    public static void articleVisits(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'click-events' as json, find the events
     * that are for objects of @type 'ClassifiedAd' and send the
     * object prices to the topic 'classified-ad-prices' as ints
     */
    public static void classifiedAdPrices(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'click-events' as json and convert the
     * classified ad events to a simplified format using JSTL:
     *
     *   {
     *     "title": "The object name",
     *     "price": 123 // the object price
     *   }
     *
     * Send the resulting events as json to the topic
     * 'simplified-classified-ads'
     */
    public static void simplifiedClassifiedAds(StreamsBuilder builder) {

    }

    /**
     * Read the Kafka topic 'click-events' as json and split it into
     * two new streams, one containing Article events (send to
     * 'articles' topic) and the other containing ClassifiedAd events
     * (send to 'classified-ads' topic).
     *
     * You can use the supplied 'objectType' method to create
     * predicates for different event types.
     *
     * Can you think of more than one way to solve it?
     */
    public static void splitArticlesAndAds(StreamsBuilder builder) {

    }

    public static Predicate<String, JsonNode> objectType(String type) {
        return (key, json) -> json.path("object").path("@type").asText().equals(type);
    }

    /**
     * Read the Kafka topic 'click-events' as strings and filter out
     * events that are invalid json. Send the correct events to the
     * topic 'json-events' as json.
     *
     * You can use the supplied 'tryParseJson' method to handle
     * parsing and error handling.
     */
    public static void filterOutInvalidJson(StreamsBuilder builder) {

    }

    private static ObjectMapper mapper = new ObjectMapper();

    public static Iterable<JsonNode> tryParseJson(String event) {
        try {
            return Collections.singletonList(mapper.readTree(event));
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

}
