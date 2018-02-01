package kafkastreams.javaexercises;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class Exercise_2_Aggregations {

    private static Serde<String> strings = Serdes.String();
    private static Serde<Integer> ints = Serdes.Integer();
    private static Serde<Long> longs = Serdes.Long();
    private static Serde<JsonNode> json = new JsonNodeSerde();

    /**
     * Read the topic 'colors' and count the number of occurrences of
     * each color. Write the result to the topic 'color-counts'.
     */
    public static void countColorOccurrences(StreamsBuilder builder) {

    }

    /**
     * Read the topic 'hamlet' and count the number of occurrences
     * of each word in the text. Write the result to the topic
     * 'word-counts'.
     */
    public static void countWordOccurrences(StreamsBuilder builder) {

    }

    /**
     * Read the topic 'click-events' and count the number of events
     * per site (field 'provider.@id'). Write the results to the topic
     * 'clicks-per-site'.
     */
    public static void clicksPerSite(StreamsBuilder builder) {

    }

    /**
     * Read the topic 'click-events' and compute the total value
     * (field 'object.price') of the classified ads per site. Write
     * the results to the topic 'total-classifieds-price-per-site'.
     *
     * Hint: Use method 'reduce' on the grouped stream.
     */
    public static void totalClassifiedsPricePerSite(StreamsBuilder builder) {

    }

    /**
     * Read the topic 'pulse-events' and count the number of events
     * per site (field 'provider.@id') per hour. Write the results to
     * the state store 'clicks-per-hour'.
     */
    public static void clicksPerHour(StreamsBuilder builder) {

    }

}
