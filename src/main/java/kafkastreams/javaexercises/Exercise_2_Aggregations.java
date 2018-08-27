package kafkastreams.javaexercises;

import com.fasterxml.jackson.databind.JsonNode;
import kafkastreams.serdes.JsonNodeSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static kafkastreams.javaexercises.Exercise_1_FilterAndTransform.objectType;

public class Exercise_2_Aggregations {

    private Serde<String> strings = Serdes.String();
    private Serde<Integer> ints = Serdes.Integer();
    private Serde<Long> longs = Serdes.Long();
    private Serde<JsonNode> json = new JsonNodeSerde();

    /**
     * Read the topic 'colors' and count the number of occurrences of
     * each color. Write the result to the topic 'color-counts'.
     */
    public void countColorOccurrences(StreamsBuilder builder) {
        builder.stream("colors", Consumed.with(strings, strings))
                .groupBy((key, color) -> color, Serialized.with(strings, strings))
                .count()
                .toStream()
                .to("color-counts", Produced.with(strings, longs));

        /* Alternatively

        builder.stream("colors", Consumed.with(strings, strings))
                .map((key, color) -> KeyValue.pair(color, 1))
                .groupByKey(Serialized.with(strings, ints))
                .count()
                .toStream()
                .to("color-counts", Produced.with(strings, longs));
         */
    }

    /**
     * Read the topic 'hamlet' and count the number of occurrences
     * of each word in the text. Write the result to the topic
     * 'word-counts'.
     */
    public void countWordOccurrences(StreamsBuilder builder) {
        builder.stream("hamlet", Consumed.with(strings, strings))
                .flatMapValues(line -> Arrays.asList(line.split(" ")))
                .mapValues(word -> word.toLowerCase())
                .groupBy((k, word) -> word, Serialized.with(strings, strings))
                .count()
                .toStream()
                .to("word-counts", Produced.with(strings, longs));

        /* Alternatively

        builder.stream("hamlet", Consumed.with(strings, strings))
                .flatMapValues(line -> Arrays.asList(line.split(" ")))
                .map((key, word) -> KeyValue.pair(word.toLowerCase(), 1))
                .groupByKey(Serialized.with(strings, ints))
                .count()
                .toStream()
                .to("word-counts", Produced.with(strings, longs));
         */
    }

    /**
     * Read the topic 'click-events' and count the number of events
     * per site (field 'provider.@id'). Write the results to the topic
     * 'clicks-per-site'.
     */
    public void clicksPerSite(StreamsBuilder builder) {
        builder.stream("click-events", Consumed.with(strings, json))
                .selectKey((key, json) -> json.path("provider").path("@id").asText())
                .groupByKey(Serialized.with(strings, json))
                .count()
                .toStream()
                .to("clicks-per-site", Produced.with(strings, longs));

        /* Alternatively

        builder.stream("click-events", Consumed.with(strings, json))
                .map((key, json) -> KeyValue.pair(json.path("provider").path("@id").asText(), 1))
                .groupByKey(Serialized.with(strings, ints))
                .count()
                .toStream()
                .to("clicks-per-site", Produced.with(strings, longs));
         */
    }

    /**
     * Read the topic 'click-events' and compute the total value
     * (field 'object.price') of the classified ads per site. Write
     * the results to the topic 'total-classifieds-price-per-site'.
     *
     * Hint: Use method 'reduce' on the grouped stream.
     */
    public void totalClassifiedsPricePerSite(StreamsBuilder builder) {
        builder.stream("click-events", Consumed.with(strings, json))
                .filter(objectType("ClassifiedAd"))
                .map((key, json) -> KeyValue.pair(
                        json.path("provider").path("@id").asText(),
                        json.path("object").path("price").asInt()
                ))
                .groupByKey(Serialized.with(strings, ints))
                .reduce((a, b) -> a + b)
                .toStream()
                .to("total-classifieds-price-per-site", Produced.with(strings, ints));
    }

    /**
     * Read the topic 'click-events' and count the number of events
     * per site (field 'provider.@id') per hour. Write the results to
     * the state store 'clicks-per-hour'.
     */
    public void clicksPerHour(StreamsBuilder builder) {
        builder.stream("click-events", Consumed.with(strings, json))
                .selectKey((key, json) -> json.path("provider").path("@id").asText())
                .groupByKey(Serialized.with(strings, json))
                .windowedBy(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
                .count(Materialized.as("clicks-per-hour"));
    }

}
