package kafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonNodeSerde implements Serde<JsonNode> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<JsonNode> serializer() {
        return new Serializer<JsonNode>() {
            @Override
            public byte[] serialize(String topic, JsonNode data) {
                try {
                    return mapper.writeValueAsBytes(data);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) { /* Do nothing */ }

            @Override
            public void close() { /* Do nothing */ }
        };
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return new Deserializer<JsonNode>() {
            @Override
            public JsonNode deserialize(String topic, byte[] data) {
                try {
                    return mapper.readTree(data);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) { /* Do nothing */ }

            @Override
            public void close() { /* Do nothing */ }
        };
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { /* Do nothing */ }

    @Override
    public void close() { /* Do nothing */ }

}
