package com.skodin.producer.util.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skodin.producer.models.Message;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MessageSerializer implements Serializer<Message> {

    private final ObjectMapper objectMapper;

    @Override
    public byte[] serialize(String topic, Message message) {
        try {
            return objectMapper.writeValueAsString(message).getBytes();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
