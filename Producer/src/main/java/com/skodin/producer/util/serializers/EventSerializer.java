package com.skodin.producer.util.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skodin.producer.models.Event;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventSerializer implements Serializer<Event> {

    private final ObjectMapper objectMapper;

    @Override
    public byte[] serialize(String topic, Event event) {
        try {
            return objectMapper.writeValueAsString(event).getBytes();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
