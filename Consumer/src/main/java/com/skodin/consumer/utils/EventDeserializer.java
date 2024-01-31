package com.skodin.consumer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skodin.consumer.models.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Log4j2
@Component
@RequiredArgsConstructor
public class EventDeserializer implements Deserializer<Event> {

    private final ObjectMapper objectMapper;

    @Override
    public Event deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Event.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}