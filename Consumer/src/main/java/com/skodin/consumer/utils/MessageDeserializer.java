package com.skodin.consumer.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skodin.consumer.models.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

@Log4j2
@Component
@RequiredArgsConstructor
public class MessageDeserializer implements Deserializer<Message> {

    private final ObjectMapper objectMapper;

    @Override
    public Message deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Message.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Message deserialize(String topic, Headers headers, ByteBuffer data) {

        for (var header : headers.headers("class")) {
            if (Arrays.equals(header.value(), "Message".getBytes())) {
                return Deserializer.super.deserialize(topic, headers, data);
            }
        }

        throw new RuntimeException("Invalid type");
    }
}