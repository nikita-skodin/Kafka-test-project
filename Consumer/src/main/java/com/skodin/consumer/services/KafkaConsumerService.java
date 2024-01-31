package com.skodin.consumer.services;

import com.skodin.consumer.models.Event;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Log4j2
@Service
public class KafkaConsumerService {

    @KafkaListener(
            topics = "${application.kafka.topic}",
            containerFactory = "eventListenerContainerFactory")
    public void listen(@Payload List<Event> batch) {
        for (Event b : batch) {
            log.info("An event has occurred: {}", b);
        }
    }

}
