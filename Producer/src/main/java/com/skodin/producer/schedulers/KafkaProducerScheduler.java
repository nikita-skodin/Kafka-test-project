package com.skodin.producer.schedulers;

import com.skodin.producer.models.Event;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@RequiredArgsConstructor
public class KafkaProducerScheduler {

    @Value("${application.kafka.topic}")
    private String topicName;

    private final KafkaTemplate<String, Event> template;
    private int counter = 0;

    @Scheduled(fixedDelay = 5_000L)
    public void send() {
        log.info("SENDING...");
        Event event = getEvent(counter);
        log.info("EVENT: {}", event);
        template.send(topicName, String.valueOf(counter++), event);
        log.info("SUCCESS!");
    }

    private Event getEvent(int counter) {
        String id = String.valueOf(counter);
        return new Event(id, "name" + id, "message" + id);
    }

}
