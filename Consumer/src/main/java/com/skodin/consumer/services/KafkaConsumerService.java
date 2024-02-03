package com.skodin.consumer.services;

import com.skodin.consumer.models.Event;
import com.skodin.consumer.models.Message;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Log4j2
@Service
public class KafkaConsumerService {

    @KafkaListener(
            topics = {"${application.kafka.topics.first}", "${application.kafka.topics.second}"},
            containerFactory = "eventListenerContainerFactory"
//            ,topicPartitions = @TopicPartition(topic = "${application.kafka.topic}", partitions = {"0"})
    )
    public void listen12(@Payload List<Event> batch) {
        log.info("from first and second");
        for (Event b : batch) {
            log.info("An event has occurred: {}", b);
        }
    }

    @KafkaListener(
            topics = {"${application.kafka.topics.third}"},
            containerFactory = "eventListenerContainerFactoryTwo"
    )
    public void listen3(@Payload List<Event> batch) {
        log.info("from third");
        for (Event b : batch) {
            log.info("An event has occurred: {}", b);
        }
    }

    @KafkaListener(
            topics = {"${application.kafka.topics.fourth}"},
            containerFactory = "messageListenerContainerFactory"
    )
    public void listen4(@Payload List<Message> batch) {
        log.info("from fourth");
        for (Message b : batch) {
            log.info("An event has occurred: {}", b);
        }
    }

}
