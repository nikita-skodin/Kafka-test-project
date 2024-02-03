package com.skodin.producer.schedulers;

import com.skodin.producer.models.Event;
import com.skodin.producer.models.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

@Log4j2
@Component
@RequiredArgsConstructor
public class KafkaProducerScheduler {

    @Value("${application.kafka.topics.first}")
    private String firstTopicName;

    @Value("${application.kafka.topics.second}")
    private String secondTopicName;

    @Value("${application.kafka.topics.third}")
    private String thirdTopicName;

    @Value("${application.kafka.topics.fourth}")
    private String fourthTopicName;

    private final KafkaTemplate<String, Event> firstEventKafkaTemplate;
    private final KafkaTemplate<String, Event> secondEventKafkaTemplate;
    private final KafkaTemplate<String, Message> firstMessageKafkaTemplate;
    private int eventCounter = 0;
    private int messageCounter = 0;
    private final Random random = new Random();

    @Scheduled(fixedDelay = 1_000L)
    public void sendToTheFirstTopic() {
        Event event = getEvent(eventCounter++);
        sendEventToTopic(event, firstTopicName, firstEventKafkaTemplate, getRandomIdBetweenZeroAndOne());
        sendEventToTopic(event, secondTopicName, firstEventKafkaTemplate, null);
    }

    @Scheduled(fixedDelay = 1_000L)
    public void sendToTheThirdTopic() {
        Event event = getEvent(eventCounter++);
        sendEventToTopic(event, thirdTopicName, secondEventKafkaTemplate, null);
    }

    @Scheduled(fixedDelay = 1_000L)
    public void sendToTheFourthTopic() {
        log.info("SENDING TO {}", fourthTopicName);
        Message message = getMessage(messageCounter++);
        log.info("Message: {}", message);
        ProducerRecord<String, Message> record = new ProducerRecord<>(fourthTopicName, message);
        record.headers()
                .add("class", Message.class.getSimpleName().getBytes());
        firstMessageKafkaTemplate.send(record)
                .whenComplete((stringEventSendResult, throwable) -> {
                    if (throwable == null) {
                        log.info("SUCCESS!");
                    } else {
                        log.error("ERROR: {}", throwable, throwable);
                    }
                });
    }

    private void sendEventToTopic(Event event, String topicName, KafkaTemplate<String, Event> template, String id) {
        log.info("SENDING TO {}", topicName);
        log.info("EVENT: {}", event);

        ProducerRecord<String, Event> record;

        record = (id == null || id.isBlank()) ? new ProducerRecord<>(topicName, event) :
                new ProducerRecord<>(topicName, getRandomIdBetweenZeroAndOne(), event);

        record.headers()
                .add("class", Event.class.getSimpleName().getBytes());

        template.send(record)
                .whenComplete((stringEventSendResult, throwable) -> {
                    if (throwable == null) {
                        log.info("SUCCESS!");
                    } else {
                        log.error("ERROR: {}", throwable, throwable);
                    }
                });
    }

    private Event getEvent(int counter) {
        String id = String.valueOf(counter);
        return new Event(id, "name" + id, "message" + id);
    }

    private Message getMessage(int counter) {
        String id = String.valueOf(counter);
        return new Message("from", "to" + id, "message" + id);
    }

    private String getRandomIdBetweenZeroAndOne() {
        boolean b = random.nextBoolean();
        if (b) {
            return "1";
        } else {
            return "0";
        }
    }


}
