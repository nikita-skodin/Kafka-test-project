package com.skodin.producer.config;

import com.skodin.producer.models.Event;
import com.skodin.producer.models.Message;
import com.skodin.producer.util.serializers.EventSerializer;
import com.skodin.producer.util.serializers.MessageSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Log4j2
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${application.kafka.topics.first}")
    private String firstTopicName;

    @Value("${application.kafka.topics.second}")
    private String secondTopicName;

    @Value("${application.kafka.topics.third}")
    private String thirdTopicName;

    @Value("${application.kafka.topics.fourth}")
    private String fourthTopicName;

    private final EventSerializer eventSerializer;
    private final MessageSerializer messageSerializer;

    @Bean
    public ProducerFactory<String, Event> firstEventProducerFactory
            (KafkaProperties kafkaProperties) {

        Map<String, Object> properties = kafkaProperties.buildProducerProperties(new DefaultSslBundleRegistry());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "first-producer");

        DefaultKafkaProducerFactory<String, Event> factory = new DefaultKafkaProducerFactory<>(properties);

        factory.setValueSerializer(eventSerializer);

        return factory;
    }

    @Bean
    public ProducerFactory<String, Event> secondEventProducerFactory
            (KafkaProperties kafkaProperties) {

        Map<String, Object> properties = kafkaProperties.buildProducerProperties(new DefaultSslBundleRegistry());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "second-producer");

        DefaultKafkaProducerFactory<String, Event> factory = new DefaultKafkaProducerFactory<>(properties);

        factory.setValueSerializer(eventSerializer);

        return factory;
    }

    @Bean
    public ProducerFactory<String, Message> firstMessageProducerFactory
            (KafkaProperties kafkaProperties) {

        Map<String, Object> properties = kafkaProperties.buildProducerProperties(new DefaultSslBundleRegistry());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "third-producer");

        DefaultKafkaProducerFactory<String, Message> factory = new DefaultKafkaProducerFactory<>(properties);

        factory.setValueSerializer(messageSerializer);

        return factory;
    }


    @Bean
    public KafkaTemplate<String, Event> firstEventKafkaTemplate
            (ProducerFactory<String, Event> firstEventProducerFactory) {
        return new KafkaTemplate<>(firstEventProducerFactory);
    }

    @Bean
    public KafkaTemplate<String, Event> secondEventKafkaTemplate
            (ProducerFactory<String, Event> secondEventProducerFactory) {
        return new KafkaTemplate<>(secondEventProducerFactory);
    }

    @Bean
    public KafkaTemplate<String, Message> thirdMessageKafkaTemplate
            (ProducerFactory<String, Message> firstMessageProducerFactory) {
        return new KafkaTemplate<>(firstMessageProducerFactory);
    }

    @Bean
    public NewTopic firstTopic() {
        return TopicBuilder.name(firstTopicName).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic secondTopic() {
        return TopicBuilder.name(secondTopicName).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic thirdTopic() {
        return TopicBuilder.name(thirdTopicName).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic fourthTopic() {
        return TopicBuilder.name(fourthTopicName).partitions(1).replicas(1).build();
    }

}
