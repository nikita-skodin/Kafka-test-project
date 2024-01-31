package com.skodin.producer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skodin.producer.models.Event;
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
import org.springframework.kafka.support.serializer.JsonSerializer;

@Log4j2
@Configuration
public class KafkaConfig {

    @Value("${application.kafka.topic}")
    private String topicName;

    @Bean
    public ProducerFactory<String, Event> producerFactory
            (KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        var properties = kafkaProperties.buildProducerProperties(new DefaultSslBundleRegistry());

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        var factory = new DefaultKafkaProducerFactory<String, Event>(properties);

        factory.setValueSerializer(new JsonSerializer<>(objectMapper));

        return factory;
    }

    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate
            (ProducerFactory<String, Event> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(topicName).partitions(1).replicas(1).build();
    }

}
