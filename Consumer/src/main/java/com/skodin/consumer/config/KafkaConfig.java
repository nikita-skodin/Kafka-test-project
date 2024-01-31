package com.skodin.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skodin.consumer.models.Event;
import com.skodin.consumer.utils.EventDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    @Value("${application.kafka.topic}")
    private String topicName;

    private final EventDeserializer eventDeserializer;

    @Bean
    public ConsumerFactory<String, Event> consumerFactory
            (KafkaProperties kafkaProperties, ObjectMapper objectMapper) {

        Map<String, Object> properties = kafkaProperties.buildConsumerProperties(new DefaultSslBundleRegistry());

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3_000);

        DefaultKafkaConsumerFactory<String, Event> factory = new DefaultKafkaConsumerFactory<>(properties);

        factory.setValueDeserializer(eventDeserializer);

        return factory;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Event>> listenerContainerFactory
            (ConsumerFactory<String, Event> consumerFactory) {

        int concurrency = 1;
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Event>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setPollTimeout(1_000);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency, task -> new Thread(task, "kafka-task"));

        ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(pool);

        factory.getContainerProperties().setListenerTaskExecutor(executor);

        return factory;
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name(topicName).partitions(1).replicas(1).build();
    }


}
