package com.skodin.consumer.config;

import com.skodin.consumer.models.Event;
import com.skodin.consumer.models.Message;
import com.skodin.consumer.utils.EventDeserializer;
import com.skodin.consumer.utils.MessageDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
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

    private final EventDeserializer eventDeserializer;
    private final MessageDeserializer messageDeserializer;

    @Bean
    public ConsumerFactory<String, Event> eventConsumerFactory
            (KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, eventDeserializer, "event-listener");
    }

    @Bean
    public ConsumerFactory<String, Message> messageConsumerFactory
            (KafkaProperties kafkaProperties) {
        return createConsumerFactory(kafkaProperties, messageDeserializer, "message_listener");
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Event>> eventListenerContainerFactory(
            ConsumerFactory<String, Event> eventConsumerFactory) {
        return createListenerContainerFactory(eventConsumerFactory, 2, "kafka-task1");
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Event>> eventListenerContainerFactoryTwo(
            ConsumerFactory<String, Event> eventConsumerFactory) {
        return createListenerContainerFactory(eventConsumerFactory, 1, "kafka-task2");
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Message>> messageListenerContainerFactory(
            ConsumerFactory<String, Message> messageConsumerFactory) {
        return createListenerContainerFactory(messageConsumerFactory, 1, "kafka-task3");
    }


    private <V> ConsumerFactory<String, V> createConsumerFactory(KafkaProperties kafkaProperties, Deserializer<V> valueDeserializer, String groupId) {
        Map<String, Object> properties = kafkaProperties.buildConsumerProperties(new DefaultSslBundleRegistry());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 3_000);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        DefaultKafkaConsumerFactory<String, V> factory = new DefaultKafkaConsumerFactory<>(properties);
        factory.setValueDeserializer(valueDeserializer);

        return factory;
    }

    private <K, V> KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<K, V>> createListenerContainerFactory(
            ConsumerFactory<K, V> consumerFactory, int concurrency, String executorName) {
        var factory = new ConcurrentKafkaListenerContainerFactory<K, V>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setPollTimeout(1_000);

        ExecutorService pool = Executors.newFixedThreadPool(concurrency, task -> new Thread(task, executorName));
        ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(pool);
        factory.getContainerProperties().setListenerTaskExecutor(executor);

        return factory;
    }


}
