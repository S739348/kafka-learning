package com.aarya.kafkalearning.config;

import com.aarya.kafkalearning.model.Order;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.topic.orders}")
    private String ordersTopic;

    // ══════════════════════════════════════════════════════
    // 1. TOPIC — auto created on startup if not exists
    // ══════════════════════════════════════════════════════

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder
                .name(ordersTopic)
                .partitions(3)
                .replicas(2)
                .build();
    }

    // ══════════════════════════════════════════════════════
    // 2. ADMIN CLIENT — for admin operations
    //    (create, delete, describe topics, reset offsets)
    // ══════════════════════════════════════════════════════

    @Bean
    public AdminClient adminClient() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    // ══════════════════════════════════════════════════════
    // 3. PRODUCER FACTORY — how to create Kafka producers
    // ══════════════════════════════════════════════════════

    @Bean
    public ProducerFactory<String, Order> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    // ══════════════════════════════════════════════════════
    // 4. KAFKA TEMPLATE — tool you use to SEND messages
    // ══════════════════════════════════════════════════════

    @Bean
    public KafkaTemplate<String, Order> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ══════════════════════════════════════════════════════
    // 5. CONSUMER FACTORY — how to create Kafka consumers
    // ══════════════════════════════════════════════════════

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,                 "order-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);

        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES,   "*");
        config.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, Order.class.getName());

        return new DefaultKafkaConsumerFactory<>(config);
    }

    // ══════════════════════════════════════════════════════
    // 6. LISTENER CONTAINER FACTORY — powers @KafkaListener
    // ══════════════════════════════════════════════════════

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order>
    kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Order> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        return factory;
    }
}