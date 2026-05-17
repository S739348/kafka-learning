package com.aarya.kafkalearning.config;

import com.aarya.kafkalearning.consumer.OrderConsumer;
import com.aarya.kafkalearning.model.Order;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.topic.orders}")
    private String ordersTopic;

    @Value("${app.topic.orders-dlq}")
    private String ordersDlqTopic;

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);
    // ══════════════════════════════════════════════════════
    // TOPICS
    // ══════════════════════════════════════════════════════

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ordersTopic)
                .partitions(3).replicas(2).build();
    }

    @Bean
    public NewTopic ordersDlqTopic() {
        return TopicBuilder.name(ordersDlqTopic)
                .partitions(3).replicas(2).build();
    }

    // ══════════════════════════════════════════════════════
    // ADMIN CLIENT
    // ══════════════════════════════════════════════════════

    @Bean
    public AdminClient adminClient() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    // ══════════════════════════════════════════════════════
    // PRODUCER
    // ══════════════════════════════════════════════════════

    @Bean
    public ProducerFactory<String, Order> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Order> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ══════════════════════════════════════════════════════
    // DLQ — DeadLetterPublishingRecoverer
    // When deserialization fails -> sends bad message to DLQ
    // ══════════════════════════════════════════════════════

    // Provide a separate KafkaTemplate that writes raw bytes to the DLQ.
    // When deserialization fails, the original record value is a byte[]; using
    // a template with ByteArraySerializer prevents attempts to re-deserialize
    // the payload as an Order object.
    @Bean
    public ProducerFactory<String, byte[]> producerFactoryForDlq() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean("dlqKafkaTemplate")
    public KafkaTemplate<String, byte[]> kafkaTemplateForDlq() {
        return new KafkaTemplate<>(producerFactoryForDlq());
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            @Qualifier("dlqKafkaTemplate")
                    KafkaTemplate<String, byte[]> kafkaTemplate) {

        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // bad message from orders partition 0 -> orders-dlq partition 0
                (record, ex) -> {
                    log.error("Routing to DLQ -> topic={} partition={} offset={} reason={}",
                            record.topic(), record.partition(),
                            record.offset(), ex.getMessage());
                    return new TopicPartition(ordersDlqTopic, record.partition());
                }
        );
    }

    // ══════════════════════════════════════════════════════
    // ERROR HANDLER — uses DLQ recoverer
    // ══════════════════════════════════════════════════════

    @Bean
    public DefaultErrorHandler errorHandler(
            DeadLetterPublishingRecoverer recoverer) {
        return new DefaultErrorHandler(recoverer);
    }

    // ══════════════════════════════════════════════════════
    // CONSUMER
    // ══════════════════════════════════════════════════════

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,                 "order-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");

        // Wrap with ErrorHandlingDeserializer
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ErrorHandlingDeserializer.class);

        // Actual deserializers inside the wrapper
        config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS,
                StringDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                JacksonJsonDeserializer.class);

        // JacksonJson config
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES,   "*");
        config.put("spring.json.value.default.type",           Order.class.getName());
        config.put("spring.json.use.type.headers",             false);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    // ══════════════════════════════════════════════════════
    // LISTENER CONTAINER FACTORY
    // Wire errorHandler into the factory
    // ══════════════════════════════════════════════════════

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, Order> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Order> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler); // wire DLQ error handler

        return factory;
    }

    // Consumer factory for DLQ topic - reads raw bytes so DLQ consumer can inspect payload
    @Bean
    public ConsumerFactory<String, byte[]> consumerFactoryForDlq() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG,                 "dlq-handler-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean("dlqKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, byte[]>
    dlqKafkaListenerContainerFactory(ConsumerFactory<String, byte[]> consumerFactoryForDlq) {

        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactoryForDlq);
        // Do NOT wire the same error handler - DLQ consumer should not re-route failures back to DLQ
        return factory;
    }
}