package com.aarya.kafkalearning.producer;

import com.aarya.kafkalearning.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    @Value("${app.topic.orders}")
    private String ordersTopic;

    // KafkaTemplate<KEY, VALUE>
    // KEY   = String  (order id)
    // VALUE = Order   (the actual object — serialized to JSON automatically)
    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(Order order) {

        log.info("Sending order -> {}", order);

        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send(ordersTopic, order.getId(), order);
        //                               ^key           ^value
        //                               order id       Order object

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent successfully");
                log.info("  Topic     : {}", result.getRecordMetadata().topic());
                log.info("  Partition : {}", result.getRecordMetadata().partition());
                log.info("  Offset    : {}", result.getRecordMetadata().offset());
                log.info("  Key       : {}", order.getId());
                log.info("  Value     : {}", order);
            } else {
                log.error("Failed to send order: {}", ex.getMessage());
            }
        });
    }
}