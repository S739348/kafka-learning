package com.aarya.kafkalearning.consumer;

import com.aarya.kafkalearning.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(
            topics  = "${app.topic.orders}",
            groupId = "order-group"
    )
    // VALUE is now Order object — JsonDeserializer converts JSON back to Order
    public void consume(ConsumerRecord<String, Order> record) {

        // Get the Order object directly
        Order order = record.value();

        log.info("--------------------------------------------------");
        log.info("Order received");
        log.info("  Topic     : {}", record.topic());
        log.info("  Partition : {}", record.partition());
        log.info("  Offset    : {}", record.offset());
        log.info("  Key       : {}", record.key());
        log.info("  Order Id  : {}", order.getId());
        log.info("  Item      : {}", order.getItem());
        log.info("  Qty       : {}", order.getQty());
        log.info("--------------------------------------------------");

        // Now you can use the Order object directly
        // save to database, call service, etc
        processOrder(order);
    }

    private void processOrder(Order order) {
        log.info("Processing -> item={} qty={}", order.getItem(), order.getQty());
        // your business logic here
    }
}