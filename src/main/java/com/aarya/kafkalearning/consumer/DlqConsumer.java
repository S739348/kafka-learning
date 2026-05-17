package com.aarya.kafkalearning.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    @KafkaListener(
            topics  = "${app.topic.orders-dlq}",
            groupId = "dlq-handler-group",
            containerFactory = "dlqKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, byte[]> record) {
        // DLQ receives raw bytes — not Order object
        // because the original message was not valid Order JSON
        String rawValue = record.value() != null
                ? new String(record.value())
                : "null";

        log.warn("==================================================");
        log.warn("DLQ MESSAGE RECEIVED");
        log.warn("  Topic     : {}", record.topic());
        log.warn("  Partition : {}", record.partition());
        log.warn("  Offset    : {}", record.offset());
        log.warn("  Key       : {}", record.key());
        log.warn("  Raw Value : {}", rawValue);
        log.warn("==================================================");

        // Here you can:
        // 1. Save to database for manual review
        // 2. Send alert to engineering team
        // 3. Try to fix and reprocess
    }
}