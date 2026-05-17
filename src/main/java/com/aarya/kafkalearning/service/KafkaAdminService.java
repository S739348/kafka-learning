package com.aarya.kafkalearning.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    public KafkaAdminService(AdminClient adminClient, KafkaListenerEndpointRegistry listenerRegistry) {
        this.adminClient = adminClient;
        this.listenerRegistry = listenerRegistry;
    }

    // ══════════════════════════════════════════════════════
    // 1. LIST ALL TOPICS
    // ══════════════════════════════════════════════════════

    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        log.info("Topics: {}", topics);
        return topics;
    }

    // ══════════════════════════════════════════════════════
    // 2. CREATE TOPIC
    // ══════════════════════════════════════════════════════

    public String createTopic(String topicName, int partitions, short replicationFactor)
            throws ExecutionException, InterruptedException {

        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

        adminClient.createTopics(Collections.singleton(newTopic)).all().get();

        log.info("Topic created -> name={} partitions={} rf={}",
                topicName, partitions, replicationFactor);

        return "Topic created: " + topicName;
    }

    // ══════════════════════════════════════════════════════
    // 3. DESCRIBE TOPIC — see partitions, replicas, ISR
    // ══════════════════════════════════════════════════════

    public Map<String, Object> describeTopic(String topicName)
            throws ExecutionException, InterruptedException {

        DescribeTopicsResult result = adminClient
                .describeTopics(Collections.singleton(topicName));

        TopicDescription description = result.topicNameValues()
                .get(topicName).get();

        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name",           description.name());
        info.put("partitionCount", description.partitions().size());

        // partition details
        List<Map<String, Object>> partitions = new ArrayList<>();
        for (TopicPartitionInfo p : description.partitions()) {
            Map<String, Object> pInfo = new LinkedHashMap<>();
            pInfo.put("partition", p.partition());
            pInfo.put("leader",    p.leader().id());
            pInfo.put("replicas",  p.replicas().stream()
                    .map(Node::id).toList());
            pInfo.put("isr",       p.isr().stream()
                    .map(Node::id).toList());
            partitions.add(pInfo);
        }
        info.put("partitions", partitions);

        log.info("Topic info -> {}", info);
        return info;
    }

    // ══════════════════════════════════════════════════════
    // 4. INCREASE PARTITION COUNT
    // (can only increase, never decrease)
    // ══════════════════════════════════════════════════════

    public String increasePartitions(String topicName, int newPartitionCount)
            throws ExecutionException, InterruptedException {

        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topicName, NewPartitions.increaseTo(newPartitionCount));

        adminClient.createPartitions(newPartitionsMap).all().get();

        log.info("Partitions increased -> topic={} newCount={}", topicName, newPartitionCount);
        return "Partitions increased to " + newPartitionCount + " for topic: " + topicName;
    }

    // ══════════════════════════════════════════════════════
    // 5. DELETE TOPIC
    // ══════════════════════════════════════════════════════

    public String deleteTopic(String topicName)
            throws ExecutionException, InterruptedException {

        adminClient.deleteTopics(Collections.singleton(topicName)).all().get();

        log.info("Topic deleted -> {}", topicName);
        return "Topic deleted: " + topicName;
    }

    // ══════════════════════════════════════════════════════
    // 6. LIST CONSUMER GROUPS
    // ══════════════════════════════════════════════════════

    public List<String> listConsumerGroups()
            throws ExecutionException, InterruptedException {

        List<String> groups = adminClient.listGroups()
                .all().get()
                .stream()
                .map(GroupListing::groupId)
                .toList();

        log.info("Consumer groups: {}", groups);
        return groups;
    }

    // ══════════════════════════════════════════════════════
    // 7. DESCRIBE CONSUMER GROUP — see lag, offset, partition
    // ══════════════════════════════════════════════════════

    public Map<String, Object> describeConsumerGroup(String groupId)
            throws ExecutionException, InterruptedException {

        // Get committed offsets for the group
        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();

        // Get end offsets (latest offset in each partition)
        List<TopicPartition> partitions = new ArrayList<>(offsets.keySet());

        // Need a temporary consumer to fetch end offsets
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> partitionDetails = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp      = entry.getKey();
                long committedOffset   = entry.getValue().offset();
                long endOffset         = endOffsets.getOrDefault(tp, 0L);
                long lag               = endOffset - committedOffset;

                Map<String, Object> pInfo = new LinkedHashMap<>();
                pInfo.put("topic",           tp.topic());
                pInfo.put("partition",        tp.partition());
                pInfo.put("committedOffset",  committedOffset);
                pInfo.put("endOffset",        endOffset);
                pInfo.put("lag",              lag);
                partitionDetails.add(pInfo);

                log.info("Group={} Topic={} Partition={} Lag={}",
                        groupId, tp.topic(), tp.partition(), lag);
            }
        }

        result.put("groupId",    groupId);
        result.put("partitions", partitionDetails);
        return result;
    }

    // ══════════════════════════════════════════════════════
    // 8. RESET OFFSET — TO EARLIEST (re-read everything)
    // Consumer group must be INACTIVE (stopped) first
    // ══════════════════════════════════════════════════════

    public String resetOffsetToEarliest(String groupId, String topicName)
            throws ExecutionException, InterruptedException {

        // Step 1 — Stop all @KafkaListener consumers
        log.info("Stopping all listeners...");
        listenerRegistry.stop();
        Thread.sleep(2000);  // wait for consumers to fully leave the group

        // Step 2 — Reset offset
        DescribeTopicsResult topicResult = adminClient
                .describeTopics(Collections.singleton(topicName));
        TopicDescription description = topicResult
                .topicNameValues().get(topicName).get();

        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
        for (TopicPartitionInfo p : description.partitions()) {
            TopicPartition tp = new TopicPartition(topicName, p.partition());
            resetOffsets.put(tp, new OffsetAndMetadata(0));
        }

        adminClient.alterConsumerGroupOffsets(groupId, resetOffsets).all().get();
        log.info("Offset reset to EARLIEST -> group={} topic={}", groupId, topicName);

        // Step 3 — Restart all @KafkaListener consumers
        log.info("Restarting all listeners...");
        listenerRegistry.start();

        return "Offset reset to earliest for group: " + groupId;
    }

    // ══════════════════════════════════════════════════════
    // RESET OFFSET TO SPECIFIC
    // ══════════════════════════════════════════════════════

    public String resetOffsetToSpecific(String groupId, String topicName,
                                        int partition, long offset)
            throws ExecutionException, InterruptedException {

        // Step 1 — Stop listeners
        log.info("Stopping all listeners...");
        listenerRegistry.stop();
        Thread.sleep(2000);

        // Step 2 — Reset specific partition offset
        TopicPartition tp = new TopicPartition(topicName, partition);
        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
        resetOffsets.put(tp, new OffsetAndMetadata(offset));

        adminClient.alterConsumerGroupOffsets(groupId, resetOffsets).all().get();
        log.info("Offset reset -> group={} topic={} partition={} offset={}",
                groupId, topicName, partition, offset);

        // Step 3 — Restart listeners
        log.info("Restarting all listeners...");
        listenerRegistry.start();

        return "Offset reset to " + offset + " for partition " + partition;
    }
    // ══════════════════════════════════════════════════════
    // 10. DELETE CONSUMER GROUP
    // ══════════════════════════════════════════════════════

    public String deleteConsumerGroup(String groupId)
            throws ExecutionException, InterruptedException {

        adminClient.deleteConsumerGroups(Collections.singleton(groupId)).all().get();

        log.info("Consumer group deleted -> {}", groupId);
        return "Consumer group deleted: " + groupId;
    }
}
