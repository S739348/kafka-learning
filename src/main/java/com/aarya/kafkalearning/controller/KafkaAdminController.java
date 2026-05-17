package com.aarya.kafkalearning.controller;


import com.aarya.kafkalearning.service.KafkaAdminService;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/kafka/admin")
public class KafkaAdminController {

    private final KafkaAdminService adminService;

    public KafkaAdminController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    // GET http://localhost:8080/kafka/admin/topics
    @GetMapping("/topics")
    public Set<String> listTopics()
            throws ExecutionException, InterruptedException {
        return adminService.listTopics();
    }

    // POST http://localhost:8080/kafka/admin/topics/create?name=orders&partitions=3&rf=2
    @PostMapping("/topics/create")
    public String createTopic(@RequestParam String name,
                              @RequestParam int partitions,
                              @RequestParam short rf)
            throws ExecutionException, InterruptedException {
        return adminService.createTopic(name, partitions, rf);
    }

    // GET http://localhost:8080/kafka/admin/topics/orders/describe
    @GetMapping("/topics/{topicName}/describe")
    public Map<String, Object> describeTopic(@PathVariable String topicName)
            throws ExecutionException, InterruptedException {
        return adminService.describeTopic(topicName);
    }

    // PUT http://localhost:8080/kafka/admin/topics/orders/partitions?count=5
    @PutMapping("/topics/{topicName}/partitions")
    public String increasePartitions(@PathVariable String topicName,
                                     @RequestParam int count)
            throws ExecutionException, InterruptedException {
        return adminService.increasePartitions(topicName, count);
    }

    // DELETE http://localhost:8080/kafka/admin/topics/orders
    @DeleteMapping("/topics/{topicName}")
    public String deleteTopic(@PathVariable String topicName)
            throws ExecutionException, InterruptedException {
        return adminService.deleteTopic(topicName);
    }

    // GET http://localhost:8080/kafka/admin/groups
    @GetMapping("/groups")
    public List<String> listGroups()
            throws ExecutionException, InterruptedException {
        return adminService.listConsumerGroups();
    }

    // GET http://localhost:8080/kafka/admin/groups/order-group/describe
    @GetMapping("/groups/{groupId}/describe")
    public Map<String, Object> describeGroup(@PathVariable String groupId)
            throws ExecutionException, InterruptedException {
        return adminService.describeConsumerGroup(groupId);
    }

    // PUT http://localhost:8080/kafka/admin/groups/order-group/reset/earliest?topic=orders
    @PutMapping("/groups/{groupId}/reset/earliest")
    public String resetEarliest(@PathVariable String groupId,
                                @RequestParam String topic)
            throws ExecutionException, InterruptedException {
        return adminService.resetOffsetToEarliest(groupId, topic);
    }

    // PUT http://localhost:8080/kafka/admin/groups/order-group/reset/specific?topic=orders&partition=0&offset=5
    @PutMapping("/groups/{groupId}/reset/specific")
    public String resetSpecific(@PathVariable String groupId,
                                @RequestParam String topic,
                                @RequestParam int partition,
                                @RequestParam long offset)
            throws ExecutionException, InterruptedException {
        return adminService.resetOffsetToSpecific(groupId, topic, partition, offset);
    }

    // DELETE http://localhost:8080/kafka/admin/groups/order-group
    @DeleteMapping("/groups/{groupId}")
    public String deleteGroup(@PathVariable String groupId)
            throws ExecutionException, InterruptedException {
        return adminService.deleteConsumerGroup(groupId);
    }
}