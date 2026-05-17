package com.aarya.kafkalearning.controller;
import com.aarya.kafkalearning.model.Order;
import com.aarya.kafkalearning.producer.OrderProducer;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    // POST http://localhost:8080/orders/send
    // Body: {"id":"order1","item":"shoes","qty":2}
    @PostMapping("/send")
    public String send(@RequestBody Order order) {
        producer.sendOrder(order);
        return "Order sent: " + order.getId();
    }
}