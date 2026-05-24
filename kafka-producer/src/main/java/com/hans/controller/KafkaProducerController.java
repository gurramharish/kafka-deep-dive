package com.hans.controller;

import com.hans.event.OrderCreatedEvent;
import com.hans.event.producer.NotifyEvent;
import com.hans.event.producer.PaymentEvent;
import com.hans.producer.OrderCreatedEventProducer;
import com.hans.producer.OtherEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/publish")
public class KafkaProducerController {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerController.class);

    private final OrderCreatedEventProducer orderCreatedEventProducer;
    private final OtherEventProducer otherEventProducer;

    public KafkaProducerController(OrderCreatedEventProducer orderCreatedEventProducer, OtherEventProducer otherEventProducer) {
        this.orderCreatedEventProducer = orderCreatedEventProducer;
        this.otherEventProducer = otherEventProducer;
    }

    @PostMapping("/order")
    public String publishOrder(@RequestBody OrderCreatedEvent orderCreatedEvent) {
        log.info("Received REST request to publish order: {}", orderCreatedEvent);
        orderCreatedEventProducer.produce(orderCreatedEvent);
        return "Order event published successfully!";
    }

    @PostMapping("/payment")
    public String publishPayment(@RequestBody PaymentEvent paymentEvent) {
        log.info("Received REST request to publish payment: {}", paymentEvent);
        otherEventProducer.publishPaymentEvent(paymentEvent);
        return "Payment event published successfully!";
    }

    @PostMapping("/notify")
    public String publishNotify(@RequestBody NotifyEvent notifyEvent) {
        log.info("Received REST request to publish notification: {}", notifyEvent);
        otherEventProducer.publishNotifyEvent(notifyEvent);
        return "Notification event published successfully!";
    }
}
