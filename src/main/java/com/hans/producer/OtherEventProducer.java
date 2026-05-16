package com.hans.producer;

import com.hans.event.producer.NotifyEvent;
import com.hans.event.producer.PaymentEvent;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class OtherEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OtherEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OtherEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentEvent(PaymentEvent event) {
        publish("payment-events", event);
    }

    public void publishNotifyEvent(NotifyEvent event) {
        publish("notify-events", event);
    }

    private void publish(String topic, Object event) {
        Message<Object> msg = MessageBuilder
            .withPayload(event)
            .setHeader(KafkaHeaders.TOPIC, topic)
            .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
            .build();

        CompletableFuture<SendResult<String, Object>> send = kafkaTemplate.send(msg);

        send.thenAccept(result -> {
            log.info("📤 Produced a message to topic '{}' successfully. Partition: {}, Offset: {}", 
                     topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        }).exceptionally(ex -> {
            log.error("Failed to produce message to topic '{}'", topic, ex);
            return null;
        });
    }
}
