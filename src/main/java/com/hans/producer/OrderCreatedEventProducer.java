package com.hans.producer;

import com.hans.event.OrderCreatedEvent;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class OrderCreatedEventProducer {


  private static final Logger log = LoggerFactory.getLogger(OrderCreatedEventProducer.class);

  private final String topicName;

  private final ObjectMapper objectMapper;

  private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

  public OrderCreatedEventProducer(@Value("${topic.name}") String topicName, ObjectMapper objectMapper, @Autowired  @Qualifier("orderCreatedEventKafkaTemplate") KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
    this.topicName = topicName;
    this.objectMapper = objectMapper;
    this.kafkaTemplate = kafkaTemplate;
  }

  public void produce(OrderCreatedEvent orderCreatedEvent) {
    Message<OrderCreatedEvent> msg = MessageBuilder
        .withPayload(orderCreatedEvent)
        .setHeader(KafkaHeaders.TOPIC, topicName)
        .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
        .build();

    CompletableFuture<SendResult<String, OrderCreatedEvent>> send = kafkaTemplate.send(msg);

    try {
      SendResult<String, OrderCreatedEvent> sendResult = send.get();

      int partition = sendResult.getRecordMetadata().partition();
      long offset = sendResult.getRecordMetadata().offset();

      log.info("\uD83D\uDCE4 Produced a message successfully Partition: {}, Offset: {}: {}", partition, offset, objectMapper.writeValueAsString(orderCreatedEvent));
    } catch (ExecutionException | InterruptedException exception) {
      log.error(exception.getMessage());
    }
  }
}
