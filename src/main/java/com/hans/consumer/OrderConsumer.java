package com.hans.consumer;


import com.hans.event.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBooleanProperty(name = "enable.record.consumer")
public class OrderConsumer {

  private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);


  @KafkaListener(topics = "order-events-topic", containerFactory = "orderConcurrentKafkaListenerContainerFactory")
  public void consume(Order order) {

    log.info("Consumed order is : {}", order.orderId());

  }

  @KafkaListener(topics = "order-events-topic", groupId = "order-consumer-record-group", containerFactory = "orderConcurrentKafkaListenerContainerFactory")
  public void consumeRecord(ConsumerRecord<String, Object> record) {
    log.info("Consumer headers: {}", record.headers());
    log.info("Consumed order deserialized is : {}", ((Order)record.value()).orderId());
  }

}
