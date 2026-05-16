package com.hans.consumer;


import com.hans.event.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

  private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);


  @KafkaListener(topics = "order-events-topic")
  public void consume(Order order) {

    log.info("Consumed order is : {}", order.orderId());

  }

}
