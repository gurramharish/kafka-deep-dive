package com.hans.config;

import com.hans.event.Order;
import com.hans.event.consumer.PaymentEvent;
import java.util.Map;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

@Configuration
public class AppConfig {

  @Bean
  public ConsumerFactory<String, PaymentEvent> paymentEventConsumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = kafkaProperties.buildConsumerProperties();
    props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, PaymentEvent.class);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> paymentEventConcurrentKafkaListenerContainerFactory(ConsumerFactory<String, PaymentEvent> paymentEventConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
    listenerFactory.setConsumerFactory(paymentEventConsumerFactory);
    return listenerFactory;
  }

  @Bean
  public ConsumerFactory<String, Order> orderConsumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> props = kafkaProperties.buildConsumerProperties();
    props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, Order.class);
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, Order> orderConcurrentKafkaListenerContainerFactory(ConsumerFactory<String, Order> orderConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, Order> listenerFactory = new ConcurrentKafkaListenerContainerFactory<>();
    listenerFactory.setConsumerFactory(orderConsumerFactory);
    listenerFactory.setConcurrency(2);
    return listenerFactory;
  }

}
