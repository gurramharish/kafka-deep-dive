package com.hans.config;

import com.hans.event.OrderCreatedEvent;
import com.hans.producer.OrderSummarySerializer;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class AppConfig {

  @Bean
  public KafkaTemplate<String, OrderCreatedEvent> orderCreatedEventKafkaTemplate(KafkaProperties kafkaProperties) {
    Map<String, Object> configProps = kafkaProperties.buildProducerProperties();

    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSummarySerializer.class);

    DefaultKafkaProducerFactory<String, OrderCreatedEvent> factory = new DefaultKafkaProducerFactory<>(configProps);

    return new KafkaTemplate<>(factory);
  }

  @Bean
  public KafkaTemplate<String, Object> defaultKafkaTemplate(
      KafkaProperties kafkaProperties) {
    Map<String, Object> configProps = kafkaProperties.buildProducerProperties();


    DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(configProps);

    return new KafkaTemplate<>(factory);
  }

}
