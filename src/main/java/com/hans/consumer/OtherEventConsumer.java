package com.hans.consumer;

import com.hans.event.consumer.NotifyEvent;
import com.hans.event.consumer.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBooleanProperty(name = "enable.record.consumer")
public class OtherEventConsumer {

  private static final Logger log = LoggerFactory.getLogger(OtherEventConsumer.class);

  @KafkaListener(topics = "payment-events", groupId = "payment-consumer-group", containerFactory = "paymentEventConcurrentKafkaListenerContainerFactory")
  public void consumePayment(ConsumerRecord<String, Object> record) {
    log.info("Consumer headers for payment: {}", record.headers());
    if (record.value() instanceof PaymentEvent payment) {
      log.info("Consumed payment event deserialized: paymentId={}, orderId={}, status={}", payment.paymentId(), payment.orderId(), payment.status());
    } else {
      log.info("Consumed payment event (unknown type): {}", record.value().getClass());
    }
  }

  @KafkaListener(topics = "notify-events", groupId = "notify-consumer-group")
  public void consumeNotify(ConsumerRecord<String, Object> record) {
    log.info("Consumer headers for notify: {}", record.headers());
    if (record.value() instanceof NotifyEvent notify) {
      log.info("Consumed notify event deserialized: notifyId={}, customerId={}, type={}", notify.notifyId(), notify.customerId(), notify.type());
    } else {
      log.info("Consumed notify event (unknown type): {}", record.value().getClass());
    }
  }
}
