package com.hans.producer;

import com.hans.event.OrderCreatedEvent;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class OrderSummarySerializer implements Serializer<OrderCreatedEvent> {

  private final ObjectMapper objectMapper;

  public OrderSummarySerializer() {
    this.objectMapper = new ObjectMapper();
  }


  @Override
  public byte[] serialize(String topic, OrderCreatedEvent data) {
    if (data == null) {
      return null;
    }

    try {
      Map<String, Object> summary = new LinkedHashMap<>();
      summary.put("orderId", data.id());
      summary.put("productId", data.productId());
      return objectMapper.writeValueAsBytes(summary);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize order summary", e);
    }
  }
}
