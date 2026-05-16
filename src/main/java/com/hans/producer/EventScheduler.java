package com.hans.producer;

import com.hans.event.OrderCreatedEvent;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@EnableScheduling
@Component
@ConditionalOnBooleanProperty(name = "enable.scheduled.producer")
public class EventScheduler {

  private final OrderCreatedEventProducer orderCreatedEventProducer;
  private final Random random = new Random();
  private final AtomicLong orderIdCounter = new AtomicLong(1);
  
  private static final String[] CUSTOMER_IDS = {"CUST001", "CUST002", "CUST003", "CUST004", "CUST005"};
  private static final String[] PRODUCT_IDS = {"PROD001", "PROD002", "PROD003", "PROD004", "PROD005"};

  public EventScheduler(OrderCreatedEventProducer orderCreatedEventProducer) {
    this.orderCreatedEventProducer = orderCreatedEventProducer;
  }

  @Scheduled(fixedRate = 1000)
  public void schedule() {
    OrderCreatedEvent randomOrder = createRandomOrder();
    orderCreatedEventProducer.produce(randomOrder);
  }
  
  private OrderCreatedEvent createRandomOrder() {
    Long orderId = orderIdCounter.getAndIncrement();
    String orderNumber = "ORD-" + System.currentTimeMillis() + "-" + random.nextInt(1000);
    String customerId = CUSTOMER_IDS[random.nextInt(CUSTOMER_IDS.length)];
    String productId = PRODUCT_IDS[random.nextInt(PRODUCT_IDS.length)];
    int quantity = random.nextInt(10) + 1; // 1-10 items
    double price = 10.0 + (random.nextDouble() * 990.0); // $10-$1000
    
    return new OrderCreatedEvent(orderId, orderNumber, customerId, productId, quantity, price);
  }
}
