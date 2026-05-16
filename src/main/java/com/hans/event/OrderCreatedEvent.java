package com.hans.event;

public record OrderCreatedEvent(Long id, String orderNumber, String customerId, String productId, int quantity, double price) {

}
