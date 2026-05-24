package com.hans.event.consumer;

public record PaymentEvent(String paymentId, String orderId, String status) {}
