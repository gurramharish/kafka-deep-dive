package com.hans.event.producer;

public record PaymentEvent(String paymentId, String orderId, double amount, String status, String currency) {}
