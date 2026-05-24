package com.hans.event.consumer;

public record NotifyEvent(String notifyId, String customerId, String type) {}
