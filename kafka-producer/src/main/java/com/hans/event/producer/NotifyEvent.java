package com.hans.event.producer;

public record NotifyEvent(String notifyId, String customerId, String message, String type, String priority) {}
