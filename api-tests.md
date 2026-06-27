# API Publication Tests

Use the run buttons inside the markdown preview mode to test the REST endpoints in the `kafka-producer` application.

---

### 1. Publish Order Event
Publishes an `OrderCreatedEvent` to the `order-events-topic`.

```sh
curl -X POST http://localhost:8080/api/publish/order \
  -H "Content-Type: application/json" \
  -d '{
    "id": 101,
    "orderNumber": "ORD-INTEL-001",
    "customerId": "CUST-999",
    "productId": "PROD-ABC",
    "quantity": 5,
    "price": 749.99
  }'
```

---

### 2. Publish Payment Event
Publishes a `PaymentEvent` to the `payment-events` topic.

```sh
curl -X POST http://localhost:8080/api/publish/payment \
  -H "Content-Type: application/json" \
  -d '{
    "paymentId": "PAY-INTEL-002",
    "orderId": "ORD-INTEL-001",
    "amount": 749.99,
    "status": "SUCCESS",
    "currency": "USD"
  }'
```

---

### 3. Publish Notification Event
Publishes a `NotifyEvent` to the `notify-events` topic.

```sh
curl -X POST http://localhost:8080/api/publish/notify \
  -H "Content-Type: application/json" \
  -d '{
    "notifyId": "NOT-INTEL-003",
    "customerId": "CUST-999",
    "message": "Your order ORD-INTEL-001 has been received and payment is successful.",
    "type": "SMS",
    "priority": "HIGH"
  }'
```

### 1. Publish Order Event and Payment Event in Transaction
Publishes an `OrderCreatedEvent` to the `order-events-topic`.
Publishes and `PaymentEvent` to the `payment-events` topic.

```sh
curl -X POST http://localhost:8080/api/publish/order-txn-commit \
  -H "Content-Type: application/json" \
  -d '{
    "id": 112,
    "orderNumber": "ORD-INTEL-112",
    "customerId": "CUST-999",
    "productId": "PROD-ABC",
    "quantity": 5,
    "price": 749.99
  }'
```
