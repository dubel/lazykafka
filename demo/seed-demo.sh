#!/usr/bin/env bash
# seed-demo.sh — populates demo Kafka with realistic topics & messages.
# Runs inside the apache/kafka container image.

set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka-demo:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

echo "==> Waiting for broker to accept connections..."
until "$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BROKER" >/dev/null 2>&1; do
  sleep 2
done
echo "==> Broker is reachable at $BROKER"

# ─── Topic definitions ───────────────────────────────────────────────
# Format: topic_name:partitions:replication_factor
TOPICS=(
  "orders-created:6:1"
  "orders-confirmed:3:1"
  "orders-shipped:3:1"
  "orders-delivered:3:1"
  "orders-cancelled:1:1"
  "users-registered:3:1"
  "users-profile-updated:3:1"
  "users-login-events:6:1"
  "users-preferences:1:1"
  "payments-initiated:3:1"
  "payments-completed:3:1"
  "payments-failed:1:1"
  "payments-refunded:1:1"
  "inventory-stock-updated:6:1"
  "inventory-low-stock-alerts:1:1"
  "notifications-email:3:1"
  "notifications-sms:3:1"
  "notifications-push:3:1"
  "analytics-page-views:6:1"
  "analytics-click-events:6:1"
  "analytics-conversions:3:1"
  "logs-application:6:1"
  "logs-audit-trail:3:1"
  "iot-sensor-readings:6:1"
  "iot-device-status:3:1"
  "contemporary-measurements-data:3:1"
)

echo "==> Creating ${#TOPICS[@]} topics..."
for entry in "${TOPICS[@]}"; do
  IFS=':' read -r name partitions rf <<< "$entry"
  "$KAFKA_BIN/kafka-topics.sh" \
    --bootstrap-server "$BROKER" \
    --create \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$rf" \
    --if-not-exists \
    2>/dev/null && echo "    ✓ $name ($partitions partitions)" || echo "    – $name (already exists)"
done

# ─── Helper: produce messages ────────────────────────────────────────
produce() {
  local topic="$1"
  shift
  # Each remaining arg is one message value
  printf '%s\n' "$@" | "$KAFKA_BIN/kafka-console-producer.sh" \
    --bootstrap-server "$BROKER" \
    --topic "$topic" \
    >/dev/null 2>&1
  echo "    ✓ $topic  (${#@} messages)"
}

echo ""
echo "==> Publishing seed messages..."

# --- orders ----------------------------------------------------------
produce "orders-created" \
  '{"order_id":"ORD-10001","customer_id":"C-501","items":[{"sku":"LAPTOP-PRO-15","qty":1,"price":1299.99}],"total":1299.99,"currency":"USD","created_at":"2026-06-15T09:12:33Z"}' \
  '{"order_id":"ORD-10002","customer_id":"C-302","items":[{"sku":"MECH-KB-RGB","qty":1,"price":149.99},{"sku":"MOUSE-ERGO","qty":1,"price":79.99}],"total":229.98,"currency":"USD","created_at":"2026-06-15T09:14:01Z"}' \
  '{"order_id":"ORD-10003","customer_id":"C-118","items":[{"sku":"MONITOR-4K-27","qty":2,"price":449.99}],"total":899.98,"currency":"EUR","created_at":"2026-06-15T09:21:17Z"}' \
  '{"order_id":"ORD-10004","customer_id":"C-742","items":[{"sku":"USB-HUB-7P","qty":3,"price":34.99}],"total":104.97,"currency":"USD","created_at":"2026-06-15T09:33:44Z"}' \
  '{"order_id":"ORD-10005","customer_id":"C-219","items":[{"sku":"HEADSET-NC","qty":1,"price":249.99},{"sku":"STAND-LAPTOP","qty":1,"price":59.99}],"total":309.98,"currency":"GBP","created_at":"2026-06-15T09:45:02Z"}'

produce "orders-confirmed" \
  '{"order_id":"ORD-10001","confirmed_at":"2026-06-15T09:13:01Z","estimated_delivery":"2026-06-18"}' \
  '{"order_id":"ORD-10002","confirmed_at":"2026-06-15T09:15:22Z","estimated_delivery":"2026-06-19"}' \
  '{"order_id":"ORD-10003","confirmed_at":"2026-06-15T09:22:05Z","estimated_delivery":"2026-06-20"}'

produce "orders-shipped" \
  '{"order_id":"ORD-10001","carrier":"FedEx","tracking":"FX-998877661","shipped_at":"2026-06-15T14:30:00Z"}' \
  '{"order_id":"ORD-10002","carrier":"UPS","tracking":"1Z999AA10123456784","shipped_at":"2026-06-15T16:12:00Z"}'

produce "orders-delivered" \
  '{"order_id":"ORD-10001","delivered_at":"2026-06-17T11:22:00Z","signed_by":"M. Johnson"}'

produce "orders-cancelled" \
  '{"order_id":"ORD-10004","reason":"customer_request","cancelled_at":"2026-06-15T10:01:33Z","refund_amount":104.97}'

# --- users -----------------------------------------------------------
produce "users-registered" \
  '{"user_id":"C-850","email":"alice.wonder@example.com","plan":"premium","registered_at":"2026-06-15T08:00:12Z"}' \
  '{"user_id":"C-851","email":"bob.builder@example.com","plan":"free","registered_at":"2026-06-15T08:45:30Z"}' \
  '{"user_id":"C-852","email":"carol.danvers@example.com","plan":"premium","registered_at":"2026-06-15T09:30:00Z"}'

produce "users-profile-updated" \
  '{"user_id":"C-501","field":"address","old_value":"123 Oak St","new_value":"456 Maple Ave","updated_at":"2026-06-15T10:11:00Z"}' \
  '{"user_id":"C-302","field":"phone","old_value":"+1-555-0100","new_value":"+1-555-0199","updated_at":"2026-06-15T11:22:33Z"}'

produce "users-login-events" \
  '{"user_id":"C-501","ip":"203.0.113.42","user_agent":"Mozilla/5.0","success":true,"at":"2026-06-15T09:10:00Z"}' \
  '{"user_id":"C-302","ip":"198.51.100.7","user_agent":"Chrome/126.0","success":true,"at":"2026-06-15T09:12:00Z"}' \
  '{"user_id":"C-999","ip":"192.0.2.1","user_agent":"curl/8.7.1","success":false,"at":"2026-06-15T09:15:00Z"}' \
  '{"user_id":"C-118","ip":"203.0.113.55","user_agent":"Safari/17.5","success":true,"at":"2026-06-15T09:20:00Z"}' \
  '{"user_id":"C-742","ip":"198.51.100.22","user_agent":"Firefox/127.0","success":true,"at":"2026-06-15T09:30:00Z"}' \
  '{"user_id":"C-501","ip":"203.0.113.42","user_agent":"Mozilla/5.0","success":true,"at":"2026-06-15T14:00:00Z"}'

produce "users-preferences" \
  '{"user_id":"C-501","theme":"dark","language":"en","notifications":{"email":true,"push":true,"sms":false}}'

# --- payments --------------------------------------------------------
produce "payments-initiated" \
  '{"payment_id":"PAY-3001","order_id":"ORD-10001","amount":1299.99,"method":"credit_card","initiated_at":"2026-06-15T09:12:40Z"}' \
  '{"payment_id":"PAY-3002","order_id":"ORD-10002","amount":229.98,"method":"paypal","initiated_at":"2026-06-15T09:14:10Z"}' \
  '{"payment_id":"PAY-3003","order_id":"ORD-10003","amount":899.98,"method":"credit_card","initiated_at":"2026-06-15T09:21:25Z"}' \
  '{"payment_id":"PAY-3004","order_id":"ORD-10005","amount":309.98,"method":"apple_pay","initiated_at":"2026-06-15T09:45:10Z"}'

produce "payments-completed" \
  '{"payment_id":"PAY-3001","status":"success","processor_ref":"ch_1abc","completed_at":"2026-06-15T09:12:55Z"}' \
  '{"payment_id":"PAY-3002","status":"success","processor_ref":"PP-xyz789","completed_at":"2026-06-15T09:14:30Z"}' \
  '{"payment_id":"PAY-3004","status":"success","processor_ref":"ap_9f8e","completed_at":"2026-06-15T09:45:25Z"}'

produce "payments-failed" \
  '{"payment_id":"PAY-3003","reason":"insufficient_funds","processor_ref":"ch_fail_2def","failed_at":"2026-06-15T09:21:40Z"}'

produce "payments-refunded" \
  '{"payment_id":"PAY-3005","order_id":"ORD-10004","amount":104.97,"reason":"order_cancelled","refunded_at":"2026-06-15T10:05:00Z"}'

# --- inventory -------------------------------------------------------
produce "inventory-stock-updated" \
  '{"sku":"LAPTOP-PRO-15","warehouse":"US-EAST","prev_qty":42,"new_qty":41,"reason":"sale","at":"2026-06-15T09:13:00Z"}' \
  '{"sku":"MECH-KB-RGB","warehouse":"US-EAST","prev_qty":156,"new_qty":155,"reason":"sale","at":"2026-06-15T09:15:00Z"}' \
  '{"sku":"MOUSE-ERGO","warehouse":"US-EAST","prev_qty":89,"new_qty":88,"reason":"sale","at":"2026-06-15T09:15:00Z"}' \
  '{"sku":"MONITOR-4K-27","warehouse":"EU-WEST","prev_qty":28,"new_qty":26,"reason":"sale","at":"2026-06-15T09:22:00Z"}' \
  '{"sku":"USB-HUB-7P","warehouse":"US-EAST","prev_qty":310,"new_qty":307,"reason":"sale","at":"2026-06-15T09:34:00Z"}' \
  '{"sku":"USB-HUB-7P","warehouse":"US-EAST","prev_qty":307,"new_qty":310,"reason":"restock_from_cancel","at":"2026-06-15T10:05:00Z"}'

produce "inventory-low-stock-alerts" \
  '{"sku":"MONITOR-4K-27","warehouse":"EU-WEST","current_qty":26,"threshold":30,"severity":"warning","at":"2026-06-15T09:22:05Z"}' \
  '{"sku":"HEADSET-NC","warehouse":"US-WEST","current_qty":8,"threshold":15,"severity":"critical","at":"2026-06-15T10:00:00Z"}'

# --- notifications ---------------------------------------------------
produce "notifications-email" \
  '{"notification_id":"N-7001","user_id":"C-501","template":"order_confirmation","subject":"Your order ORD-10001 is confirmed!","sent_at":"2026-06-15T09:13:05Z"}' \
  '{"notification_id":"N-7002","user_id":"C-302","template":"order_confirmation","subject":"Your order ORD-10002 is confirmed!","sent_at":"2026-06-15T09:15:30Z"}' \
  '{"notification_id":"N-7003","user_id":"C-501","template":"order_shipped","subject":"ORD-10001 has shipped!","sent_at":"2026-06-15T14:31:00Z"}'

produce "notifications-sms" \
  '{"notification_id":"N-8001","user_id":"C-501","phone":"+1-555-0142","body":"Your order ORD-10001 has shipped! Track: FX-998877661","sent_at":"2026-06-15T14:31:30Z"}' \
  '{"notification_id":"N-8002","user_id":"C-742","phone":"+1-555-0199","body":"Refund of $104.97 for ORD-10004 has been processed.","sent_at":"2026-06-15T10:06:00Z"}'

produce "notifications-push" \
  '{"notification_id":"N-9001","user_id":"C-501","title":"Order Delivered","body":"ORD-10001 was delivered. Signed by M. Johnson.","sent_at":"2026-06-17T11:23:00Z"}' \
  '{"notification_id":"N-9002","user_id":"C-850","title":"Welcome!","body":"Thanks for joining! Explore premium features.","sent_at":"2026-06-15T08:01:00Z"}'

# --- analytics -------------------------------------------------------
produce "analytics-page-views" \
  '{"session_id":"s-aa01","user_id":"C-501","page":"/products/laptop-pro-15","referrer":"google.com","at":"2026-06-15T09:10:30Z"}' \
  '{"session_id":"s-aa01","user_id":"C-501","page":"/cart","referrer":"/products/laptop-pro-15","at":"2026-06-15T09:11:45Z"}' \
  '{"session_id":"s-bb02","user_id":"C-302","page":"/products/mech-kb-rgb","referrer":"reddit.com","at":"2026-06-15T09:12:10Z"}' \
  '{"session_id":"s-cc03","user_id":null,"page":"/","referrer":"direct","at":"2026-06-15T09:20:00Z"}' \
  '{"session_id":"s-cc03","user_id":null,"page":"/products","referrer":"/","at":"2026-06-15T09:20:30Z"}' \
  '{"session_id":"s-dd04","user_id":"C-118","page":"/products/monitor-4k-27","referrer":"twitter.com","at":"2026-06-15T09:19:00Z"}'

produce "analytics-click-events" \
  '{"session_id":"s-aa01","element":"#add-to-cart","page":"/products/laptop-pro-15","at":"2026-06-15T09:11:20Z"}' \
  '{"session_id":"s-aa01","element":"#checkout-btn","page":"/cart","at":"2026-06-15T09:12:00Z"}' \
  '{"session_id":"s-bb02","element":"#add-to-cart","page":"/products/mech-kb-rgb","at":"2026-06-15T09:13:10Z"}' \
  '{"session_id":"s-dd04","element":"#add-to-cart","page":"/products/monitor-4k-27","at":"2026-06-15T09:20:45Z"}' \
  '{"session_id":"s-dd04","element":"#buy-now","page":"/products/monitor-4k-27","at":"2026-06-15T09:21:00Z"}'

produce "analytics-conversions" \
  '{"session_id":"s-aa01","user_id":"C-501","order_id":"ORD-10001","revenue":1299.99,"at":"2026-06-15T09:12:33Z"}' \
  '{"session_id":"s-bb02","user_id":"C-302","order_id":"ORD-10002","revenue":229.98,"at":"2026-06-15T09:14:01Z"}' \
  '{"session_id":"s-dd04","user_id":"C-118","order_id":"ORD-10003","revenue":899.98,"at":"2026-06-15T09:21:17Z"}'

# --- logs -------------------------------------------------------------
produce "logs-application" \
  '{"level":"INFO","service":"order-service","msg":"Order ORD-10001 created","trace_id":"t-aaa111","ts":"2026-06-15T09:12:33Z"}' \
  '{"level":"INFO","service":"payment-service","msg":"Payment PAY-3001 initiated","trace_id":"t-aaa111","ts":"2026-06-15T09:12:40Z"}' \
  '{"level":"INFO","service":"payment-service","msg":"Payment PAY-3001 completed","trace_id":"t-aaa111","ts":"2026-06-15T09:12:55Z"}' \
  '{"level":"WARN","service":"payment-service","msg":"Payment PAY-3003 failed: insufficient_funds","trace_id":"t-ccc333","ts":"2026-06-15T09:21:40Z"}' \
  '{"level":"ERROR","service":"inventory-service","msg":"Stock below threshold for HEADSET-NC in US-WEST","trace_id":"t-eee555","ts":"2026-06-15T10:00:01Z"}' \
  '{"level":"INFO","service":"notification-service","msg":"Email N-7001 sent to C-501","trace_id":"t-aaa111","ts":"2026-06-15T09:13:05Z"}' \
  '{"level":"DEBUG","service":"api-gateway","msg":"Rate limiter: 1450/2000 requests in window","ts":"2026-06-15T09:30:00Z"}' \
  '{"level":"INFO","service":"user-service","msg":"New registration C-850 (premium)","trace_id":"t-fff666","ts":"2026-06-15T08:00:12Z"}'

produce "logs-audit-trail" \
  '{"actor":"C-501","action":"update_address","resource":"user/C-501","old":"123 Oak St","new":"456 Maple Ave","ip":"203.0.113.42","at":"2026-06-15T10:11:00Z"}' \
  '{"actor":"system","action":"refund","resource":"payment/PAY-3005","amount":104.97,"reason":"order_cancelled","at":"2026-06-15T10:05:00Z"}' \
  '{"actor":"C-742","action":"cancel_order","resource":"order/ORD-10004","reason":"customer_request","at":"2026-06-15T10:01:33Z"}'

# --- IoT --------------------------------------------------------------
produce "iot-sensor-readings" \
  '{"device_id":"TEMP-W1-01","type":"temperature","value":22.4,"unit":"celsius","location":"warehouse-1","at":"2026-06-15T09:00:00Z"}' \
  '{"device_id":"TEMP-W1-01","type":"temperature","value":22.6,"unit":"celsius","location":"warehouse-1","at":"2026-06-15T09:05:00Z"}' \
  '{"device_id":"TEMP-W1-01","type":"temperature","value":23.1,"unit":"celsius","location":"warehouse-1","at":"2026-06-15T09:10:00Z"}' \
  '{"device_id":"HUM-W1-01","type":"humidity","value":45.2,"unit":"percent","location":"warehouse-1","at":"2026-06-15T09:00:00Z"}' \
  '{"device_id":"TEMP-W2-01","type":"temperature","value":18.9,"unit":"celsius","location":"warehouse-2","at":"2026-06-15T09:00:00Z"}' \
  '{"device_id":"TEMP-W2-01","type":"temperature","value":19.3,"unit":"celsius","location":"warehouse-2","at":"2026-06-15T09:05:00Z"}' \
  '{"device_id":"DOOR-W1-MAIN","type":"door_sensor","value":1,"unit":"open/closed","location":"warehouse-1","at":"2026-06-15T08:55:00Z"}' \
  '{"device_id":"DOOR-W1-MAIN","type":"door_sensor","value":0,"unit":"open/closed","location":"warehouse-1","at":"2026-06-15T09:02:00Z"}'

produce "iot-device-status" \
  '{"device_id":"TEMP-W1-01","status":"online","battery":87,"firmware":"2.4.1","last_seen":"2026-06-15T09:10:00Z"}' \
  '{"device_id":"HUM-W1-01","status":"online","battery":62,"firmware":"2.4.1","last_seen":"2026-06-15T09:00:00Z"}' \
  '{"device_id":"TEMP-W2-01","status":"online","battery":94,"firmware":"2.4.0","last_seen":"2026-06-15T09:05:00Z"}' \
  '{"device_id":"DOOR-W1-MAIN","status":"online","battery":100,"firmware":"1.2.0","last_seen":"2026-06-15T09:02:00Z"}'

echo ""
echo "==> Demo seeding complete!"
echo "    Topics: ${#TOPICS[@]}"

# ─── Consumer groups ──────────────────────────────────────────────────
# Create 3 consumer groups by consuming a few messages with --group.
# Each consumer reads a small subset of messages and then exits, leaving
# the group in "Empty" state with committed offsets.  Because the topics
# have more messages than we consume, the groups will show realistic lag
# in the cluster state view.

create_consumer_group() {
  local group="$1"
  local topic="$2"
  local max_messages="${3:-1}"

  "$KAFKA_BIN/kafka-console-consumer.sh" \
    --bootstrap-server "$BROKER" \
    --topic "$topic" \
    --group "$group" \
    --from-beginning \
    --max-messages "$max_messages" \
    --timeout-ms 10000 \
    >/dev/null 2>&1 || true
}

echo ""
echo "==> Creating demo consumer groups..."

# Group 1: order-processing-service — subscribed to multiple order topics
create_consumer_group "order-processing-service" "orders-created" 2
create_consumer_group "order-processing-service" "orders-confirmed" 1
create_consumer_group "order-processing-service" "payments-completed" 1
echo "    ✓ order-processing-service  (orders-created, orders-confirmed, payments-completed)"

# Group 2: analytics-pipeline — subscribed to analytics topics, partially caught up
create_consumer_group "analytics-pipeline" "analytics-page-views" 2
create_consumer_group "analytics-pipeline" "analytics-click-events" 1
create_consumer_group "analytics-pipeline" "analytics-conversions" 1
echo "    ✓ analytics-pipeline  (analytics-page-views, analytics-click-events, analytics-conversions)"

# Group 3: notification-dispatcher — subscribed to all notification channels
create_consumer_group "notification-dispatcher" "notifications-email" 1
create_consumer_group "notification-dispatcher" "notifications-sms" 1
create_consumer_group "notification-dispatcher" "notifications-push" 1
echo "    ✓ notification-dispatcher  (notifications-email, notifications-sms, notifications-push)"

echo ""
echo "==> Demo environment ready!"
echo "    Topics: ${#TOPICS[@]}"
echo "    Consumer groups: 3 (order-processing-service, analytics-pipeline, notification-dispatcher)"
echo "    Connect lazykafka to localhost:19092 to explore."

