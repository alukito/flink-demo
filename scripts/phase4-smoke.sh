#!/bin/sh
# Exercise every Level 3 CEP pattern against the Compose demo. Requires docker, curl, and jq.
set -eu

base=http://localhost:15300
suffix=$(date +%s)
alerts_topic=flink.cep.alerts

fail() {
  echo "Phase 4 smoke test failed." >&2
  docker compose logs --no-color flink-jobmanager flink-taskmanager flink-job-submit app kafka || true
  exit 1
}

running_jobs() {
  curl --fail --silent http://localhost:8081/jobs/overview |
    jq '[.jobs[] | select(.state == "RUNNING")] | length'
}

ready() {
  curl --fail --silent "$base/api/health" >/dev/null && [ "$(running_jobs)" -eq 12 ]
}

topic_offset() {
  offset=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --bootstrap-server kafka:9092 --topic "$1" --time -1) || return 1
  case "$offset" in
    *'
'*) return 1 ;;
    "$1":0:*) offset=${offset#"$1":0:} ;;
    *) return 1 ;;
  esac
  case "$offset" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$offset"
}

api_token() {
  curl --fail --silent -H 'Content-Type: application/json' \
    -d "{\"name\":\"$1\",\"role\":\"$2\"}" "$base/api/session" | jq -er '.token'
}

add_item() {
  curl --fail --silent -H "Authorization: Bearer $1" -H 'Content-Type: application/json' \
    -d "{\"cart_id\":\"$2\",\"product_id\":\"$3\",\"quantity\":1}" \
    "$base/api/buyer/cart/items" >/dev/null
}

checkout() {
  curl --fail --silent -H "Authorization: Bearer $1" -H 'Content-Type: application/json' \
    -d "{\"cart_id\":\"$2\",\"items\":[{\"product_id\":\"$3\",\"quantity\":1}],\"shipping_address\":\"Jakarta\"}" \
    "$base/api/buyer/cart/checkout" | jq -er '.orders[0].order_id'
}

confirm() {
  curl --fail --silent -X POST -H "Authorization: Bearer $1" "$base/api/seller/orders/$2/confirm" >/dev/null
}

pick() {
  curl --fail --silent -X POST -H "Authorization: Bearer $1" "$base/api/shipper/jobs/$2/pick" >/dev/null
}

deliver() {
  curl --fail --silent -X POST -H "Authorization: Bearer $1" "$base/api/shipper/jobs/$2/deliver" >/dev/null
}

topic_messages_since() {
  docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 --topic "$1" --partition 0 --offset "$2" \
    --max-messages 50 --timeout-ms 10000 2>/dev/null || true
}

alerts_present() {
  printf '%s\n' "$1" | jq -se \
    --arg abandoned "abandoned_cart:$2" \
    --arg cart "$2" \
    --arg trend "$3" \
    --arg surge "$4" \
    --arg slow "slow_delivery:$5" \
    --arg slow_order "$5" \
    --arg completed "delivery_completed:$6" \
    --arg completed_order "$6" '
      . as $alerts |
      ([ $alerts[] | select(.alert_id == $abandoned and .pattern == "abandoned_cart" and .detail.cart_id == $cart) ] | length == 1) and
      ([ $alerts[] | select(.alert_id == $trend and .pattern == "trending_product" and (.detail.qualifying_count | type == "number") and .detail.qualifying_count == 3) ] | length == 1) and
      ([ $alerts[] | select(.alert_id == $surge and .pattern == "order_surge" and (.detail.checkout_count | type == "number") and .detail.checkout_count == 3) ] | length == 1) and
      ([ $alerts[] | select(.alert_id == $slow and .pattern == "slow_delivery" and .detail.order_id == $slow_order) ] | length == 1) and
      ([ $alerts[] | select(.alert_id == $completed and .pattern == "delivery_completed" and .detail.order_id == $completed_order and (.detail.elapsed_seconds | type == "number")) ] | length == 1)
    ' >/dev/null
}

if ! ready; then
  docker compose up -d --build || fail
  for attempt in $(seq 1 120); do
    ready && break
    [ "$attempt" -eq 120 ] && fail
    sleep 2
  done
fi

alerts_offset=$(topic_offset "$alerts_topic") || fail
cart_offset=$(topic_offset cart.item.added) || fail
checkout_offset=$(topic_offset cart.checkout) || fail
seller=$(api_token "phase4-seller-$suffix" seller) || fail
buyer_one=$(api_token "phase4-buyer-one-$suffix" buyer) || fail
buyer_two=$(api_token "phase4-buyer-two-$suffix" buyer) || fail
buyer_three=$(api_token "phase4-buyer-three-$suffix" buyer) || fail
shipper=$(api_token "phase4-shipper-$suffix" shipper) || fail

product=$(curl --fail --silent -H "Authorization: Bearer $seller" -H 'Content-Type: application/json' \
  -d '{"name":"Phase 4 trend product","price":489000,"quantity":20}' "$base/api/seller/products") || fail
product_id=$(printf '%s' "$product" | jq -er '.id') || fail
abandoned_product=$(curl --fail --silent -H "Authorization: Bearer $seller" -H 'Content-Type: application/json' \
  -d '{"name":"Phase 4 abandoned product","price":99000,"quantity":2}' "$base/api/seller/products") || fail
abandoned_product_id=$(printf '%s' "$abandoned_product" | jq -er '.id') || fail

abandoned_cart="phase4-abandoned-cart-$suffix"
add_item "$buyer_one" "$abandoned_cart" "$abandoned_product_id" || fail

# These three additions satisfy the three-buyer trending-product pattern.
add_item "$buyer_one" "phase4-trend-cart-one-$suffix" "$product_id" || fail
add_item "$buyer_two" "phase4-trend-cart-two-$suffix" "$product_id" || fail
add_item "$buyer_three" "phase4-trend-cart-three-$suffix" "$product_id" || fail

# These three checkouts satisfy the three-buyer order-surge pattern.
completed_order=$(checkout "$buyer_one" "phase4-checkout-one-$suffix" "$product_id") || fail
slow_order=$(checkout "$buyer_two" "phase4-checkout-two-$suffix" "$product_id") || fail
advance_order=$(checkout "$buyer_three" "phase4-checkout-three-$suffix" "$product_id") || fail

confirm "$seller" "$completed_order" || fail
confirm "$seller" "$slow_order" || fail
confirm "$seller" "$advance_order" || fail
pick "$shipper" "$completed_order" || fail
deliver "$shipper" "$completed_order" || fail
pick "$shipper" "$slow_order" || fail

# Event-time timeouts need later source events. Advance both windows after 65 seconds.
sleep 65
pick "$shipper" "$advance_order" || fail

# Advance the two-minute cart watermark only after the abandoned cart is due.
sleep 65
add_item "$buyer_one" "phase4-watermark-cart-$suffix" "$abandoned_product_id" || fail

# CEP constructs the trend/surge IDs from the first qualifying input timestamp.
cart_events=$(topic_messages_since cart.item.added "$cart_offset")
checkout_events=$(topic_messages_since cart.checkout "$checkout_offset")
trend_start=$(printf '%s\n' "$cart_events" | jq -ser --arg product "$product_id" \
  '[.[] | select(.event_type == "cart.item.added" and .payload.product_id == $product) | .timestamp] | first') || fail
surge_start=$(printf '%s\n' "$checkout_events" | jq -ser \
  '[.[] | select(.event_type == "cart.checkout") | .timestamp] | first') || fail
case "$trend_start:$surge_start" in
  null:*|*:null|:*) fail ;;
esac
trend_alert="trending_product:$product_id:$trend_start"
surge_alert="order_surge:$surge_start"

for attempt in $(seq 1 12); do
  alerts=$(topic_messages_since "$alerts_topic" "$alerts_offset")
  if alerts_present "$alerts" "$abandoned_cart" "$trend_alert" "$surge_alert" "$slow_order" "$completed_order"; then
    printf '%s\n' 'Phase 4 smoke test passed: twelve jobs running and five CEP alert patterns observed.'
    exit 0
  fi
  sleep 5
done

fail
