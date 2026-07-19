#!/bin/sh
set -eu
base=http://localhost:15300
suffix=$(date +%s)
fail() { docker compose logs --no-color flink-jobmanager flink-taskmanager flink-job-submit app; exit 1; }
parse_stats_offset() {
  case "$1" in
    *'
'*) return 1 ;;
    flink.window.stats:0:*) stats_offset=${1#flink.window.stats:0:} ;;
    *) return 1 ;;
  esac
  case "$stats_offset" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$stats_offset"
}
ready() { curl --fail --silent "$base/api/health" >/dev/null && [ "$(curl --fail --silent http://localhost:8081/jobs/overview | jq '[.jobs[] | select(.state == "RUNNING")] | length')" -eq 7 ]; }
if ! ready; then
  docker compose up -d --build || fail
  for attempt in $(seq 1 60); do
    ready && break
    [ "$attempt" -eq 60 ] && fail
    sleep 2
  done
fi
stats_end=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 --topic flink.window.stats --time -1) || fail
stats_offset=$(parse_stats_offset "$stats_end") || fail
top_start=0
[ "$stats_offset" -gt 100 ] && top_start=$((stats_offset - 100))
top_messages=$((stats_offset - top_start))
if [ "$top_messages" -gt 0 ]; then
  previous_stats=$(docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic flink.window.stats --partition 0 --offset "$top_start" --max-messages "$top_messages" --timeout-ms 20000) || fail
  top_count=$(printf '%s\n' "$previous_stats" | jq -er -s '[.[] | select(.metric == "top_product" and .scope == "window") | .value] | max // 0') || fail
else
  top_count=0
fi
case "$top_count" in
  *[!0-9]*|'') fail ;;
esac
cart_adds=$((top_count + 1))
seller=$(curl --fail --silent -H 'Content-Type: application/json' -d "{\"name\":\"seller-$suffix\",\"role\":\"seller\"}" "$base/api/session" | jq -r .token)
buyer=$(curl --fail --silent -H 'Content-Type: application/json' -d "{\"name\":\"buyer-$suffix\",\"role\":\"buyer\"}" "$base/api/session" | jq -r .token)
product=$(curl --fail --silent -H "Authorization: Bearer $seller" -H 'Content-Type: application/json' -d '{"name":"Widget","price":489000,"quantity":10}' "$base/api/seller/products")
product_id=$(printf '%s' "$product" | jq -r .id)
for add in $(seq 1 "$cart_adds"); do
  curl --fail --silent -H "Authorization: Bearer $buyer" -H 'Content-Type: application/json' -d "{\"product_id\":\"$product_id\",\"quantity\":1}" "$base/api/buyer/cart/items" >/dev/null
done
curl --fail --silent -H "Authorization: Bearer $buyer" -H 'Content-Type: application/json' -d "{\"items\":[{\"product_id\":\"$product_id\",\"quantity\":1}],\"shipping_address\":\"Jakarta\"}" "$base/api/buyer/cart/checkout" >/dev/null
output=$(docker compose exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic flink.window.stats --partition 0 --offset "$stats_offset" --max-messages 20 --timeout-ms 20000) || fail
printf '%s\n' "$output" | jq -e -s 'any(.[]; .metric == "tx_count" and .scope == "window" and .value >= 1)' >/dev/null || fail
printf '%s\n' "$output" | jq -e -s 'any(.[]; .metric == "revenue" and .scope == "daily" and .value >= 489000)' >/dev/null || fail
printf '%s\n' "$output" | jq -e -s --arg id "$product_id" 'any(.[]; .metric == "top_product" and .scope == "window" and .detail.product_id == $id and .detail.name == "Widget")' >/dev/null || fail
printf '%s\n' 'Phase 3 smoke test passed: seven jobs running and Level 2 metrics observed.'
