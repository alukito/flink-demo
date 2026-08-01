#!/bin/sh
set -eu
until curl --fail --silent http://flink-jobmanager:8081/overview >/dev/null; do sleep 2; done
for metric in listings_count cart_adds_count tx_count confirmed_orders delivered_orders top_product revenue; do
  /opt/flink/bin/flink run -d -m flink-jobmanager:8081 -c com.flinkdemo.level2.MetricJob /opt/flink/usrlib/level2-jobs.jar --metric "$metric" --brokers kafka:9092
done
