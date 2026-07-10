CREATE TABLE trade_events (
  event_type STRING,
  event_time TIMESTAMP(3),
  shop_id STRING,
  order_id STRING,
  user_id STRING,
  status STRING,
  amount DECIMAL(12, 2),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'trade_events',
  'properties.bootstrap.servers' = 'centos01:9092,centos02:9092,centos03:9092',
  'properties.group.id' = 'flink-risk-online',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE risk_alerts (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  shop_id STRING,
  fail_count BIGINT,
  fail_amount DECIMAL(18, 2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'risk_alerts',
  'properties.bootstrap.servers' = 'centos01:9092,centos02:9092,centos03:9092',
  'format' = 'json'
);

INSERT INTO risk_alerts
SELECT
  window_start,
  window_end,
  shop_id,
  COUNT(*) AS fail_count,
  SUM(amount) AS fail_amount
FROM TABLE(
  TUMBLE(TABLE trade_events, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
)
WHERE event_type = 'payment_result'
  AND status = 'PAY_FAILED'
GROUP BY window_start, window_end, shop_id
HAVING COUNT(*) >= 3 OR SUM(amount) >= 10000;
