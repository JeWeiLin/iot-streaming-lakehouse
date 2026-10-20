-- Kafka -> Flink -> Iceberg (MinIO)
-- 需搭配 init.sql（定義 test_topic source）
--   docker compose run --rm sql-client bin/sql-client.sh -i /opt/flink/sql/init.sql -f /opt/flink/sql/iceberg_job.sql
--
-- 寫入三張 Iceberg 表，資料檔 (Parquet) 會出現在 MinIO 的 warehouse/iot/ 底下
-- (舊版 producer 的 raw_events / temp_1min / alerts 表仍保留，不再寫入)

-- Iceberg sink 只有在 checkpoint 完成時才會 commit，沒開 checkpoint 資料永遠不會出現在表裡
SET 'execution.checkpointing.interval' = '30 s';

-- Iceberg catalog：metadata 由 REST catalog 管理，資料檔透過 S3 API 寫進 MinIO
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password',
  'client.region' = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lake.iot;

-- 原始讀數：依日期分區，MinIO 上會看到 dt=2026-10-20/ 這樣的資料夾
CREATE TABLE IF NOT EXISTS lake.iot.sensor_readings (
  device_id    STRING,
  location     STRING,
  seq          BIGINT,
  temperature  DOUBLE,
  humidity     DOUBLE,
  battery      DOUBLE,
  event_time   TIMESTAMP_LTZ(3),  -- 量測時間
  arrival_time TIMESTAMP_LTZ(3),  -- Kafka 收到的時間
  dt           STRING
) PARTITIONED BY (dt);

-- 每台裝置每 1 分鐘的統計
CREATE TABLE IF NOT EXISTS lake.iot.sensor_1min (
  device_id    STRING,
  location     STRING,
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  avg_temp     DOUBLE,
  min_temp     DOUBLE,
  max_temp     DOUBLE,
  avg_humidity DOUBLE,
  min_battery  DOUBLE,
  cnt          BIGINT   -- 每秒 1 筆時正常是 60，偏少代表封包遺失或遲到太久
);

-- 溫度告警
CREATE TABLE IF NOT EXISTS lake.iot.sensor_alerts (
  device_id   STRING,
  location    STRING,
  seq         BIGINT,
  temperature DOUBLE,
  level       STRING,
  event_time  TIMESTAMP_LTZ(3)
);

-- 三個 INSERT 包成一個 job，只佔 1 個 slot，也只讀一次 Kafka
EXECUTE STATEMENT SET
BEGIN

INSERT INTO lake.iot.sensor_readings
SELECT
  device_id, location, seq, temperature, humidity, battery,
  event_time, arrival_time, DATE_FORMAT(event_time, 'yyyy-MM-dd')
FROM test_topic
WHERE device_id IS NOT NULL;   -- 略過舊格式的訊息

INSERT INTO lake.iot.sensor_1min
SELECT
  device_id,
  location,
  window_start,
  window_end,
  ROUND(AVG(temperature), 2),
  MIN(temperature),
  MAX(temperature),
  ROUND(AVG(humidity), 1),
  MIN(battery),
  COUNT(*)
FROM TABLE(
  TUMBLE(TABLE test_topic, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE device_id IS NOT NULL
GROUP BY device_id, location, window_start, window_end;

-- 32°C 以上告警，35°C 以上為 CRITICAL
INSERT INTO lake.iot.sensor_alerts
SELECT
  device_id,
  location,
  seq,
  temperature,
  CASE WHEN temperature > 35 THEN 'CRITICAL' ELSE 'WARNING' END,
  event_time
FROM test_topic
WHERE temperature > 32;

END;
