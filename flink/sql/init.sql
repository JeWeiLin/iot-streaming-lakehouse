-- 共用的初始化設定：所有 job 與互動式查詢都先載入這個檔案
--   互動式：docker compose run --rm sql-client bin/sql-client.sh -i /opt/flink/sql/init.sql
--   跑 job：docker compose run --rm sql-client bin/sql-client.sh -i /opt/flink/sql/init.sql -f /opt/flink/sql/<job>.sql

-- 查詢結果直接以表格印在終端機（預設的 table 模式會開全螢幕畫面）
SET 'sql-client.execution.result-mode' = 'tableau';

-- 沒有資料的 partition 超過 5 秒就不參與 watermark 計算，避免 window 卡住不輸出
SET 'table.exec.source.idle-timeout' = '5 s';

-- Kafka source：讀取 producer.py 送到 test-topic 的感測器資料
CREATE TABLE test_topic (
  device_id   STRING,
  location    STRING,
  seq         BIGINT,   -- 每台裝置的流水號，斷號 = 封包遺失
  temperature DOUBLE,
  humidity    DOUBLE,
  battery     DOUBLE,
  ts          BIGINT,   -- 量測時間 (毫秒)，不是送達時間
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  -- Kafka 收到訊息的時間；和 event_time 相減就是網路延遲
  arrival_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
  -- producer 會模擬 1~8 秒的網路延遲；容許 5 秒，遲到更久的資料不會算進 window
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'test-topic',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-test',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'   -- 舊格式的訊息解析失敗時略過
);
