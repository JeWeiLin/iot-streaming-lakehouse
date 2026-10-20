FROM flink:1.20.1-scala_2.12-java17

# Flink 官方 image 不含 Kafka connector，需另外放進 lib/
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/

# ---- Iceberg ----
# iceberg-flink-runtime: Flink 讀寫 Iceberg 表
# iceberg-aws-bundle:    S3FileIO，透過 S3 API 讀寫 MinIO
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.9.1/iceberg-flink-runtime-1.20-1.9.1.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar /opt/flink/lib/

# Iceberg 的 Flink 模組內部會用到 Hadoop 的類別 (Configuration 等)，Flink image 本身沒有
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar /opt/flink/lib/
ADD --chown=flink:flink https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar /opt/flink/lib/
