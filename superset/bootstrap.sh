#!/bin/sh
# Superset 啟動流程：初始化 metadata DB -> 建立管理員 -> 註冊 Trino 連線 -> 啟動 web server
# metadata (帳號、圖表、儀表板) 存在 /app/superset_home 的 volume，重啟不會消失
set -e

superset db upgrade

# 帳號已存在時會失敗，忽略即可
superset fab create-admin \
  --username admin --password admin \
  --firstname Admin --lastname User --email admin@example.com || true

superset init

# 自動建立連到 Trino 的資料庫連線 (catalog = iceberg)
superset set-database-uri \
  --database_name "Iceberg (Trino)" \
  --uri "trino://admin@trino:8080/iceberg"

exec /app/docker/entrypoints/run-server.sh
