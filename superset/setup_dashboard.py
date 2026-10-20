"""
透過 Superset REST API 建立儀表板「IoT 感測器監控」（只用標準函式庫，在本機執行）

用法: python superset/setup_dashboard.py
  - 需要 superset、trino、iceberg-rest、minio 都在執行，且 iot.sensor_* 表已有資料
  - 重跑會先刪掉同名的儀表板、圖表、dataset 再重建

Iceberg 裡的時間是 UTC，這裡用 virtual dataset (自訂 SQL) 轉成台灣時間再給圖表用
"""
import http.cookiejar
import json
import urllib.parse
import urllib.request

BASE = "http://localhost:8088"
DATABASE_NAME = "Iceberg (Trino)"
DASHBOARD_TITLE = "IoT 感測器監控"
TZ = "Asia/Taipei"

DATASETS = {
    # 原始讀數 + 網路延遲 (Kafka 收到時間 - 量測時間)
    "iot_sensor_readings": f"""
SELECT
  device_id, location, seq, temperature, humidity, battery,
  CAST(event_time AT TIME ZONE '{TZ}' AS timestamp(3)) AS event_time,
  date_diff('millisecond', event_time, arrival_time) / 1000.0 AS delay_sec
FROM iot.sensor_readings""",

    # Flink 算好的每分鐘統計
    "iot_sensor_1min": f"""
SELECT
  device_id, location,
  CAST(with_timezone(window_start, 'UTC') AT TIME ZONE '{TZ}' AS timestamp(3)) AS window_start,
  avg_temp, min_temp, max_temp, avg_humidity, min_battery, cnt
FROM iot.sensor_1min""",

    "iot_sensor_alerts": f"""
SELECT
  device_id, location, seq, temperature, level,
  CAST(event_time AT TIME ZONE '{TZ}' AS timestamp(3)) AS event_time
FROM iot.sensor_alerts""",

    # 每台裝置的健康狀態：用 seq 斷號算遺失筆數 (seq 變小代表 producer 重新啟動，不算遺失)
    "iot_device_status": f"""
SELECT
  device_id,
  location,
  count(*) AS received,
  sum(CASE WHEN seq > prev_seq THEN seq - prev_seq - 1 ELSE 0 END) AS lost,
  round(100.0 * count(*) / (count(*) + sum(CASE WHEN seq > prev_seq THEN seq - prev_seq - 1 ELSE 0 END)), 2) AS completeness_pct,
  max_by(battery, event_time) AS battery,
  max(temperature) AS max_temp,
  round(avg(date_diff('millisecond', event_time, arrival_time)) / 1000.0, 2) AS avg_delay_sec,
  CAST(max(event_time) AT TIME ZONE '{TZ}' AS timestamp(3)) AS last_seen
FROM (
  SELECT *, lag(seq) OVER (PARTITION BY device_id ORDER BY event_time, seq) AS prev_seq
  FROM iot.sensor_readings
)
GROUP BY device_id, location""",
}


def metric(column, aggregate, label):
    return {"expressionType": "SIMPLE", "column": {"column_name": column},
            "aggregate": aggregate, "label": label}


def sql_metric(expression, label):
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


COUNT = sql_metric("COUNT(*)", "筆數")


def big_number(metric_, subheader, fmt="SMART_NUMBER"):
    return {"viz_type": "big_number_total", "metric": metric_, "subheader": subheader, "y_axis_format": fmt}


def line(column, aggregate, label, groupby, unit):
    return {
        "viz_type": "echarts_timeseries_line",
        "x_axis": "window_start",
        "time_grain_sqla": "PT1M",
        "metrics": [metric(column, aggregate, label)],
        "groupby": [groupby],
        "row_limit": 10000,
        "y_axis_title": unit,
        "y_axis_title_position": "Left",
    }


# (圖表名稱, dataset, 寬度, 參數)；寬度加總 12 為一列
CHARTS = [
    # ---- 第 1 列：總覽數字 ----
    ("總讀數", "iot_sensor_readings", 3, big_number(COUNT, "6 台感測器")),
    ("溫度告警", "iot_sensor_alerts", 3, big_number(COUNT, "溫度 > 32°C")),
    ("資料完整率", "iot_device_status", 3, big_number(
        sql_metric("ROUND(100.0 * SUM(received) / (SUM(received) + SUM(lost)), 2)", "完整率 %"),
        "% (依 seq 斷號計算封包遺失)", ".2f")),
    ("遲到超過 5 秒", "iot_sensor_readings", 3, big_number(
        sql_metric("SUM(CASE WHEN delay_sec > 5 THEN 1 ELSE 0 END)", "遲到筆數"),
        "筆，超過 watermark，未列入每分鐘統計")),

    # ---- 第 2 列：溫度趨勢 ----
    ("各裝置每分鐘最高溫度", "iot_sensor_1min", 12,
     line("max_temp", "MAX", "最高溫度", "device_id", "°C")),

    # ---- 第 3 列 ----
    ("各位置平均溼度", "iot_sensor_1min", 6,
     line("avg_humidity", "AVG", "平均溼度", "location", "%RH")),
    ("各裝置告警數", "iot_sensor_alerts", 6, {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": "device_id",
        "metrics": [COUNT],
        "groupby": ["level"],
        "stack": "Stack",
        "row_limit": 1000,
    }),

    # ---- 第 4 列 ----
    ("各裝置電量", "iot_sensor_1min", 6,
     line("min_battery", "MIN", "電量", "device_id", "%")),
    ("每分鐘收到筆數", "iot_sensor_1min", 6,
     line("cnt", "SUM", "筆數", "device_id", "筆/分鐘")),

    # ---- 第 5、6 列：明細 ----
    ("裝置狀態", "iot_device_status", 12, {
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ["device_id", "location", "battery", "max_temp", "received", "lost",
                        "completeness_pct", "avg_delay_sec", "last_seen"],
        "order_by_cols": [json.dumps(["device_id", True])],
        "row_limit": 100,
    }),
    ("最新告警", "iot_sensor_alerts", 12, {
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": ["event_time", "device_id", "location", "temperature", "level"],
        "order_by_cols": [json.dumps(["event_time", False])],
        "row_limit": 50,
    }),
]

# 舊版 (producer 改版前) 建立的儀表板、dataset、圖表，重跑時一併清掉
LEGACY_DASHBOARDS = ["IoT 溫度監控"]
LEGACY_DATASETS = ["iot_temp_1min", "iot_alerts", "iot_raw_events"]
LEGACY_CHARTS = ["總筆數", "告警總數", "各 user 告警數", "每分鐘平均溫度"]


class Superset:
    def __init__(self):
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.headers = {"Content-Type": "application/json", "Referer": BASE}
        token = self.call("POST", "/api/v1/security/login", {
            "username": "admin", "password": "admin", "provider": "db"})["access_token"]
        self.headers["Authorization"] = f"Bearer {token}"
        self.headers["X-CSRFToken"] = self.call("GET", "/api/v1/security/csrf_token/")["result"]

    def call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(BASE + path, data=data, method=method, headers=self.headers)
        try:
            with self.opener.open(req) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            raise SystemExit(f"❌ {method} {path} -> {e.code}: {e.read().decode()[:500]}")

    def find(self, resource, column, value):
        q = json.dumps({"filters": [{"col": column, "opr": "eq", "value": value}]})
        return [r["id"] for r in self.call("GET", f"/api/v1/{resource}/?q={urllib.parse.quote(q)}")["result"]]


def main():
    ss = Superset()
    db_id = ss.find("database", "database_name", DATABASE_NAME)[0]

    # 清掉舊的 (重跑時)
    for title in [DASHBOARD_TITLE] + LEGACY_DASHBOARDS:
        for dash_id in ss.find("dashboard", "dashboard_title", title):
            ss.call("DELETE", f"/api/v1/dashboard/{dash_id}")
    for name in [c[0] for c in CHARTS] + LEGACY_CHARTS:
        for chart_id in ss.find("chart", "slice_name", name):
            ss.call("DELETE", f"/api/v1/chart/{chart_id}")
    for name in list(DATASETS) + LEGACY_DATASETS:
        for ds_id in ss.find("dataset", "table_name", name):
            ss.call("DELETE", f"/api/v1/dataset/{ds_id}")

    datasets = {}
    for name, sql in DATASETS.items():
        datasets[name] = ss.call("POST", "/api/v1/dataset/", {
            "database": db_id, "schema": "iot", "table_name": name, "sql": sql.strip()})["id"]
        print(f"📦 dataset {name} (id={datasets[name]})")

    dash_id = ss.call("POST", "/api/v1/dashboard/", {"dashboard_title": DASHBOARD_TITLE})["id"]

    # 版面：寬度加總 12 為一列
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }
    row, row_width = None, 12
    for i, (name, ds, width, params) in enumerate(CHARTS):
        chart_id = ss.call("POST", "/api/v1/chart/", {
            "slice_name": name,
            "viz_type": params["viz_type"],
            "datasource_id": datasets[ds],
            "datasource_type": "table",
            "params": json.dumps({**params, "datasource": f"{datasets[ds]}__table", "adhoc_filters": []}),
            "dashboards": [dash_id],
        })["id"]
        print(f"📊 chart {name} (id={chart_id})")

        if row_width + width > 12:
            row = f"ROW-{i}"
            layout[row] = {"type": "ROW", "id": row, "children": [],
                           "parents": ["ROOT_ID", "GRID_ID"],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout["GRID_ID"]["children"].append(row)
            row_width = 0
        key = f"CHART-{chart_id}"
        height = 30 if params["viz_type"] == "big_number_total" else 50
        layout[key] = {"type": "CHART", "id": key, "children": [],
                       "parents": ["ROOT_ID", "GRID_ID", row],
                       "meta": {"chartId": chart_id, "sliceName": name, "width": width, "height": height}}
        layout[row]["children"].append(key)
        row_width += width

    ss.call("PUT", f"/api/v1/dashboard/{dash_id}", {
        "position_json": json.dumps(layout),
        "json_metadata": json.dumps({"refresh_frequency": 60}),  # 每 60 秒自動重新整理
        "published": True,
    })
    print(f"✅ 儀表板完成: {BASE}/superset/dashboard/{dash_id}/")


if __name__ == "__main__":
    main()
