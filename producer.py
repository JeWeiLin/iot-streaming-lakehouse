"""
IoT 溫溼度感測器模擬器 -> Kafka (topic: test-topic)

用法: python producer.py [rounds] [interval]
  rounds    回報幾輪，每輪 6 台裝置各送 1 筆；0 = 不停送直到 Ctrl+C (預設 10)
  interval  每輪間隔幾秒 (預設 1)

範例:
  python producer.py          # 送 10 輪 (60 筆)，每秒 1 輪
  python producer.py 0 1      # 每秒 1 輪 (每秒 6 筆)，不停送 (給 Flink 用)
  python producer.py 0 0.5    # 每 0.5 秒 1 輪 (每秒 12 筆)

訊息 (key = device_id):
  {"device_id": "sensor-03", "location": "warehouse-b", "seq": 128,
   "temperature": 27.41, "humidity": 58.2, "battery": 86.4, "ts": 1792411021538}

模擬的真實情境:
  - 3 個位置各 2 台，各有自己的基準溫度；溫度平滑變化 (不會亂跳)
  - 溼度與溫度反向變動；電量每次回報慢慢下降 (sensor-06 是快沒電的舊裝置)
  - 偶爾發生「過熱事件」：數十秒內升溫約 12°C 再慢慢降回 -> 觸發告警
  - 網路延遲：約 3% 的資料晚 1~8 秒才送達 (ts 是量測時間) -> 練習 watermark
  - 封包遺失：約 1% 的讀數直接遺失 -> 可以從 seq 斷號算出遺失率
"""
import argparse
import itertools
import json
import random
import time
from dataclasses import dataclass

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "test-topic"

# (device_id, 位置, 基準溫度 °C, 起始電量 %, 每次回報耗電 %)
DEVICES = [
    ("sensor-01", "office-1f", 24.0, 95.0, 0.002),
    ("sensor-02", "office-1f", 24.5, 88.0, 0.002),
    ("sensor-03", "warehouse-b", 27.0, 76.0, 0.003),
    ("sensor-04", "warehouse-b", 27.5, 64.0, 0.003),
    ("sensor-05", "server-room", 20.5, 91.0, 0.002),
    ("sensor-06", "server-room", 21.0, 23.0, 0.015),   # 快沒電的舊裝置
]

OVERHEAT_PROB = 0.003   # 每次回報開始過熱事件的機率 (6 台每秒各 1 筆 => 約每分鐘 1 次)
OVERHEAT_PEAK = 12.0    # 過熱最高多幾度
LATE_PROB = 0.03        # 延遲送達的機率
LATE_SECONDS = (1, 8)   # 延遲幾秒 (watermark 容許 5 秒，超過的不會算進每分鐘統計)
DROP_PROB = 0.01        # 封包遺失的機率


@dataclass
class Device:
    device_id: str
    location: str
    base_temp: float
    battery: float
    drain: float
    temperature: float = 0.0
    humidity: float = 60.0
    seq: int = 0
    overheat: float = 0.0     # 過熱事件額外增加的溫度
    heating: bool = False     # 過熱事件是否還在升溫

    def __post_init__(self):
        self.temperature = self.base_temp + random.uniform(-0.5, 0.5)
        self.humidity = random.uniform(55, 65)

    def read(self):
        """產生下一筆讀數 (模擬感測器量測)"""
        self.seq += 1

        # 溫度：往基準值拉回 + 小幅隨機變動 => 平滑的曲線
        self.temperature += (self.base_temp - self.temperature) * 0.05 + random.gauss(0, 0.15)

        # 過熱事件：先升溫到約 +12°C，再慢慢降回
        if not self.heating and self.overheat == 0 and random.random() < OVERHEAT_PROB:
            self.heating = True
        if self.heating:
            self.overheat += random.uniform(0.6, 1.2)
            if self.overheat >= OVERHEAT_PEAK:
                self.heating = False
        elif self.overheat > 0:
            self.overheat = max(0.0, self.overheat - random.uniform(0.2, 0.5))

        temp = self.temperature + self.overheat

        # 溼度：溫度越高越乾
        target_humidity = 60 - (temp - self.base_temp) * 3
        self.humidity += (target_humidity - self.humidity) * 0.1 + random.gauss(0, 0.5)
        self.battery = max(0.0, self.battery - self.drain * random.uniform(0.5, 1.5))

        return {
            "device_id": self.device_id,
            "location": self.location,
            "seq": self.seq,
            "temperature": round(temp, 2),
            "humidity": round(min(max(self.humidity, 0), 100), 1),
            "battery": round(self.battery, 1),
            "ts": int(time.time() * 1000),   # 量測時間 (毫秒)
        }


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ 傳送失敗: {err}")


def parse_args():
    parser = argparse.ArgumentParser(description="IoT 溫溼度感測器模擬器")
    parser.add_argument("rounds", nargs="?", type=int, default=10, help="回報幾輪，0 = 不停送 (預設 10)")
    parser.add_argument("interval", nargs="?", type=float, default=1.0, help="每輪間隔幾秒 (預設 1)")
    return parser.parse_args()


def main():
    args = parse_args()
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    devices = [Device(*d) for d in DEVICES]
    delayed = []   # (預計送達時間, 讀數)

    def send(reading, note=""):
        producer.produce(TOPIC, key=reading["device_id"], value=json.dumps(reading), callback=delivery_report)
        print(f"{note}{reading['device_id']} seq={reading['seq']:<5} "
              f"{reading['temperature']:6.2f}°C {reading['humidity']:5.1f}% 🔋{reading['battery']:5.1f}%")

    total = "不停送" if args.rounds == 0 else f"共 {args.rounds} 輪"
    print(f"🚀 {len(devices)} 台裝置，每 {args.interval} 秒回報一次，{total} -> {TOPIC}，Ctrl+C 結束")

    rounds = itertools.count() if args.rounds == 0 else range(args.rounds)
    next_round = time.time()
    try:
        for _ in rounds:
            now = time.time()

            # 延遲到期的資料現在才送達 (ts 仍是當初的量測時間 => 亂序)
            for item in [d for d in delayed if d[0] <= now]:
                delayed.remove(item)
                send(item[1], note=f"🐢 遲到 {now - item[1]['ts'] / 1000:.1f}s  ")

            for device in devices:
                reading = device.read()
                if random.random() < DROP_PROB:
                    print(f"📉 遺失    {reading['device_id']} seq={reading['seq']}")
                    continue
                if random.random() < LATE_PROB:
                    delayed.append((now + random.uniform(*LATE_SECONDS), reading))
                    continue
                send(reading, note="🔥 過熱    " if device.overheat > 0 else "          ")

            producer.flush()
            # 以固定的時間點排程，送出本身花的時間不會讓間隔越拖越長
            next_round += args.interval
            time.sleep(max(0.0, next_round - time.time()))

        # 結束前把還在延遲中的資料送出
        for _, reading in delayed:
            send(reading, note="🐢 遲到    ")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
