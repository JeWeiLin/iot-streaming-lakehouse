"""
用法: python consumer.py [group_id] [topic]
  python consumer.py                         # group=test-group, topic=test-topic
  python consumer.py my-group                # 指定 group
"""
import json
import sys

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC = "test-topic"


def main():
    group_id = sys.argv[1] if len(sys.argv) > 1 else "test-group"
    topic = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_TOPIC
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",  # 新的 group 從最舊的訊息開始讀
        "enable.auto.commit": True,
    })

    def on_assign(c, partitions):
        print(f"📌 分配到 partitions: {[p.partition for p in partitions]}")

    consumer.subscribe([topic], on_assign=on_assign)
    print(f"🎧 Consumer 啟動 (topic={topic}, group={group_id})，等待訊息中... Ctrl+C 結束")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ 錯誤: {msg.error()}")
                continue

            value = json.loads(msg.value())
            print(
                f"📩 partition={msg.partition()} offset={msg.offset()} "
                f"key={msg.key().decode() if msg.key() else None} value={value}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("👋 Consumer 已關閉")


if __name__ == "__main__":
    main()
