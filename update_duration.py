#!/usr/bin/env python3
"""
从 Telegram Desktop 导出的 JSON 文件中提取视频时长，批量更新到数据库。
用法：
  1. 修改下方配置变量
  2. python update_duration.py
"""

import json
import requests
import time

# ====== 配置区 ======
WORKER_URL = "https://nekorrvg.18378006973.workers.dev"  # 你的 Worker URL
ADMIN_SECRET = ""  # 如果有设置 ADMIN_SECRET 则填写
JSON_FILE_PATH = "result.json"  # Telegram Desktop 导出的 JSON 文件路径
TARGET_CHAT_ID = -1001234567890  # 目标群组 ID（超级群组格式：-100 + JSON里的id）
BATCH_SIZE = 50  # 每批更新条数
# ====================


def load_duration_map(json_path):
    """从 JSON 文件中提取 message_id -> duration 的映射"""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = data.get("messages", [])
    duration_map = {}

    for msg in messages:
        if msg.get("type") != "message":
            continue

        msg_id = msg.get("id")
        if not msg_id:
            continue

        # 尝试获取 duration（秒）
        duration = None
        if "duration_seconds" in msg:
            duration = msg["duration_seconds"]
        elif "duration" in msg:
            duration = msg["duration"]

        # 只处理有 duration 的视频/动图消息
        if duration is not None and isinstance(duration, (int, float)):
            duration_map[msg_id] = int(duration)

    return duration_map


def update_durations_via_api(worker_url, admin_secret, chat_id, duration_map):
    """通过 Worker API 批量更新 duration"""

    # 构建更新数据
    updates = [
        {"message_id": msg_id, "chat_id": chat_id, "duration": dur}
        for msg_id, dur in duration_map.items()
    ]

    print(f"共找到 {len(updates)} 条带时长的记录")

    headers = {"Content-Type": "application/json"}
    if admin_secret:
        headers["Authorization"] = admin_secret

    success_count = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]

        try:
            resp = requests.post(
                f"{worker_url}/api/update_duration",
                headers=headers,
                json={"updates": batch},
                timeout=30
            )

            if resp.status_code == 200:
                result = resp.json()
                updated = result.get("updated", 0)
                success_count += updated
                print(f"批次 {i // BATCH_SIZE + 1}: 更新 {updated} 条")
            else:
                print(f"批次 {i // BATCH_SIZE + 1} 失败: HTTP {resp.status_code} - {resp.text}")

        except Exception as e:
            print(f"批次 {i // BATCH_SIZE + 1} 异常: {e}")

        # 避免请求过快
        time.sleep(0.5)

    print(f"\n完成！共更新 {success_count} 条记录")


def main():
    print("=" * 50)
    print("Telegram 视频时长批量更新工具")
    print("=" * 50)

    print(f"\n1. 正在读取 JSON 文件: {JSON_FILE_PATH}")
    duration_map = load_duration_map(JSON_FILE_PATH)
    print(f"   找到 {len(duration_map)} 条带时长的消息")

    if not duration_map:
        print("   没有找到带时长的消息，退出。")
        return

    print(f"\n2. 开始向 Worker 发送更新请求...")
    update_durations_via_api(WORKER_URL, ADMIN_SECRET, TARGET_CHAT_ID, duration_map)


if __name__ == "__main__":
    main()
