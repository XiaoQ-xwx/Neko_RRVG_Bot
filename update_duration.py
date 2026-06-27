#!/usr/bin/env python3
"""
从 Telegram Desktop 导出的 JSON 文件中提取历史媒体元数据，批量回填到数据库。
用法：
  1. 修改下方配置变量
  2. python update_duration.py
"""

import json
import requests
import time

# ====== 配置区 ======
WORKER_URL = "https://nekorrvg.18378006973.workers.dev/"  # 你的 Worker URL
ADMIN_SECRET = ""  # 如果有设置 ADMIN_SECRET 则填写
JSON_FILE_PATH = "result.json"  # Telegram Desktop 导出的 JSON 文件路径
TARGET_CHAT_ID = -1003890159546  # 目标群组 ID（超级群组格式：-100 + JSON里的id）
BATCH_SIZE = 50  # 每批更新条数
# ====================


def load_media_updates(json_path):
    """从 JSON 文件中提取 message_id -> 历史媒体元数据 的映射"""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = data.get("messages", [])
    media_updates = []

    for msg in messages:
        if msg.get("type") != "message":
            continue

        msg_id = msg.get("id")
        if not isinstance(msg_id, int):
            continue

        media_type = None
        if "photo" in msg:
            media_type = "photo"
        elif msg.get("media_type") == "video_file":
            media_type = "video"
        elif msg.get("media_type") == "animation":
            media_type = "animation"
        elif "media_type" in msg:
            media_type = "document"

        if not media_type:
            continue

        duration = None
        if isinstance(msg.get("duration_seconds"), (int, float)):
            duration = int(msg["duration_seconds"])
        elif isinstance(msg.get("duration"), (int, float)):
            duration = int(msg["duration"])

        media_updates.append({
            "message_id": msg_id,
            "chat_id": TARGET_CHAT_ID,
            "duration": duration,
            "raw_message_json": {
                "source": "telegram_desktop_export",
                "message": msg,
            },
        })

    return media_updates


def update_media_metadata_via_api(worker_url, admin_secret, media_updates):
    """通过 Worker API 批量回填历史媒体元数据"""
    print(f"共找到 {len(media_updates)} 条媒体记录")

    headers = {"Content-Type": "application/json"}
    if admin_secret:
        headers["Authorization"] = admin_secret

    success_count = 0
    for i in range(0, len(media_updates), BATCH_SIZE):
        batch = media_updates[i:i + BATCH_SIZE]

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
                print(f"批次 {i // BATCH_SIZE + 1}: 回填 {updated} 条")
            else:
                print(f"批次 {i // BATCH_SIZE + 1} 失败: HTTP {resp.status_code} - {resp.text}")

        except Exception as e:
            print(f"批次 {i // BATCH_SIZE + 1} 异常: {e}")

        time.sleep(0.5)

    print(f"\n完成！共回填 {success_count} 条记录")


def main():
    print("=" * 50)
    print("Telegram 历史媒体元数据回填工具")
    print("=" * 50)

    print(f"\n1. 正在读取 JSON 文件: {JSON_FILE_PATH}")
    media_updates = load_media_updates(JSON_FILE_PATH)
    print(f"   找到 {len(media_updates)} 条媒体消息")

    if not media_updates:
        print("   没有找到可回填的媒体消息，退出。")
        return

    print(f"\n2. 开始向 Worker 发送回填请求...")
    update_media_metadata_via_api(WORKER_URL, ADMIN_SECRET, media_updates)


if __name__ == "__main__":
    main()
