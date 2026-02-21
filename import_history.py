import json
import requests
import time

# ==========================================
# ⚙️ 导入脚本配置区 (必填)
# ==========================================
# 1. 你的 Worker 完整域名 API 接口 (必须包含 /api/import)
WORKER_URL = "https://你的worker域名.workers.dev/api/import"

# 2. 与你在 Cloudflare 环境变量中设置的一致的密钥
ADMIN_SECRET = "你的_ADMIN_SECRET_密钥"

# 3. Telegram 导出的 JSON 文件路径 (放在同目录下直接写文件名)
JSON_FILE_PATH = "result.json"

# 4. 目标群组的 Chat ID (重要！带负号的一长串数字，用于群组数据隔离，如 -1001234567890)
TARGET_CHAT_ID = -1001234567890 

# 5. 你想把这批历史数据导入到哪个分类下？
TARGET_CATEGORY = "历史精选" 
# ==========================================

def process_and_upload():
    print(f"📦 正在读取 JSON 文件: {JSON_FILE_PATH} ...")
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("❌ 找不到 JSON 文件，请检查路径！")
        return

    messages = data.get('messages', [])
    valid_media = []

    print("🔍 正在解析媒体消息...")
    for msg in messages:
        # 过滤掉非普通消息
        if msg.get('type') != 'message': continue
        
        # 判断是否包含媒体
        media_type = None
        if 'photo' in msg:
            media_type = 'photo'
        elif 'media_type' in msg:
            if msg['media_type'] == 'video_file':
                media_type = 'video'
            elif msg['media_type'] == 'animation':
                media_type = 'animation'
            else:
                media_type = 'document'
        
        if not media_type:
            continue
        
        # 提取文字配文 (处理 TG JSON 特殊的 text_entities 结构)
        text_entities = msg.get('text', [])
        caption = ""
        if isinstance(text_entities, list):
            caption = "".join([t if isinstance(t, str) else t.get('text', '') for t in text_entities])
        elif isinstance(text_entities, str):
            caption = text_entities

        # 组装适配数据库结构的数据
        # 历史记录无 file_id，使用 message_id 伪造 unique_id 用于防重
        valid_media.append({
            "message_id": msg['id'],
            "chat_id": TARGET_CHAT_ID,
            "topic_id": None, # 历史数据统一为无 Topic
            "category_name": TARGET_CATEGORY,
            "file_unique_id": f"import_{TARGET_CHAT_ID}_{msg['id']}", 
            "file_id": "", 
            "media_type": media_type,
            "caption": caption[:100] # 截断部分超长文本防止数据库溢出
        })

    total = len(valid_media)
    if total == 0:
        print("⚠️ 没有找到任何有效的媒体消息，请确认导出的 JSON 是否包含媒体内容。")
        return
        
    print(f"✅ 解析完成！共发现 {total} 条有效媒体记录。开始分批推送...")
    
    # 每次批量发送 50 条，防止触发 Cloudflare 限流
    batch_size = 50
    success_count = 0
    
    headers = {
        'Authorization': ADMIN_SECRET, 
        'Content-Type': 'application/json'
    }

    for i in range(0, total, batch_size):
        batch = valid_media[i : i + batch_size]
        try:
            res = requests.post(WORKER_URL, headers=headers, json={"data": batch})
            if res.status_code == 200:
                success_count += len(batch)
                print(f"🚀 进度: [{success_count} / {total}] 条上传成功...")
            else:
                print(f"❌ 上传失败: HTTP {res.status_code} - {res.text}")
        except Exception as e:
            print(f"🔌 网络请求出错: {e}")
            
        time.sleep(0.5) # 请求节流

    print("\n🎉 大功告成！所有历史数据已成功推送至数据库！")

if __name__ == "__main__":
    process_and_upload()
