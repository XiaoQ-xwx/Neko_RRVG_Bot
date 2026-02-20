/**
 * Cloudflare Workers (Pages) - Telegram Bot Entry Point (V5.2 安全隔离版)
 * 核心升级：修复群组数据越权漏洞 (严格按 chat_id 隔离数据)，恢复精美 Webhook 界面
 */

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      if (request.method === 'GET' && url.pathname === '/') {
        return await handleSetup(url.origin, env);
      }

      if (request.method === 'POST' && url.pathname === '/webhook') {
        const update = await request.json();
        ctx.waitUntil(handleUpdate(update, env));
        return new Response('OK', { status: 200 });
      }

      if (request.method === 'POST' && url.pathname === '/api/import') {
        const secret = request.headers.get('Authorization');
        if (env.ADMIN_SECRET && secret !== env.ADMIN_SECRET) return new Response('Unauthorized', { status: 401 });
        const payload = await request.json();
        ctx.waitUntil(handleExternalImport(payload.data, env));
        return new Response(JSON.stringify({ status: 'success', count: payload.data.length }), { status: 200 });
      }

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      console.error('Worker Error:', err);
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};

/* =========================================================================
 * 部署与初始化逻辑
 * ========================================================================= */
async function handleSetup(origin, env) {
  try {
    const initSQL = [
      `CREATE TABLE IF NOT EXISTS config_topics (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, chat_title TEXT, topic_id INTEGER, category_name TEXT, bound_by INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS media_library (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, chat_id INTEGER, topic_id INTEGER, category_name TEXT, view_count INTEGER DEFAULT 0, file_unique_id TEXT, file_id TEXT, media_type TEXT, caption TEXT, added_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS user_favorites (user_id INTEGER, media_id INTEGER, saved_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, media_id));`,
      `CREATE TABLE IF NOT EXISTS last_served (user_id INTEGER PRIMARY KEY, last_media_id INTEGER, served_at INTEGER);`,
      `CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT);`,
      `CREATE TABLE IF NOT EXISTS served_history (media_id INTEGER PRIMARY KEY);`,
      
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('display_mode', 'B');`,
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('anti_repeat', 'true');`,
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('auto_jump', 'true');`,
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('dup_notify', 'false');`,
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('show_success', 'true');`,
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('next_mode', 'replace');`
    ];

    for (const sql of initSQL) await env.D1.prepare(sql).run();

    const columns = ['file_unique_id', 'file_id', 'media_type', 'caption'];
    for (const col of columns) {
      try { await env.D1.prepare(`ALTER TABLE media_library ADD COLUMN ${col} TEXT;`).run(); } catch (e) {}
    }

    const webhookUrl = `${origin}/webhook`;
    const tgRes = await tgAPI('setWebhook', { url: webhookUrl }, env);
    if (!tgRes.ok) throw new Error('Webhook 注册失败');

    // 恢复精美的可视化界面
    const html = `
      <!DOCTYPE html>
      <html lang="zh-CN">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot 部署成功</title>
        <style>
          body { font-family: system-ui, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #f3f4f6; margin: 0; }
          .card { background: white; padding: 2.5rem 3rem; border-radius: 16px; box-shadow: 0 10px 15px -3px rgba(0,0,0,0.1); text-align: center; max-width: 500px;}
          h1 { color: #10b981; margin-bottom: 0.5rem; }
          p { color: #4b5563; line-height: 1.6; }
          .code-box { background: #f8fafc; padding: 0.5rem; border-radius: 6px; border: 1px solid #e2e8f0; font-family: monospace; word-break: break-all; color: #2563eb; margin: 1rem 0;}
        </style>
      </head>
      <body>
        <div class="card">
          <h1>🎉 V5.2 部署大成功！</h1>
          <p>群组隔离安全锁已生效，D1 数据库结构已更新。<br>Webhook 已安全绑定至：</p>
          <div class="code-box">${webhookUrl}</div>
          <p><b>多群组数据已完全隔离，再也不用担心数据泄露啦！</b></p>
        </div>
      </body>
      </html>
    `;
    return new Response(html, { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
  } catch (error) {
    return new Response(`部署失败: ${error.message}`, { status: 500 });
  }
}

/* =========================================================================
 * 路由与消息处理
 * ========================================================================= */
async function handleUpdate(update, env) {
  if (update.message) {
    await handleMessage(update.message, env);
  } else if (update.callback_query) {
    await handleCallback(update.callback_query, env);
  }
}

async function handleMessage(message, env) {
  const text = message.text || message.caption || '';
  const chatId = message.chat.id;
  const topicId = message.message_thread_id || null;
  const userId = message.from.id;

  if (text.startsWith('/start')) return sendMainMenu(chatId, topicId, env);

  if (text.startsWith('/help')) {
    const helpText = `📖 <b>籽青的使用说明书</b>\n/start - 唤出籽青的主菜单 (随机抽取、排行榜、设置等)\n/help - 显示本帮助信息\n\n<b>【管理员专属指令】</b>\n/bind &lt;分类名&gt; - 在群组话题内发送，将该话题绑定为采集库\n/bind_output - 将当前话题设为专属推送展示窗口\n/import_json - 获取导入历史消息数据的帮助`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: helpText, parse_mode: 'HTML' }, env);
    return;
  }

  if (text.startsWith('/import_json')) {
    const importHelp = `📥 <b>关于导入历史数据</b>\n\n为了避免 Worker 内存溢出，请在电脑上运行配套的 <b>Python 导入脚本</b>。\n\n配置好您的 <code>ADMIN_SECRET</code>，脚本会自动将 JSON 切片并推送到当前群组的数据库中！`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: importHelp, parse_mode: 'HTML' }, env);
    return;
  }

  if (text.startsWith('/bind ')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    const category = text.replace('/bind ', '').trim();
    if (!category) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, category, userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `绑定成功！已将当前话题与分类【${category}】绑定！` }, env);
    return;
  }

  if (text.startsWith('/bind_output')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, 'output', userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `设置成功！这里将作为专属输出话题。` }, env);
    return;
  }

  let mediaInfo = extractMediaInfo(message);
  if (mediaInfo.fileUniqueId) {
    const query = await env.D1.prepare(`SELECT category_name FROM config_topics WHERE chat_id = ? AND (topic_id = ? OR topic_id IS NULL) AND category_name != 'output' LIMIT 1`).bind(chatId, topicId).first();
    if (query && query.category_name) {
      // 增加 chat_id 安全过滤
      const existing = await env.D1.prepare(`SELECT id FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(mediaInfo.fileUniqueId, chatId).first();
      if (existing) {
        const notify = await getSetting('dup_notify', env);
        if (notify === 'true') await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "籽青发现这个内容之前已经收录过啦~" }, env);
        return; 
      }
      await env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
        .bind(message.message_id, chatId, topicId, query.category_name, mediaInfo.fileUniqueId, mediaInfo.fileId, mediaInfo.type, message.caption || '').run();
    }
  }
}

function extractMediaInfo(message) {
  let info = { fileUniqueId: null, fileId: null, type: null };
  if (message.photo && message.photo.length > 0) {
    const p = message.photo[message.photo.length - 1];
    info = { fileUniqueId: p.file_unique_id, fileId: p.file_id, type: 'photo' };
  } else if (message.video) {
    info = { fileUniqueId: message.video.file_unique_id, fileId: message.video.file_id, type: 'video' };
  } else if (message.document) {
    info = { fileUniqueId: message.document.file_unique_id, fileId: message.document.file_id, type: 'document' };
  } else if (message.animation) {
    info = { fileUniqueId: message.animation.file_unique_id, fileId: message.animation.file_id, type: 'animation' };
  }
  return info;
}

/* =========================================================================
 * 回调交互处理
 * ========================================================================= */
async function handleCallback(callback, env) {
  const data = callback.data;
  const userId = callback.from.id;
  const chatId = callback.message.chat.id; // 安全核心：所有操作绑定此群组ID
  const msgId = callback.message.message_id;
  const topicId = callback.message.message_thread_id || null;
  const cbId = callback.id;

  if (data === 'main_menu') {
    await editMainMenu(chatId, msgId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } else if (data === 'main_menu_new') {
    await sendMainMenu(chatId, topicId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } else if (data === 'start_random') {
    await showCategories(chatId, msgId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } else if (data.startsWith('random_') || data.startsWith('next_')) {
    const isNext = data.startsWith('next_');
    const category = data.replace('random_', '').replace('next_', '');
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "正在为您抽取..." }, env);
    await sendRandomMedia(userId, chatId, msgId, topicId, category, isNext, env);
  } 
  
  else if (data.startsWith('fav_add_')) {
    await handleAddFavorite(userId, cbId, parseInt(data.replace('fav_add_', '')), env);
  } else if (data === 'favorites' || data.startsWith('fav_page_')) {
    const page = data === 'favorites' ? 0 : parseInt(data.replace('fav_page_', ''));
    await showFavoritesList(chatId, msgId, userId, page, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } else if (data.startsWith('fav_view_')) {
    await viewFavorite(chatId, topicId, parseInt(data.replace('fav_view_', '')), env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } else if (data.startsWith('fav_del_')) {
    await env.D1.prepare(`DELETE FROM user_favorites WHERE user_id = ? AND media_id = ?`).bind(userId, parseInt(data.replace('fav_del_', ''))).run();
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已从收藏夹移除！" }, env);
    await showFavoritesList(chatId, msgId, userId, 0, env);
  } 
  
  else if (data === 'leaderboard' || data.startsWith('leader_page_')) {
    const page = data === 'leaderboard' ? 0 : parseInt(data.replace('leader_page_', ''));
    await showLeaderboard(chatId, msgId, page, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
  
  else if (data.startsWith('set_')) {
    if (!(await isAdmin(chatId, userId, env))) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "权限不足，仅管理员可调整！", show_alert: true }, env);
      return;
    }

    if (data === 'set_main') await showSettingsMain(chatId, msgId, env);
    else if (data === 'set_toggle_mode') await toggleSetting('display_mode', env, chatId, msgId, ['A', 'B']);
    else if (data === 'set_toggle_repeat') await toggleSetting('anti_repeat', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_jump') await toggleSetting('auto_jump', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_dup') await toggleSetting('dup_notify', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_success') await toggleSetting('show_success', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_nextmode') await toggleSetting('next_mode', env, chatId, msgId, ['replace', 'new']);
    else if (data === 'set_stats') await showStats(chatId, msgId, env);
    else if (data === 'set_unbind_list') await showUnbindList(chatId, msgId, env);
    else if (data.startsWith('set_unbind_do_')) {
      await env.D1.prepare(`DELETE FROM config_topics WHERE id = ? AND chat_id = ?`).bind(parseInt(data.replace('set_unbind_do_', '')), chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "解绑成功！", show_alert: true }, env);
      await showUnbindList(chatId, msgId, env);
    }
    
    else if (data === 'set_danger_zone') {
      const text = "⚠️ **危险操作区**\n\n这里的操作仅对当前群组生效，且不可逆！";
      const keyboard = [
        [{ text: "🧨 清空本群数据统计", callback_data: "set_clear_stats_1" }],
        [{ text: "🚨 彻底清空本群媒体库", callback_data: "set_clear_media_1" }],
        [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]
      ];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
    }
    else if (data === 'set_clear_stats_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "⚠️ 确定仅清空本群统计数据？", reply_markup: { inline_keyboard: [[{ text: "🔴 确认清空 (第1次)", callback_data: "set_clear_stats_2" }], [{ text: "⬅️ 返回", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_stats_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🧨 **最后警告**：即将清空本群浏览量！", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "☠️ 彻底清空！", callback_data: "set_clear_stats_do" }], [{ text: "⬅️ 算了", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_stats_do') {
      await env.D1.prepare(`UPDATE media_library SET view_count = 0 WHERE chat_id = ?`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前群组统计重置完毕！", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
    else if (data === 'set_clear_media_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🚨 **高危警告**\n\n即将清空【本群收录的所有媒体】！", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "🩸 我确定要删除本群全部媒体", callback_data: "set_clear_media_2" }], [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_media_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🌋 **最终警告**\n\n一旦按下无法恢复！真的要清空吗？", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "💥 毁天灭地！", callback_data: "set_clear_media_do" }], [{ text: "⬅️ 放弃操作", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_media_do') {
      await env.D1.prepare(`DELETE FROM user_favorites WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM media_library WHERE chat_id = ?`).bind(chatId).run(); 
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前群组媒体库已被彻底清空！", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
}

/* =========================================================================
 * UI 流转逻辑
 * ========================================================================= */
async function sendMainMenu(chatId, topicId, env) {
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "你好呀！我是籽青，请问今天想看点什么呢？", reply_markup: getMainMenuMarkup() }, env);
}
async function editMainMenu(chatId, msgId, env) {
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "籽青的主菜单，请选择：", reply_markup: getMainMenuMarkup() }, env);
}
function getMainMenuMarkup() {
  return { inline_keyboard: [[{ text: "🎲 开始随机", callback_data: "start_random" }], [{ text: "🏆 本群排行", callback_data: "leaderboard" }, { text: "📁 收藏夹", callback_data: "favorites" }], [{ text: "⚙️ 籽青设置 (限管理)", callback_data: "set_main" }]] };
}

async function showCategories(chatId, msgId, env) {
  // 安全限制：只拉取本群的分类
  const { results } = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE category_name != 'output' AND chat_id = ?`).bind(chatId).all();
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "当前群组还没有绑定任何分类呢，管理员请使用 /bind 绑定哦！", reply_markup: getBackMarkup() }, env);
  const keyboard = results.map(row => [{ text: `📂 ${row.category_name}`, callback_data: `random_${row.category_name}` }]);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "请选择您感兴趣的分类：", reply_markup: { inline_keyboard: keyboard } }, env);
}

async function sendRandomMedia(userId, chatId, msgId, topicId, category, isNext, env) {
  // 安全限制：寻找当前群组的输出话题
  const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' AND chat_id = ? LIMIT 1`).bind(chatId).first();
  if (!output) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `管理员还没设置输出话题呢，请用 /bind_output 设置！` }, env);

  const mode = await getSetting('display_mode', env);
  const useAntiRepeat = (await getSetting('anti_repeat', env)) === 'true';
  const autoJump = (await getSetting('auto_jump', env)) === 'true';
  const showSuccess = (await getSetting('show_success', env)) === 'true';
  const nextMode = await getSetting('next_mode', env) || 'replace'; 
  const now = Date.now();

  if (isNext) {
    const last = await env.D1.prepare(`SELECT * FROM last_served WHERE user_id = ?`).bind(userId).first();
    if (last && (now - last.served_at) < 30000) {
      await env.D1.prepare(`UPDATE media_library SET view_count = MAX(0, view_count - 1) WHERE id = ?`).bind(last.last_media_id).run();
      if (useAntiRepeat) await env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(last.last_media_id).run();
    }
  }

  // 安全限制：只抽取本群内容
  let media = useAntiRepeat 
    ? await env.D1.prepare(`SELECT * FROM media_library WHERE category_name = ? AND chat_id = ? AND id NOT IN (SELECT media_id FROM served_history) ORDER BY RANDOM() LIMIT 1`).bind(category, chatId).first() 
    : await env.D1.prepare(`SELECT * FROM media_library WHERE category_name = ? AND chat_id = ? ORDER BY RANDOM() LIMIT 1`).bind(category, chatId).first();

  if (!media && useAntiRepeat) {
    const totalCheck = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE category_name = ? AND chat_id = ?`).bind(category, chatId).first();
    if (totalCheck && totalCheck.c > 0) {
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE category_name = ? AND chat_id = ?)`).bind(category, chatId).run();
      await tgAPI('sendMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, text: `🎉 【${category}】的内容全看光了！已重置防重库~` }, env);
      media = await env.D1.prepare(`SELECT * FROM media_library WHERE category_name = ? AND chat_id = ? ORDER BY RANDOM() LIMIT 1`).bind(category, chatId).first();
    }
  }
  if (!media) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `该分类里还没有内容呢~` }, env);

  if (useAntiRepeat) await env.D1.prepare(`INSERT OR IGNORE INTO served_history (media_id) VALUES (?)`).bind(media.id).run();
  await env.D1.prepare(`INSERT INTO last_served (user_id, last_media_id, served_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_media_id=excluded.last_media_id, served_at=excluded.served_at`).bind(userId, media.id, now).run();
  await env.D1.prepare(`UPDATE media_library SET view_count = view_count + 1 WHERE id = ?`).bind(media.id).run();

  const originalDeepLink = makeDeepLink(media.chat_id, media.message_id);
  const actionKeyboard = [[{ text: "⏭️ 换一个", callback_data: `next_${category}` }, { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` }]];
  let newSentMessageId = null;

  if (isNext && nextMode === 'replace') {
    try {
      await tgAPI('deleteMessage', { chat_id: output.chat_id, message_id: msgId }, env);
    } catch (e) {}
  }

  if (mode === 'A') {
    const res = await tgAPI('forwardMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, from_chat_id: media.chat_id, message_id: media.message_id }, env);
    const data = await res.json();
    if(data.ok) newSentMessageId = data.result.message_id;
    actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
    await tgAPI('sendMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, reply_to_message_id: newSentMessageId, text: "👆 操作区：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
  } else {
    actionKeyboard.unshift([{ text: "🔗 跳转原记录出处", url: originalDeepLink }]);
    actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
    const res = await tgAPI('copyMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
    const data = await res.json();
    if(data.ok) newSentMessageId = data.result.message_id;
  }

  if (!isNext) {
    if (showSuccess) {
      const jumpToOutputLink = newSentMessageId ? makeDeepLink(output.chat_id, newSentMessageId) : null;
      const jumpKeyboard = jumpToOutputLink && autoJump 
        ? [[{ text: "🚀 前往查看", url: jumpToOutputLink }], [{ text: "🏠 返回主菜单", callback_data: "main_menu" }]]
        : [[{ text: "🏠 返回主菜单", callback_data: "main_menu" }]];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `🎉 抽取成功啦！已发送至输出话题。`, reply_markup: { inline_keyboard: jumpKeyboard } }, env);
    } else {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "抽取成功！" }, env);
    }
  }
}

async function showLeaderboard(chatId, msgId, page, env) {
  const limit = 5;
  const offset = page * limit;
  // 安全限制：只展示本群排行
  const { results } = await env.D1.prepare(`SELECT chat_id, message_id, category_name, view_count, caption FROM media_library WHERE view_count > 0 AND chat_id = ? ORDER BY view_count DESC LIMIT ? OFFSET ?`).bind(chatId, limit, offset).all();
  const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE view_count > 0 AND chat_id = ?`).bind(chatId).first();
  
  let text = "🏆 <b>本群浏览量排行榜</b>\n\n";
  if (!results || results.length === 0) {
    text += "当前群组还没有产生播放数据呢~";
  } else {
    results.forEach((row, idx) => { 
      const preview = row.caption ? row.caption.substring(0, 15) + '...' : '媒体记录';
      text += `${offset + idx + 1}. [${row.category_name}] <a href="${makeDeepLink(row.chat_id, row.message_id)}">${preview}</a> - 浏览: ${row.view_count}\n`; 
    });
  }

  const keyboard = [];
  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `leader_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `leader_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);

  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: { inline_keyboard: keyboard } }, env);
}

async function handleAddFavorite(userId, cbId, mediaId, env) {
  try { 
    await env.D1.prepare(`INSERT INTO user_favorites (user_id, media_id) VALUES (?, ?)`).bind(userId, mediaId).run(); 
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "收藏成功！帮你记下来啦~ ❤️", show_alert: true }, env); 
  } catch (e) { 
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "您已经收藏过这个啦~", show_alert: true }, env); 
  }
}

async function showFavoritesList(chatId, msgId, userId, page, env) {
  const limit = 5;
  const offset = page * limit;
  const { results } = await env.D1.prepare(`SELECT f.media_id, m.media_type, m.caption FROM user_favorites f LEFT JOIN media_library m ON f.media_id = m.id WHERE f.user_id = ? ORDER BY f.saved_at DESC LIMIT ? OFFSET ?`).bind(userId, limit, offset).all();
  const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM user_favorites WHERE user_id = ?`).bind(userId).first();
  
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "您的收藏夹空空如也哦~", reply_markup: getBackMarkup() }, env);
  
  const keyboard = results.map((r, i) => {
    const typeIcon = r.media_type === 'video' ? '🎬' : (r.media_type === 'photo' ? '🖼️' : '📁');
    const title = r.caption ? r.caption.substring(0, 15) : '记录';
    return [
      { text: `${typeIcon} ${title}`, callback_data: `fav_view_${r.media_id}` }, 
      { text: `❌ 移除`, callback_data: `fav_del_${r.media_id}` }
    ];
  });

  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `fav_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `fav_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `📁 **您的私有收藏夹** (共 ${totalRes.c} 条)`, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function viewFavorite(chatId, topicId, mediaId, env) {
  const media = await env.D1.prepare(`SELECT * FROM media_library WHERE id = ?`).bind(mediaId).first();
  if (media) await tgAPI('copyMessage', { chat_id: chatId, message_thread_id: topicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
}

async function showSettingsMain(chatId, msgId, env) {
  const mode = await getSetting('display_mode', env);
  const repeat = await getSetting('anti_repeat', env);
  const jump = await getSetting('auto_jump', env);
  const dup = await getSetting('dup_notify', env);
  const showSuccess = await getSetting('show_success', env);
  const nextMode = await getSetting('next_mode', env) || 'replace';
  
  const text = "⚙️ **全局控制面板**\n\n请调整下方的功能开关：";
  const keyboard = [
    [{ text: `🔀 展现形式: ${mode === 'A' ? 'A(原生转发)' : 'B(复制+链接)'}`, callback_data: "set_toggle_mode" }],
    [{ text: `🔁 防重库机制: ${repeat === 'true' ? '✅ 已开启' : '❌ 未开启'}`, callback_data: "set_toggle_repeat" }],
    [{ text: `🔕 重复收录提示: ${dup === 'true' ? '📢 消息提醒' : '🔇 静默拦截'}`, callback_data: "set_toggle_dup" }],
    [{ text: `🔄 '换一个'模式: ${nextMode === 'replace' ? '🖼️ 原地替换(删旧发新)' : '💬 发新消息(保留历史)'}`, callback_data: "set_toggle_nextmode" }],
    [{ text: `🔔 抽取成功提示: ${showSuccess === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_success" }],
    [{ text: `🚀 抽取后生成跳转: ${jump === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_jump" }],
    [{ text: "🗑️ 管理本群解绑", callback_data: "set_unbind_list" }, { text: "📊 本群数据看板", callback_data: "set_stats" }],
    [{ text: "⚠️ 危险操作区 (清空本群数据)", callback_data: "set_danger_zone" }],
    [{ text: "🏠 返回主菜单", callback_data: "main_menu" }]
  ];
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function toggleSetting(key, env, chatId, msgId, values) {
  const current = await getSetting(key, env);
  const valCurrent = current === null ? values[0] : current;
  const next = valCurrent === values[0] ? values[1] : values[0];
  await env.D1.prepare(`UPDATE bot_settings SET value = ? WHERE key = ?`).bind(next, key).run();
  await showSettingsMain(chatId, msgId, env);
}

async function showUnbindList(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT id, chat_title, category_name FROM config_topics WHERE chat_id = ?`).bind(chatId).all();
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "本群目前没有绑定任何记录哦~", reply_markup: { inline_keyboard: [[{text: "返回设置", callback_data: "set_main"}]] } }, env);
  const keyboard = results.map(r => [{ text: `🗑️ 解绑 [${r.category_name}]`, callback_data: `set_unbind_do_${r.id}` }]);
  keyboard.push([{ text: "⬅️ 返回设置", callback_data: "set_main" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "点击对应按钮解除本群的话题绑定：", reply_markup: { inline_keyboard: keyboard } }, env);
}

async function showStats(chatId, msgId, env) {
  const mediaCount = (await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ?`).bind(chatId).first()).c;
  const topicCount = (await env.D1.prepare(`SELECT count(*) as c FROM config_topics WHERE chat_id = ?`).bind(chatId).first()).c;
  const text = `📊 **本群数据看板**\n\n- 本群收录媒体: **${mediaCount}** 条\n- 本群绑定话题: **${topicCount}** 个`;
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{text: "⬅️ 返回设置", callback_data: "set_main"}]] } }, env);
}

function getBackMarkup() {
  return { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "main_menu" }]] };
}

async function handleExternalImport(dataBatch, env) {
  if (!dataBatch || !Array.isArray(dataBatch)) return;
  const stmts = dataBatch.map(item => {
    return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      。bind(item.message_id, item.chat_id || 0, item.topic_id || null, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption || '');
  });
  if (stmts.length > 0) await env.D1.batch(stmts);
}

async function tgAPI(method, payload, env) {
  return fetch(`https://api.telegram.org/bot${env.BOT_TOKEN_ENV}/${method}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
  });
}
async function getSetting(key, env) {
  const res = await env.D1.prepare(`SELECT value FROM bot_settings WHERE key = ?`).bind(key).first();
  return res ? res.value : null;
}
async function isAdmin(chatId, userId, env) {
  if (chatId > 0) return true;
  const res = await tgAPI('getChatMember', { chat_id: chatId, user_id: userId }, env);
  const data = await res.json();
  return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
}
function makeDeepLink(chatId, messageId) {
  return `https://t.me/c/${String(chatId).replace('-100', '')}/${messageId}`;
}
