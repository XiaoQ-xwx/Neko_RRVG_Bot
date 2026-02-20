/**
 * Cloudflare Workers (Pages) - Telegram Bot Entry Point (V2)
 * Topics Routing Fix, Admin Settings, Global Anti-Repeat, Dual Display Modes
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
      `CREATE TABLE IF NOT EXISTS media_library (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, chat_id INTEGER, topic_id INTEGER, category_name TEXT, view_count INTEGER DEFAULT 0, added_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS user_favorites (user_id INTEGER, media_id INTEGER, saved_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, media_id));`,
      `CREATE TABLE IF NOT EXISTS last_served (user_id INTEGER PRIMARY KEY, last_media_id INTEGER, served_at INTEGER);`,
      `CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT);`,
      `CREATE TABLE IF NOT EXISTS served_history (media_id INTEGER PRIMARY KEY);`,
      // 初始化默认全局设置
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('display_mode', 'B');`,
      `INSERT OR IGNORE INTO bot_settings (key, value) VALUES ('anti_repeat', 'true');`
    ];

    for (const sql of initSQL) {
      await env.D1.prepare(sql).run();
    }

    const webhookUrl = `${origin}/webhook`;
    const tgRes = await tgAPI('setWebhook', { url: webhookUrl }, env);
    if (!tgRes.ok) throw new Error('Webhook 注册失败');

    return new Response(`🎉 部署成功！数据库及全局设置已初始化，Webhook 已绑定至: ${webhookUrl}`, { headers: { 'Content-Type': 'text/plain;charset=UTF-8' } });
  } catch (error) {
    return new Response(`部署失败: ${error.message}`, { status: 500 });
  }
}

/* =========================================================================
 * 路由分发
 * ========================================================================= */
async function handleUpdate(update, env) {
  if (update.message) {
    await handleMessage(update.message, env);
  } else if (update.callback_query) {
    await handleCallback(update.callback_query, env);
  }
}

/* =========================================================================
 * 消息处理与收录
 * ========================================================================= */
async function handleMessage(message, env) {
  const text = message.text || message.caption || '';
  const chatId = message.chat.id;
  const topicId = message.message_thread_id || null;
  const userId = message.from.id;

  if (text.startsWith('/start')) {
    await sendMainMenu(chatId, topicId, env);
    return;
  }

  if (text.startsWith('/bind ')) {
    if (!(await isAdmin(chatId, userId, env))) {
       await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "呜呜，只有管理员才能绑定话题哦！QwQ" }, env);
       return;
    }
    const category = text.replace('/bind ', '').trim();
    if (!category) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, category, userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `管理员您好，籽青已经把当前话题和分类【${category}】绑定啦！(๑•̀ㅂ•́)و✧` }, env);
    return;
  }

  if (text.startsWith('/bind_output')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, 'output', userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `设置成功！这里将作为籽青的专属输出话题哦~ QwQ` }, env);
    return;
  }

  const hasMedia = message.photo || message.video || message.document || message.animation;
  if (hasMedia) {
    const query = await env.D1.prepare(`SELECT category_name FROM config_topics WHERE chat_id = ? AND (topic_id = ? OR topic_id IS NULL) AND category_name != 'output' LIMIT 1`).bind(chatId, topicId).first();
    if (query && query.category_name) {
      await env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name) VALUES (?, ?, ?, ?)`).bind(message.message_id, chatId, topicId, query.category_name).run();
    }
  }
}

/* =========================================================================
 * 回调交互 (UI)
 * ========================================================================= */
async function handleCallback(callback, env) {
  const data = callback.data;
  const userId = callback.from.id;
  const chatId = callback.message.chat.id;
  const msgId = callback.message.message_id;
  const topicId = callback.message.message_thread_id || null;
  const cbId = callback.id;

  if (data === 'main_menu') {
    await editMainMenu(chatId, msgId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } 
  else if (data === 'start_random') {
    await showCategories(chatId, msgId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  } 
  else if (data.startsWith('random_') || data.startsWith('next_')) {
    const isNext = data.startsWith('next_');
    const category = data.replace('random_', '').replace('next_', '');
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "正在为您抽取..." }, env);
    await sendRandomMedia(userId, chatId, msgId, topicId, category, isNext, env);
  } 
  else if (data.startsWith('fav_add_')) {
    const mediaId = parseInt(data.replace('fav_add_', ''));
    await handleAddFavorite(userId, cbId, mediaId, env);
  }
  else if (data === 'favorites') {
    await showFavoritesList(chatId, msgId, userId, 0, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
  else if (data.startsWith('fav_page_')) {
    const page = parseInt(data.replace('fav_page_', ''));
    await showFavoritesList(chatId, msgId, userId, page, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
  else if (data.startsWith('fav_view_')) {
    const mediaId = parseInt(data.replace('fav_view_', ''));
    await viewFavorite(chatId, topicId, mediaId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
  else if (data.startsWith('fav_del_')) {
    const mediaId = parseInt(data.replace('fav_del_', ''));
    await env.D1.prepare(`DELETE FROM user_favorites WHERE user_id = ? AND media_id = ?`).bind(userId, mediaId).run();
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已从收藏夹移除啦！" }, env);
    await showFavoritesList(chatId, msgId, userId, 0, env); // 刷新列表
  }
  else if (data === 'leaderboard') {
    await showLeaderboard(chatId, msgId, env);
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
  
  // 设置相关路由 (强鉴权)
  else if (data.startsWith('set_')) {
    if (!(await isAdmin(chatId, userId, env))) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "呜呜，只有群管理员才能调整设置哦！", show_alert: true }, env);
      return;
    }

    if (data === 'set_main') {
      await showSettingsMain(chatId, msgId, env);
    } else if (data === 'set_toggle_mode') {
      const current = await getSetting('display_mode', env);
      const next = current === 'A' ? 'B' : 'A';
      await env.D1.prepare(`UPDATE bot_settings SET value = ? WHERE key = 'display_mode'`).bind(next).run();
      await showSettingsMain(chatId, msgId, env);
    } else if (data === 'set_toggle_repeat') {
      const current = await getSetting('anti_repeat', env);
      const next = current === 'true' ? 'false' : 'true';
      await env.D1.prepare(`UPDATE bot_settings SET value = ? WHERE key = 'anti_repeat'`).bind(next).run();
      await showSettingsMain(chatId, msgId, env);
    } else if (data === 'set_stats') {
      await showStats(chatId, msgId, env);
    } else if (data === 'set_unbind_list') {
      await showUnbindList(chatId, msgId, env);
    } else if (data.startsWith('set_unbind_do_')) {
      const tId = parseInt(data.replace('set_unbind_do_', ''));
      await env.D1.prepare(`DELETE FROM config_topics WHERE id = ?`).bind(tId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "解绑成功！", show_alert: true }, env);
      await showUnbindList(chatId, msgId, env);
    }
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
  }
}

/* =========================================================================
 * 核心功能子模块
 * ========================================================================= */

// 主菜单
async function sendMainMenu(chatId, topicId, env) {
  const text = "你好呀！我是籽青 (≧∇≦)\n请问今天想看点什么呢？";
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text, reply_markup: getMainMenuMarkup() }, env);
}

async function editMainMenu(chatId, msgId, env) {
  const text = "这是籽青的主菜单哦 (≧∇≦) 请选择：";
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, reply_markup: getMainMenuMarkup() }, env);
}

function getMainMenuMarkup() {
  return {
    inline_keyboard: [
      [{ text: "🎲 开始随机", callback_data: "start_random" }],
      [{ text: "🏆 排行榜", callback_data: "leaderboard" }, { text: "📁 收藏夹", callback_data: "favorites" }],
      [{ text: "⚙️ 籽青设置 (限管理)", callback_data: "set_main" }]
    ]
  };
}

async function showCategories(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE category_name != 'output'`).all();
  if (!results || results.length === 0) {
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "当前还没有绑定任何分类呢 (QwQ) 管理员请使用 /bind 绑定哦！", reply_markup: getBackMarkup() }, env);
    return;
  }
  const keyboard = results.map(row => [{ text: `📂 ${row.category_name}`, callback_data: `random_${row.category_name}` }]);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "请选择您感兴趣的分类：", reply_markup: { inline_keyboard: keyboard } }, env);
}

// 核心随机分发逻辑
async function sendRandomMedia(userId, chatId, msgId, topicId, category, isNext, env) {
  const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' LIMIT 1`).first();
  if (!output) {
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `管理员还没设置输出话题呢，请用 /bind_output 设置一下哦！` }, env);
    return;
  }

  const mode = await getSetting('display_mode', env);
  const useAntiRepeat = (await getSetting('anti_repeat', env)) === 'true';
  const now = Date.now();

  // 30秒防刷机制与防重复回退
  if (isNext) {
    const last = await env.D1.prepare(`SELECT * FROM last_served WHERE user_id = ?`).bind(userId).first();
    if (last && (now - last.served_at) < 30000) {
      await env.D1.prepare(`UPDATE media_library SET view_count = MAX(0, view_count - 1) WHERE id = ?`).bind(last.last_media_id).run();
      if (useAntiRepeat) {
        await env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(last.last_media_id).run(); // 取消已读状态
      }
    }
  }

  // 抽取逻辑
  let media;
  if (useAntiRepeat) {
    media = await env.D1.prepare(`SELECT * FROM media_library WHERE category_name = ? AND id NOT IN (SELECT media_id FROM served_history) ORDER BY RANDOM() LIMIT 1`).bind(category).first();
    if (!media) {
      // 触发全部分发完毕重置逻辑
      const totalCheck = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE category_name = ?`).bind(category).first();
      if (totalCheck && totalCheck.c > 0) {
         await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE category_name = ?)`).bind(category).run();
         await tgAPI('sendMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, text: `🎉 大家太猛啦，【${category}】的内容全看光了！籽青已重置防重复记忆，开启新一轮~ QwQ` }, env);
         media = await env.D1.prepare(`SELECT * FROM media_library WHERE category_name = ? ORDER BY RANDOM() LIMIT 1`).bind(category).first();
      }
    }
  } else {
    media = await env.D1.prepare(`SELECT * FROM media_library WHERE category_name = ? ORDER BY RANDOM() LIMIT 1`).bind(category).first();
  }

  if (!media) {
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `呜呜，分类【${category}】里还没有内容呢~` }, env);
    return;
  }

  // 写入已读与统计更新
  if (useAntiRepeat) await env.D1.prepare(`INSERT OR IGNORE INTO served_history (media_id) VALUES (?)`).bind(media.id).run();
  await env.D1.prepare(`INSERT INTO last_served (user_id, last_media_id, served_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_media_id=excluded.last_media_id, served_at=excluded.served_at`).bind(userId, media.id, now).run();
  await env.D1.prepare(`UPDATE media_library SET view_count = view_count + 1 WHERE id = ?`).bind(media.id).run();

  // 消息生成与组装
  const deepLink = `https://t.me/c/${media.chat_id.toString().replace('-100', '')}/${media.message_id}`;
  const actionKeyboard = [
    [{ text: "⏭️ 换一个", callback_data: `next_${category}` }, { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` }]
  ];

  if (mode === 'A') {
    // 方案 A: 转发 + 附随菜单
    await tgAPI('forwardMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, from_chat_id: media.chat_id, message_id: media.message_id }, env);
    actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu" }]);
    await tgAPI('sendMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, text: "👆 您可以对上方的内容进行操作：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
  } else {
    // 方案 B: Copy + URL按钮跳转
    actionKeyboard.unshift([{ text: "🔗 跳转至原消息所在出处", url: deepLink }]);
    actionKeyboard.push([{ text: "🏠 主菜单", callback_data: "main_menu" }]);
    await tgAPI('copyMessage', { chat_id: output.chat_id, message_thread_id: output.topic_id, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
  }
}

// 收藏夹管理
async function handleAddFavorite(userId, cbId, mediaId, env) {
  try {
    await env.D1.prepare(`INSERT INTO user_favorites (user_id, media_id) VALUES (?, ?)`).bind(userId, mediaId).run();
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "收藏成功！籽青帮你记下来啦~ ❤️", show_alert: true }, env);
  } catch (e) {
    if (e.message.includes('UNIQUE constraint failed')) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "您已经收藏过这个啦~", show_alert: true }, env);
    }
  }
}

async function showFavoritesList(chatId, msgId, userId, page, env) {
  const limit = 5;
  const offset = page * limit;
  const { results } = await env.D1.prepare(`SELECT media_id FROM user_favorites WHERE user_id = ? ORDER BY saved_at DESC LIMIT ? OFFSET ?`).bind(userId, limit, offset).all();
  const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM user_favorites WHERE user_id = ?`).bind(userId).first();
  
  if (!results || results.length === 0) {
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "您的收藏夹空空如也哦~", reply_markup: getBackMarkup() }, env);
    return;
  }

  let text = `📁 **您的收藏夹** (共 ${totalRes.c} 条)\n\n下面是您珍藏的内容哦：`;
  const keyboard = [];
  
  for (let i = 0; i < results.length; i++) {
    const mId = results[i].media_id;
    keyboard.push([
      { text: `👁️ 查看收藏 #${offset + i + 1}`, callback_data: `fav_view_${mId}` },
      { text: `❌ 移除`, callback_data: `fav_del_${mId}` }
    ]);
  }

  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `fav_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `fav_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function viewFavorite(chatId, topicId, mediaId, env) {
  const media = await env.D1.prepare(`SELECT * FROM media_library WHERE id = ?`).bind(mediaId).first();
  if (media) {
    await tgAPI('copyMessage', { chat_id: chatId, message_thread_id: topicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
  }
}

// 设置与统计模块
async function showSettingsMain(chatId, msgId, env) {
  const mode = await getSetting('display_mode', env);
  const repeat = await getSetting('anti_repeat', env);
  
  const text = "⚙️ **籽青全局控制面板**\n\n仅管理员可用，请调整下方的功能开关：";
  const keyboard = [
    [{ text: `🔀 展现形式: ${mode === 'A' ? '方案A (原生转发)' : '方案B (Copy+URL链)'}`, callback_data: "set_toggle_mode" }],
    [{ text: `🔁 避开看过的: ${repeat === 'true' ? '✅ 已开启全局防重' : '❌ 未开启'}`, callback_data: "set_toggle_repeat" }],
    [{ text: "🗑️ 管理/解绑话题", callback_data: "set_unbind_list" }, { text: "📊 整体数据统计", callback_data: "set_stats" }],
    [{ text: "🏠 返回主菜单", callback_data: "main_menu" }]
  ];
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function showUnbindList(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT id, chat_title, category_name FROM config_topics`).all();
  if (!results || results.length === 0) {
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "目前没有绑定任何记录哦~", reply_markup: { inline_keyboard: [[{text: "返回设置", callback_data: "set_main"}]] } }, env);
    return;
  }
  const keyboard = results.map(r => [{ text: `🗑️ 解绑 [${r.category_name}] - ${r.chat_title}`, callback_data: `set_unbind_do_${r.id}` }]);
  keyboard.push([{ text: "⬅️ 返回设置", callback_data: "set_main" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "点击对应按钮解除话题绑定：", reply_markup: { inline_keyboard: keyboard } }, env);
}

async function showStats(chatId, msgId, env) {
  const mediaCount = (await env.D1.prepare(`SELECT count(*) as c FROM media_library`).first()).c;
  const topicCount = (await env.D1.prepare(`SELECT count(*) as c FROM config_topics`).first()).c;
  const favCount = (await env.D1.prepare(`SELECT count(*) as c FROM user_favorites`).first()).c;
  const text = `📊 **籽青的数据看板**\n\n- 总收录媒体数: **${mediaCount}** 条\n- 已绑定的话题/分类数: **${topicCount}** 个\n- 全局被收藏总次数: **${favCount}** 次\n\n*(大家都在努力创造内容呢 QwQ)*`;
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{text: "⬅️ 返回设置", callback_data: "set_main"}]] } }, env);
}

async function showLeaderboard(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT category_name, view_count FROM media_library ORDER BY view_count DESC LIMIT 5`).all();
  let text = "🏆 **籽青统计的排行榜 Top 5**\n\n";
  if (!results || results.length === 0) text += "当前还没有数据呢~";
  else results.forEach((row, idx) => { text += `${idx + 1}. 分类 [${row.category_name}] 的某记录 - 浏览: ${row.view_count}\n`; });
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: getBackMarkup() }, env);
}

function getBackMarkup() {
  return { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "main_menu" }]] };
}

/* =========================================================================
 * 工具与 API 封装
 * ========================================================================= */
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
  if (chatId > 0) return true; // 私聊默认有权限
  const res = await tgAPI('getChatMember', { chat_id: chatId, user_id: userId }, env);
  const data = await res.json();
  return data.ok && (data.result.status === 'administrator' || data.result.status === 'creator');
}
