/**
 * Telegram Bot: Ziqing (话题化随机推荐 Bot)
 * Environment: Cloudflare Workers (Pages Compatible)
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 路由：注册 Webhook
    if (url.pathname === '/registerWebhook' && request.method === 'GET') {
      const webhookUrl = `${url.origin}/webhook`;
      const res = await tgApi(env, 'setWebhook', { url: webhookUrl });
      return new Response(res.ok ? "Webhook set successfully" : "Failed to set webhook", { status: 200 });
    }

    // 路由：注销 Webhook
    if (url.pathname === '/unRegisterWebhook' && request.method === 'GET') {
      const res = await tgApi(env, 'deleteWebhook', {});
      return new Response(res.ok ? "Webhook removed" : "Failed to remove webhook", { status: 200 });
    }

    // 路由：处理 Telegram 核心推送
    if (url.pathname === '/webhook' && request.method === 'POST') {
      try {
        const update = await request.json();
        // 使用 ctx.waitUntil 避免 Worker 超时并直接返回 200 给 TG 阻止重试
        ctx.waitUntil(handleUpdate(update, env).catch(err => console.error("Update Error:", err)));
      } catch (e) {
        console.error("JSON parse error:", e);
      }
      return new Response('OK', { status: 200 });
    }

    return new Response('Not Found', { status: 404 });
  }
};

/* ================== 核心处理逻辑 ================== */

async function handleUpdate(update, env) {
  if (update.message) {
    await handleMessage(update.message, env);
  } else if (update.callback_query) {
    await handleCallbackQuery(update.callback_query, env);
  }
}

async function handleMessage(msg, env) {
  const chatId = msg.chat.id;
  const threadId = msg.message_thread_id || 0;
  const text = msg.text || '';

  // 1. 初始化数据库指令 (仅限群组管理员或私聊)
  if (text === '/init_db') {
    await initDatabase(env);
    return tgApi(env, 'sendMessage', {
      chat_id: chatId,
      message_thread_id: threadId,
      text: "数据库初始化完成啦！籽青已经准备好工作了哦~ (≧∇≦)"
    });
  }

  // 2. 绑定专属输出话题
  if (text === '/bind_output') {
    await env.D1.prepare(
      `INSERT INTO config_topics (category_name, chat_id, topic_id) VALUES ('output', ?, ?) 
       ON CONFLICT(category_name) DO UPDATE SET chat_id=excluded.chat_id, topic_id=excluded.topic_id`
    ).bind(chatId, threadId).run();
    return tgApi(env, 'sendMessage', {
      chat_id: chatId, message_thread_id: threadId,
      text: "绑定成功！以后籽青会把推荐内容都发到这里来哦~ (๑•̀ㅂ•́)و✧"
    });
  }

  // 3. 动态分类绑定
  if (text.startsWith('/bind ')) {
    const categoryName = text.replace('/bind ', '').trim();
    if (!categoryName) return;
    await env.D1.prepare(
      `INSERT INTO config_topics (category_name, chat_id, topic_id) VALUES (?, ?, ?)
       ON CONFLICT(category_name) DO UPDATE SET chat_id=excluded.chat_id, topic_id=excluded.topic_id`
    ).bind(categoryName, chatId, threadId).run();
    return tgApi(env, 'sendMessage', {
      chat_id: chatId, message_thread_id: threadId,
      text: `分类【${categoryName}】绑定成功啦！大家发在这里的图文籽青都会乖乖记下来的~ QwQ`
    });
  }

  // 4. 用户主菜单
  if (text === '/start') {
    return sendMainMenu(env, chatId, threadId);
  }

  // 5. 媒体无感收录逻辑 (Data Indexing)
  if (msg.photo || msg.video || msg.document) {
    // 查询当前话题是否被绑定为某个分类 (排除 output)
    const topic = await env.D1.prepare(
      "SELECT category_name FROM config_topics WHERE chat_id = ? AND topic_id = ? AND category_name != 'output'"
    ).bind(chatId, threadId).first();

    if (topic) {
      await env.D1.prepare(
        "INSERT INTO media_library (chat_id, message_id, topic_id, category_name) VALUES (?, ?, ?, ?)"
      ).bind(chatId, msg.message_id, threadId, topic.category_name).run();
    }
  }
}

async function handleCallbackQuery(cb, env) {
  const data = cb.data;
  const chatId = cb.message.chat.id;
  const msgId = cb.message.message_id;
  const userId = cb.from.id;

  try {
    if (data === 'menu_main') {
      await editMainMenu(env, chatId, msgId);
    } else if (data === 'menu_random') {
      await showCategories(env, chatId, msgId);
    } else if (data === 'menu_top') {
      await showLeaderboard(env, chatId, msgId);
    } else if (data === 'menu_fav') {
      await showFavorites(env, chatId, msgId, userId);
    } else if (data === 'menu_settings') {
      await tgApi(env, 'answerCallbackQuery', {
        callback_query_id: cb.id,
        text: "设置功能还在努力开发中哦，籽青会加油哒！(ง •_•)ง",
        show_alert: true
      });
    } else if (data.startsWith('cat:')) {
      const category = data.substring(4);
      await serveRandomMedia(env, userId, category, false);
      await tgApi(env, 'answerCallbackQuery', { callback_query_id: cb.id });
    } else if (data.startsWith('nxt:')) {
      const category = data.substring(4);
      await serveRandomMedia(env, userId, category, true);
      await tgApi(env, 'answerCallbackQuery', { callback_query_id: cb.id });
    } else if (data.startsWith('fav:')) {
      const mediaId = parseInt(data.substring(4));
      await handleFavorite(env, cb.id, userId, mediaId);
    }
  } catch (err) {
    console.error("Callback Error:", err);
  }
}

/* ================== 业务功能函数 ================== */

async function sendMainMenu(env, chatId, threadId) {
  const text = "你好呀！我是籽青，很高兴为您服务~ (≧∇≦)\n请问今天想看点什么呢？";
  const replyMarkup = getMainMenuKeyboard();
  await tgApi(env, 'sendMessage', {
    chat_id: chatId,
    message_thread_id: threadId,
    text: text,
    reply_markup: replyMarkup
  });
}

async function editMainMenu(env, chatId, msgId) {
  const text = "你好呀！我是籽青，很高兴为您服务~ (≧∇≦)\n请问今天想看点什么呢？";
  await tgApi(env, 'editMessageText', {
    chat_id: chatId, message_id: msgId, text: text, reply_markup: getMainMenuKeyboard()
  });
}

function getMainMenuKeyboard() {
  return {
    inline_keyboard: [
      [{ text: "🎲 开始随机", callback_data: "menu_random" }, { text: "🏆 排行榜", callback_data: "menu_top" }],
      [{ text: "📁 收藏夹", callback_data: "menu_fav" }, { text: "⚙️ 设置", callback_data: "menu_settings" }]
    ]
  };
}

async function showCategories(env, chatId, msgId) {
  const { results } = await env.D1.prepare("SELECT category_name FROM config_topics WHERE category_name != 'output'").all();
  if (!results || results.length === 0) {
    await tgApi(env, 'editMessageText', {
      chat_id: chatId, message_id: msgId,
      text: "哎呀，管理员还没有绑定任何分类呢 (T_T)",
      reply_markup: { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "menu_main" }]] }
    });
    return;
  }

  const keyboard = results.map(row => ([{ text: `👉 ${row.category_name}`, callback_data: `cat:${row.category_name}` }]));
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "menu_main" }]);

  await tgApi(env, 'editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: "发现啦！请选择您感兴趣的分类哦：",
    reply_markup: { inline_keyboard: keyboard }
  });
}

async function serveRandomMedia(env, userId, category, isNext) {
  // 1. 检查 Output 话题
  const outputTopic = await env.D1.prepare("SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output'").first();
  if (!outputTopic) return;

  // 2. 30秒防刷机制处理
  const now = Math.floor(Date.now() / 1000);
  if (isNext) {
    const last = await env.D1.prepare("SELECT last_media_id, served_at FROM last_served WHERE user_id = ?").bind(userId).first();
    if (last && (now - last.served_at) < 30) {
      await env.D1.prepare("UPDATE media_library SET view_count = view_count - 1 WHERE id = ?").bind(last.last_media_id).run();
    }
  }

  // 3. 随机抽取媒体
  const media = await env.D1.prepare(
    "SELECT * FROM media_library WHERE category_name = ? ORDER BY RANDOM() LIMIT 1"
  ).bind(category).first();

  if (!media) return;

  // 4. 更新观看次数和最后服务记录
  await env.D1.prepare("UPDATE media_library SET view_count = view_count + 1 WHERE id = ?").bind(media.id).run();
  await env.D1.prepare(
    `INSERT INTO last_served (user_id, last_media_id, served_at) VALUES (?, ?, ?)
     ON CONFLICT(user_id) DO UPDATE SET last_media_id=excluded.last_media_id, served_at=excluded.served_at`
  ).bind(userId, media.id, now).run();

  // 5. 发送至 Output 话题
  await tgApi(env, 'copyMessage', {
    chat_id: outputTopic.chat_id,
    message_thread_id: outputTopic.topic_id,
    from_chat_id: media.chat_id,
    message_id: media.message_id,
    reply_markup: {
      inline_keyboard: [
        [{ text: "⏭️ 换一个", callback_data: `nxt:${category}` }],
        [{ text: "❤️ 收藏", callback_data: `fav:${media.id}` }, { text: "🏠 主菜单", callback_data: "menu_main" }]
      ]
    }
  });
}

async function showLeaderboard(env, chatId, msgId) {
  const { results } = await env.D1.prepare(
    "SELECT category_name, view_count, id FROM media_library ORDER BY view_count DESC LIMIT 5"
  ).all();

  let text = "当当当！这是目前的排行榜哦，大家的最爱都在这里啦 QwQ\n\n";
  if (!results || results.length === 0) {
    text += "目前还没有内容上榜呢~";
  } else {
    results.forEach((row, index) => {
      text += `${index + 1}. [${row.category_name}] 媒体标识: ${row.id} - 👀 ${row.view_count}次\n`;
    });
  }

  await tgApi(env, 'editMessageText', {
    chat_id: chatId, message_id: msgId, text: text,
    reply_markup: { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "menu_main" }]] }
  });
}

async function handleFavorite(env, cbId, userId, mediaId) {
  try {
    await env.D1.prepare("INSERT INTO user_favorites (user_id, media_id) VALUES (?, ?)").bind(userId, mediaId).run();
    await tgApi(env, 'answerCallbackQuery', {
      callback_query_id: cbId, text: "收藏成功啦！籽青已经帮您好好保存了哦~ (๑•̀ㅂ•́)و✧", show_alert: true
    });
  } catch (e) {
    // 捕获 UNIQUE 约束冲突
    await tgApi(env, 'answerCallbackQuery', {
      callback_query_id: cbId, text: "籽青发现您已经收藏过这个啦~", show_alert: true
    });
  }
}

async function showFavorites(env, chatId, msgId, userId) {
  const res = await env.D1.prepare("SELECT COUNT(*) as count FROM user_favorites WHERE user_id = ?").bind(userId).first();
  const count = res ? res.count : 0;
  
  await tgApi(env, 'editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: `您的专属收藏夹里目前有 ${count} 个宝贝哦！\n\n(查看详情功能籽青还在努力搭建中~ 敬请期待！)`,
    reply_markup: { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "menu_main" }]] }
  });
}

/* ================== 基础工具与数据库初始化 ================== */

async function tgApi(env, method, payload) {
  const url = `https://api.telegram.org/bot${env.BOT_TOKEN_ENV}/${method}`;
  const options = {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  };
  const res = await fetch(url, options);
  return res;
}

async function initDatabase(env) {
  const statements = [
    `CREATE TABLE IF NOT EXISTS config_topics (category_name TEXT PRIMARY KEY, chat_id INTEGER, topic_id INTEGER);`,
    `CREATE TABLE IF NOT EXISTS media_library (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, message_id INTEGER, topic_id INTEGER, category_name TEXT, view_count INTEGER DEFAULT 0, added_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
    `CREATE TABLE IF NOT EXISTS user_favorites (user_id INTEGER, media_id INTEGER, saved_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (user_id, media_id));`,
    `CREATE TABLE IF NOT EXISTS last_served (user_id INTEGER PRIMARY KEY, last_media_id INTEGER, served_at INTEGER);`
  ];
  const batch = statements.map(sql => env.D1.prepare(sql));
  await env.D1.batch(batch);
}
