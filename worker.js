/**
 * Cloudflare Workers (Pages) - Telegram Bot Entry Point (V5.9)
 * 核心升级：新增随机抽取过滤器（每用户独立）：媒体类型/收录时间/视频时长三维度。
 * 数据库：新增 user_filters 表，media_library 新增 duration 列。
 */

/* =========================================================================
 * 模块级常量与缓存（Cloudflare Worker 实例级别,跨请求共享）
 * ========================================================================= */

// 🌟 V5.9: 随机抽取过滤器默认值
const FILTER_DEFAULTS = Object.freeze({
  media_type:    'all',   // all | photo | video | animation
  date_mode:     'all',   // all | today | d7 | d30 | year | custom
  date_from:     '',      // YYYY-MM-DD（仅 date_mode=custom 有效）
  date_to:       '',      // YYYY-MM-DD（仅 date_mode=custom 有效）
  duration_mode: 'all',   // all | s30 | s60 | s120 | s300 | custom
  duration_max:  ''       // 整数秒字符串（仅 duration_mode=custom 有效）
});
const FILTER_DATE_RE = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/;
const FILTER_MEDIA_TYPES     = new Set(['all', 'photo', 'video', 'animation']);
const FILTER_DATE_MODES      = new Set(['all', 'today', 'd7', 'd30', 'year', 'custom']);
const FILTER_DURATION_MODES  = new Set(['all', 's30', 's60', 's120', 's300', 'custom']);
const FILTER_DURATION_PRESET_MAP = Object.freeze({ s30: 30, s60: 60, s120: 120, s300: 300 });

const SETTING_DEFAULTS = Object.freeze({
  display_mode: 'B',
  anti_repeat: 'true',
  auto_jump: 'true',
  dup_notify: 'false',
  show_success: 'true',
  next_mode: 'replace',
  strict_skip: 'false' // 🌟 默认不是严格模式（放回池子）
});

// 成员资格 TTL 缓存（60秒）
const GROUP_MEMBER_CACHE_TTL_MS = 60_000;
const GROUP_MEMBER_CACHE_MAX = 4096;
const groupMembershipCache = new Map();

let isInstanceAwake = false;

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);

      // Webhook 路由：最高优先级，绝不被冷启动阻塞
      // 先返回 200 给 Telegram，防止退避机制触发
      if (request.method === 'POST' && url.pathname === '/webhook') {
        const update = await request.json();
        // 冷启动时把 setWebhook 注册丢到后台，不阻塞本次响应
        if (!isInstanceAwake) {
          isInstanceAwake = true;
          ctx.waitUntil((async () => {
            try {
              const origin = new URL(request.url).origin;
              await fetch(`https://api.telegram.org/bot${env.BOT_TOKEN_ENV}/setWebhook`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ url: `${origin}/webhook` })
              });
            } catch (e) { console.error("后台 Webhook 注册失败:", e.message); }
          })());
        }
        ctx.waitUntil(handleUpdate(update, env, ctx));
        return new Response('OK', { status: 200 });
      }

      // 非 Webhook 路由的冷启动初始化（GET / 等场景可以阻塞等待）
      if (!isInstanceAwake) {
        try {
          await env.D1.prepare(`SELECT 1`).first();
          const currentUrl = new URL(request.url).origin;
          await fetchWithRetry(`https://api.telegram.org/bot${env.BOT_TOKEN_ENV}/setWebhook`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url: `${currentUrl}/webhook` })
          }, 3, 1000);
          console.log("🛡️ 满级复活甲触发：已稳稳地向 TG 重新报到喵！");
        } catch (e) {
          console.error("复活彻底失败（重试耗尽）:", e.message);
        }
        isInstanceAwake = true;
      }

      if (request.method === 'GET' && url.pathname === '/') {
        return await handleSetup(url.origin, env);
      }
      
      // Telegram Web App 的专属前端网页入口
      if (request.method === 'GET' && url.pathname === '/webapp') {
        return new Response(getWebAppHTML(), { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
      }
      
      if (request.method === 'POST' && url.pathname === '/api/webapp/data') {
        return await handleWebAppData(request, env);
      }
      
      if (request.method === 'POST' && url.pathname === '/api/webapp/remove_fav') {
        return await handleWebAppRemoveFav(request, env);
      }

      if (request.method === 'POST' && url.pathname === '/api/webapp/remove_hist') {
        return await handleWebAppRemoveHist(request, env);
      }

      if (request.method === 'POST' && url.pathname === '/api/import') {
        const secret = request.headers.get('Authorization');
        if (env.ADMIN_SECRET && secret !== env.ADMIN_SECRET) return new Response('Unauthorized', { status: 401 });
        const payload = await request.json();
        ctx.waitUntil(handleExternalImport(payload.data, env));
        return new Response(JSON.stringify({ status: 'success', count: payload.data.length }), { status: 200 });
      }

      // 🌟 V5.9: 批量更新视频时长 API
      if (request.method === 'POST' && url.pathname === '/api/update_duration') {
        const secret = request.headers.get('Authorization');
        if (env.ADMIN_SECRET && secret !== env.ADMIN_SECRET) return new Response('Unauthorized', { status: 401 });
        const payload = await request.json();
        const updates = payload.updates || [];
        if (!Array.isArray(updates) || updates.length === 0) {
          return new Response(JSON.stringify({ status: 'error', message: 'No updates provided' }), { status: 400 });
        }
        let updatedCount = 0;
        // 每批 50 条
        for (let i = 0; i < updates.length; i += 50) {
          const batch = updates.slice(i, i + 50);
          const stmts = batch.map(item => {
            return env.D1.prepare(
              `UPDATE media_library SET duration = ? WHERE message_id = ? AND chat_id = ? AND duration IS NULL`
            ).bind(item.duration, item.message_id, item.chat_id);
          });
          const results = await env.D1.batch(stmts);
          updatedCount += results.filter(r => r.meta?.changes > 0).length;
        }
        return new Response(JSON.stringify({ status: 'success', updated: updatedCount }), { status: 200 });
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
      `CREATE TABLE IF NOT EXISTS media_library (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, chat_id INTEGER, topic_id INTEGER, category_name TEXT, view_count INTEGER DEFAULT 0, file_unique_id TEXT, file_id TEXT, media_type TEXT, caption TEXT, duration INTEGER DEFAULT NULL, added_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS user_favorites (user_id INTEGER, media_id INTEGER, saved_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, media_id));`,
      `CREATE TABLE IF NOT EXISTS last_served (user_id INTEGER PRIMARY KEY, last_media_id INTEGER, served_at INTEGER);`,
      `CREATE TABLE IF NOT EXISTS served_history (media_id INTEGER PRIMARY KEY);`,
      `CREATE TABLE IF NOT EXISTS chat_settings (chat_id INTEGER, key TEXT, value TEXT, PRIMARY KEY(chat_id, key));`,
      `CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT);`,
      // 🌟 V5.9: 用户过滤器表
      `CREATE TABLE IF NOT EXISTS user_filters (user_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT, PRIMARY KEY(user_id, chat_id, key));`,

      `CREATE INDEX IF NOT EXISTS idx_media_chat_cat_id ON media_library (chat_id, category_name, id);`,
      `CREATE INDEX IF NOT EXISTS idx_media_chat_viewcount ON media_library (chat_id, view_count DESC);`,
      `CREATE INDEX IF NOT EXISTS idx_topics_chat_cat ON config_topics (chat_id, category_name);`,
      `CREATE INDEX IF NOT EXISTS idx_served_history_media ON served_history (media_id);`,
      // 🌟 V5.9: 过滤器相关索引（不依赖 duration 列的索引）
      `CREATE INDEX IF NOT EXISTS idx_user_filters_chat_user ON user_filters (chat_id, user_id);`,
      `CREATE INDEX IF NOT EXISTS idx_media_chat_cat_added ON media_library (chat_id, category_name, added_at DESC);`,
      // 注意：idx_media_chat_cat_duration 索引移至列迁移之后创建
      
      `CREATE TABLE IF NOT EXISTS user_history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, chat_id INTEGER, media_id INTEGER, viewed_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      `CREATE TABLE IF NOT EXISTS group_history (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, media_id INTEGER, viewed_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
      
      // 🌟 新增：用户花名册
      `CREATE TABLE IF NOT EXISTS user_roster (user_id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,

      // 触发器：维持历史记录在50条
      `CREATE TRIGGER IF NOT EXISTS limit_user_history AFTER INSERT ON user_history BEGIN DELETE FROM user_history WHERE id NOT IN (SELECT id FROM user_history WHERE user_id = NEW.user_id ORDER BY viewed_at DESC LIMIT 50) AND user_id = NEW.user_id; END;`,
      `CREATE TRIGGER IF NOT EXISTS limit_group_history AFTER INSERT ON group_history BEGIN DELETE FROM group_history WHERE id NOT IN (SELECT id FROM group_history WHERE chat_id = NEW.chat_id ORDER BY viewed_at DESC LIMIT 50) AND chat_id = NEW.chat_id; END;`,

      `CREATE INDEX IF NOT EXISTS idx_user_history_user_viewed ON user_history (user_id, viewed_at DESC);`,
      `CREATE INDEX IF NOT EXISTS idx_user_history_user_chat ON user_history (user_id, chat_id, viewed_at DESC);`,
      `CREATE INDEX IF NOT EXISTS idx_group_history_chat_viewed ON group_history (chat_id, viewed_at DESC);`,

      // 🌟 V5.7: 批量操作会话表
      `CREATE TABLE IF NOT EXISTS batch_sessions (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, user_id INTEGER, mode TEXT, collected_ids TEXT DEFAULT '[]', collected_msg_ids TEXT DEFAULT '[]', created_at TEXT DEFAULT (datetime('now')));`,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_session_user ON batch_sessions (chat_id, user_id);`
    ];

    for (const sql of initSQL) await env.D1.prepare(sql).run();

    // 🌟 V5.9: 幂等列迁移（PRAGMA 检查 + try/catch 双保险）
    const migrateColumns = [
      { name: 'file_unique_id', type: 'TEXT' },
      { name: 'file_id',        type: 'TEXT' },
      { name: 'media_type',     type: 'TEXT' },
      { name: 'caption',        type: 'TEXT' },
      { name: 'duration',       type: 'INTEGER DEFAULT NULL' }
    ];
    let existingCols = new Set();
    try {
      const pragma = await env.D1.prepare(`PRAGMA table_info(media_library)`).all();
      existingCols = new Set((pragma.results || []).map(r => String(r.name || '').toLowerCase()));
    } catch (e) {
      console.warn('PRAGMA 读取失败，回退至 try/catch 模式:', e?.message);
    }
    for (const col of migrateColumns) {
      if (existingCols.has(col.name.toLowerCase())) continue;
      try {
        await env.D1.prepare(`ALTER TABLE media_library ADD COLUMN ${col.name} ${col.type};`).run();
      } catch (e) {
        const msg = String(e?.message || '');
        if (!/duplicate column|already exists/i.test(msg)) console.error(`列迁移失败: ${col.name}`, msg);
      }
    }

    // 🌟 V5.9: duration 列相关索引（必须在列迁移之后创建）
    try {
      await env.D1.prepare(`CREATE INDEX IF NOT EXISTS idx_media_chat_cat_duration ON media_library (chat_id, category_name, duration);`).run();
    } catch (e) {
      // 索引已存在或其他非致命错误，静默忽略
      console.warn('duration 索引创建跳过:', e?.message);
    }

    const webhookUrl = `${origin}/webhook`;
    const tgRes = await tgAPI('setWebhook', { url: webhookUrl }, env);
    if (!tgRes.ok) throw new Error('Webhook 注册失败');

    // 绝美的成功页面
    const html = `
      <!DOCTYPE html>
      <html lang="zh-CN">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>籽青 (Ziqing) - 核心控制枢纽 🐾</title>
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;700&display=swap');
          body { font-family: 'Noto Sans SC', system-ui, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: linear-gradient(135deg, #fdfbfb 0%, #ebedee 100%); overflow: hidden; color: #4a4a4a; }
          .blob-1 { position: absolute; top: -10%; left: -10%; width: 400px; height: 400px; background: rgba(255, 182, 193, 0.4); border-radius: 50%; filter: blur(60px); z-index: 0; }
          .blob-2 { position: absolute; bottom: -10%; right: -10%; width: 350px; height: 350px; background: rgba(161, 196, 253, 0.4); border-radius: 50%; filter: blur(60px); z-index: 0; }
          .glass-card { background: rgba(255, 255, 255, 0.7); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border: 1px solid rgba(255, 255, 255, 0.8); padding: 3rem 3rem 2.5rem; border-radius: 28px; box-shadow: 0 20px 40px rgba(0,0,0,0.08), inset 0 0 0 1px rgba(255,255,255,0.5); text-align: center; max-width: 480px; width: 90%; position: relative; z-index: 1; animation: slideUp 0.6s cubic-bezier(0.16, 1, 0.3, 1); }
          @keyframes slideUp { from { transform: translateY(40px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
          .avatar { font-size: 4.5rem; margin-top: -5.5rem; margin-bottom: 1rem; display: inline-block; background: white; border-radius: 50%; padding: 10px; box-shadow: 0 10px 20px rgba(255, 117, 140, 0.2); animation: float 3s infinite ease-in-out; }
          @keyframes float { 0%, 100% { transform: translateY(0); } 50% { transform: translateY(-10px); } }
          h1 { background: linear-gradient(135deg, #ff758c 0%, #ff7eb3 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.8rem; font-size: 1.8rem; font-weight: 700; }
          p { line-height: 1.6; font-size: 0.95rem; margin-bottom: 1.5rem; }
          .code-box { background: rgba(255, 255, 255, 0.9); padding: 1rem; border-radius: 12px; border: 1px dashed #ffb6c1; font-family: 'Courier New', monospace; word-break: break-all; color: #ff0844; font-weight: bold; font-size: 0.9rem; box-shadow: inset 0 2px 5px rgba(0,0,0,0.03); transition: all 0.3s ease; }
          .code-box:hover { border-color: #ff758c; transform: scale(1.02); }
          .highlight { color: #ff7eb3; font-weight: bold; }
          .footer { margin-top: 2rem; font-size: 0.8rem; color: #a0aabf; font-weight: 600; letter-spacing: 1px;}
        </style>
      </head>
      <body>
        <div class="blob-1"></div>
        <div class="blob-2"></div>
        <div class="glass-card">
          <div class="avatar">🐱</div>
          <h1>🎉 籽青 V5.9 满血上线！</h1>
          <p>随机抽取过滤器已就绪！媒体类型/时间/时长三维度筛选喵～<br>Webhook 已经帮主人狠狠地绑死啦：</p>
          <div class="code-box">${webhookUrl}</div>
          <p style="margin-top: 1.5rem;">快去 Telegram 里找 <span class="highlight">籽青</span> 玩耍吧！QwQ</p>
          <div class="footer">Powered by Cloudflare Workers & D1</div>
        </div>
      </body>
      </html>
    `;
    return new Response(html, { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });

  } catch (error) {
    console.error('部署失败喵:', error);
    
    const errorHtml = `
      <!DOCTYPE html>
      <html lang="zh-CN">
      <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>摔倒了喵！</title>
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;700&display=swap');
          body { font-family: 'Noto Sans SC', system-ui, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; background: linear-gradient(135deg, #fdfbfb 0%, #ebedee 100%); overflow: hidden; color: #4a4a4a; }
          .blob-1 { position: absolute; top: -10%; left: -10%; width: 400px; height: 400px; background: rgba(255, 99, 132, 0.3); border-radius: 50%; filter: blur(60px); z-index: 0; }
          .blob-2 { position: absolute; bottom: -10%; right: -10%; width: 350px; height: 350px; background: rgba(155, 89, 182, 0.3); border-radius: 50%; filter: blur(60px); z-index: 0; }
          .glass-card { background: rgba(255, 255, 255, 0.7); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border: 1px solid rgba(255, 255, 255, 0.8); padding: 3rem 3rem 2.5rem; border-radius: 28px; box-shadow: 0 20px 40px rgba(255, 0, 0, 0.05), inset 0 0 0 1px rgba(255,255,255,0.5); text-align: center; max-width: 480px; width: 90%; position: relative; z-index: 1; animation: shake 0.6s cubic-bezier(.36,.07,.19,.97) both; }
          @keyframes shake { 10%, 90% { transform: translate3d(-1px, 0, 0); } 20%, 80% { transform: translate3d(2px, 0, 0); } 30%, 50%, 70% { transform: translate3d(-4px, 0, 0); } 40%, 60% { transform: translate3d(4px, 0, 0); } }
          .avatar { font-size: 4.5rem; margin-top: -5.5rem; margin-bottom: 1rem; display: inline-block; background: white; border-radius: 50%; padding: 10px; box-shadow: 0 10px 20px rgba(255, 99, 132, 0.2); animation: float 3s infinite ease-in-out; }
          @keyframes float { 0%, 100% { transform: translateY(0); } 50% { transform: translateY(-10px); } }
          h1 { background: linear-gradient(135deg, #ff416c 0%, #ff4b2b 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.8rem; font-size: 1.8rem; font-weight: 700; }
          p { line-height: 1.6; font-size: 0.95rem; margin-bottom: 1.5rem; }
          .code-box { background: rgba(255, 240, 245, 0.9); padding: 1rem; border-radius: 12px; border: 1px dashed #ff416c; font-family: 'Courier New', monospace; word-break: break-all; color: #d32f2f; font-weight: bold; font-size: 0.9rem; box-shadow: inset 0 2px 5px rgba(255,0,0,0.05); transition: all 0.3s ease; }
          .code-box:hover { border-color: #ff4b2b; transform: scale(1.02); }
          .highlight { color: #ff4b2b; font-weight: bold; }
          .footer { margin-top: 2rem; font-size: 0.8rem; color: #a0aabf; font-weight: 600; letter-spacing: 1px;}
        </style>
      </head>
      <body>
        <div class="blob-1"></div>
        <div class="blob-2"></div>
        <div class="glass-card">
          <div class="avatar">😿</div>
          <h1>呜呜,摔倒了喵...</h1>
          <p>部署过程中出现了一点小意外！<br>请主人检查一下 <span class="highlight">D1 数据库绑定</span> 或者 <span class="highlight">BOT_TOKEN</span> 哦：</p>
          <div class="code-box">${error.message}</div>
          <p style="margin-top: 1.5rem;">修好之后再刷新一下这个页面就可以啦！QwQ</p>
          <div class="footer">Powered by Cloudflare Workers & D1</div>
        </div>
      </body>
      </html>
    `;
    return new Response(errorHtml, { status: 500, headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
  }
}

/* =========================================================================
 * 路由与消息处理
 * ========================================================================= */
async function handleUpdate(update, env, ctx) {
  // 🌟 V5.7: 异步清理过期批量会话（5分钟超时）
  ctx.waitUntil(
    env.D1.prepare(`DELETE FROM batch_sessions WHERE datetime(created_at, '+5 minutes') < datetime('now')`).run().catch(() => {})
  );

  // 🌟 记录花名册
  const fromUser = update.message?.from || update.callback_query?.from;
  if (fromUser) {
    ctx.waitUntil(
      env.D1.prepare(`INSERT INTO user_roster (user_id, first_name, last_name) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET first_name=excluded.first_name, last_name=excluded.last_name, updated_at=CURRENT_TIMESTAMP`)
      .bind(fromUser.id, fromUser.first_name || '', fromUser.last_name || '').run().catch(() => {})
    );
  }

  if (update.message) {
    await handleMessage(update.message, env, ctx);
  } else if (update.callback_query) {
    await handleCallback(update.callback_query, env, ctx);
  }
}

async function handleMessage(message, env, ctx) {
  const text = message.text || message.caption || '';
  const chatId = message.chat.id;
  const topicId = message.message_thread_id || null;
  const userId = message.from.id;

  if (text.startsWith('/start')) return sendMainMenu(chatId, topicId, env, userId);

  if (text.startsWith('/help')) {
    const helpText = `📖 **籽青的说明书喵~ (≧∇≦)**\n/start - 唤出籽青的主菜单\n\n**【管理员专属指令喵】**\n/bind <分类名> - 将当前话题绑定为采集库\n/bind_output - 将当前话题设为专属推送展示窗口\n/import_json - 获取关于导入历史消息的说明\n\n**【快捷管理魔法】**\n直接回复某张图片/视频：\n发送 \`/d\` - 彻底抹除它\n发送 \`/mv\` - 将它转移到其他分类\n发送 \`/list\` - 查看它的收录信息\n\n**【批量操作】**\n\`/d <数量|all>\` - 批量删除当前分类最近N条\n\`/mv <数量|all> <分类名>\` - 批量转移\n\`/bd\` - 进入精确批量删除模式（转发选择）\n\`/bmv\` - 进入精确批量转移模式（转发选择）`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: helpText, parse_mode: 'Markdown' }, env);
    return;
  }

  if (text.startsWith('/import_json')) {
    const importHelp = `📥 **关于导入历史数据喵**\n\n籽青有两种方法可以吃掉历史数据哦：\n\n1. **直接投喂 (适合 5MB 以内的小包裹)**：直接把 \`.json\` 文件发给籽青,并在文件的说明(Caption)里写上 \`/import 分类名\` 即可！\n2. **脚本投喂 (适合大包裹)**：在电脑上运行配套的 Python 导入脚本,慢慢喂给籽青！QwQ`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: importHelp, parse_mode: 'Markdown' }, env);
    return;
  }

  // 🌟 V5.7: /bd 批量删除会话模式（必须在 /bind 之前，精确匹配）
  if (text === '/bd' || text === '/bd@' + (env.BOT_USERNAME || '')) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 只有管理员才能使用批量模式哦！" }, env);
    }
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
    await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode) VALUES (?, ?, 'bd')`).bind(chatId, userId).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🗑️ 已进入**批量删除模式**喵！\n\n请把要删除的媒体转发给籽青～\n每收到一条籽青会确认收集。\n\n完成后发送 `/bd end` 确认删除\n取消请发送 `/bd cancel`\n⏰ 5分钟后自动过期", parse_mode: 'Markdown' }, env);
  }

  if (text === '/bd end') {
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bd'`).bind(chatId, userId).first();
    if (!session) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "喵？你还没有进入批量删除模式哦～" }, env);
    if (Date.now() - new Date(session.created_at + 'Z').getTime() > 300000) {
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "⏰ 会话已超时，请重新发送 /bd 开始喵～" }, env);
    }
    const ids = JSON.parse(session.collected_ids || '[]');
    if (ids.length === 0) {
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "还没有收集到任何媒体呢，批量模式已退出喵～" }, env);
    }
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `📋 已收集 ${ids.length} 条媒体记录，确认全部删除吗喵？`, reply_markup: { inline_keyboard: [[{ text: "✅ 确认删除", callback_data: "bs_cfm_d" }, { text: "❌ 取消", callback_data: "bs_cancel" }]] } }, env);
  }

  if (text === '/bd cancel') {
    const session = await env.D1.prepare(`SELECT id FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bd'`).bind(chatId, userId).first();
    if (!session) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "当前没有进行中的批量删除操作喵～" }, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "已退出批量删除模式喵～" }, env);
  }

  // 🌟 V5.7: /bmv 批量转移会话模式
  if (text === '/bmv' || text === '/bmv@' + (env.BOT_USERNAME || '')) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 只有管理员才能使用批量模式哦！" }, env);
    }
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
    await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode) VALUES (?, ?, 'bmv')`).bind(chatId, userId).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🔀 已进入**批量转移模式**喵！\n\n请把要转移的媒体转发给籽青～\n\n完成后发送 `/bmv end` 选择目标分类\n取消请发送 `/bmv cancel`\n⏰ 5分钟后自动过期", parse_mode: 'Markdown' }, env);
  }

  if (text === '/bmv end') {
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bmv'`).bind(chatId, userId).first();
    if (!session) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "喵？你还没有进入批量转移模式哦～" }, env);
    if (Date.now() - new Date(session.created_at + 'Z').getTime() > 300000) {
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "⏰ 会话已超时，请重新发送 /bmv 开始喵～" }, env);
    }
    const ids = JSON.parse(session.collected_ids || '[]');
    if (ids.length === 0) {
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "还没有收集到任何媒体呢，批量模式已退出喵～" }, env);
    }
    const { results } = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE chat_id = ? AND category_name != 'output'`).bind(chatId).all();
    if (!results || results.length === 0) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "本群还没绑定其他分类呢喵~" }, env);
    }
    const keyboard = results.map(r => [{ text: `🔀 转移至: ${r.category_name}`, callback_data: `bs_mv_${r.category_name}` }]);
    keyboard.push([{ text: "❌ 取消", callback_data: "bs_cancel" }]);
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `📋 已收集 ${ids.length} 条媒体记录，请选择目标分类喵：`, reply_markup: { inline_keyboard: keyboard } }, env);
  }

  if (text === '/bmv cancel') {
    const session = await env.D1.prepare(`SELECT id FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bmv'`).bind(chatId, userId).first();
    if (!session) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "当前没有进行中的批量转移操作喵～" }, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "已退出批量转移模式喵～" }, env);
  }

  // 🌟 V5.8: /list — 查询回复媒体的收录记录（所有成员可用）
  if (message.reply_to_message && text.startsWith('/list')) {
    const info = extractMediaInfo(message.reply_to_message);
    if (!info.fileUniqueId) {
      return tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id,
        text: "喵？这不是一条媒体消息哦，请回复一张图片或视频再试试！"
      }, env);
    }

    // 🌟 V5.9: 增强查询，加入 duration 和 view_count
    const { results: mediaRecords } = await env.D1.prepare(
      `SELECT id, message_id, topic_id, category_name, media_type, duration, view_count, added_at FROM media_library WHERE file_unique_id = ? AND chat_id = ? ORDER BY added_at ASC`
    ).bind(info.fileUniqueId, chatId).all();

    if (!mediaRecords || mediaRecords.length === 0) {
      return tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id,
        text: "呜呜，籽青在库里找不到这个媒体的收录记录喵，可能从未被收录过哦～"
      }, env);
    }

    const uniqueTopicIds = [...new Set(mediaRecords.map(r => r.topic_id).filter(t => t != null))];
    const topicNameMap = {};
    if (uniqueTopicIds.length > 0) {
      const ph = uniqueTopicIds.map(() => '?').join(',');
      const { results: topicRows } = await env.D1.prepare(
        `SELECT topic_id, category_name FROM config_topics WHERE chat_id = ? AND topic_id IN (${ph}) AND category_name != 'output' LIMIT 50`
      ).bind(chatId, ...uniqueTopicIds).all();
      for (const row of (topicRows || [])) topicNameMap[row.topic_id] = row.category_name;
    }

    // 🌟 V5.9: 从原消息提取文件大小
    const replyMsg = message.reply_to_message;
    let fileSize = null;
    if (replyMsg.video?.file_size) fileSize = replyMsg.video.file_size;
    else if (replyMsg.animation?.file_size) fileSize = replyMsg.animation.file_size;
    else if (replyMsg.document?.file_size) fileSize = replyMsg.document.file_size;
    else if (replyMsg.photo?.length > 0) fileSize = replyMsg.photo[replyMsg.photo.length - 1].file_size;

    // 格式化文件大小
    const formatSize = (bytes) => {
      if (!bytes) return null;
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
      if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
      return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
    };

    // 格式化时长
    const formatDuration = (sec) => {
      if (sec == null) return null;
      const m = Math.floor(sec / 60);
      const s = sec % 60;
      return m > 0 ? `${m}分${s}秒` : `${s}秒`;
    };

    const chatIdNum = String(chatId).replace(/^-100/, '');
    const typeLabel = { photo: '🖼️ 图片', video: '🎬 视频', animation: '🎠 GIF', document: '📄 文件' };

    // 🌟 V5.9: 构建媒体概要信息（从第一条记录和原消息获取）
    const firstRec = mediaRecords[0];
    const mediaType = typeLabel[firstRec.media_type] || firstRec.media_type || '未知';
    const sizePart = fileSize ? `📦 大小：${formatSize(fileSize)}` : '';
    const durationPart = firstRec.duration != null ? `⏱ 时长：${formatDuration(firstRec.duration)}` : '';
    const totalViews = mediaRecords.reduce((sum, r) => sum + (r.view_count || 0), 0);

    let summaryLine = `📊 *媒体概要*\n${mediaType}`;
    if (sizePart) summaryLine += ` | ${sizePart}`;
    if (durationPart) summaryLine += ` | ${durationPart}`;
    summaryLine += `\n👁 总浏览：${totalViews} 次`;

    const lines = mediaRecords.map((rec, idx) => {
      const topicBound = rec.topic_id ? (topicNameMap[rec.topic_id] || '未知话题') : '无话题';
      const addedAt = rec.added_at ? String(rec.added_at).replace('T', ' ').substring(0, 16) : '未知时间';
      const link = rec.message_id
        ? (rec.topic_id
            ? `https://t.me/c/${chatIdNum}/${rec.topic_id}/${rec.message_id}`
            : `https://t.me/c/${chatIdNum}/${rec.message_id}`)
        : null;
      const linkPart = link ? ` [📎](${link})` : '';
      const viewPart = rec.view_count > 0 ? ` | 👁 ${rec.view_count}` : '';
      return `*${idx + 1}.* \`${rec.category_name}\` → ${topicBound}${viewPart}\n　　${addedAt}${linkPart}`;
    });

    return tgAPI('sendMessage', {
      chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.reply_to_message.message_id,
      text: `🔍 *籽青找到了 ${mediaRecords.length} 条收录记录喵～*\n\n${summaryLine}\n\n━━━━━━━━━━━━━━━━\n📋 *收录详情*\n${lines.join('\n')}`,
      parse_mode: 'Markdown', disable_web_page_preview: true
    }, env);
  }

  // 🌟 快捷回复管理魔法 (/d 和 /mv) — 单条回复模式
  // 排除批量格式：/d <数字|all> 和 /mv <数字|all> <分类>，让它们落到后面的批量路由
  const isBatchDFormat = /^\/d\s+(all|\d+)$/.test(text);
  const isBatchMvFormat = /^\/mv\s+(all|\d+)\s+.+$/.test(text);
  if (message.reply_to_message && (text.startsWith('/d') || text.startsWith('/mv')) && !isBatchDFormat && !isBatchMvFormat) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "🚨 呜呜，只有管理员主人才可以使用回复魔法哦！" }, env);
    }

    const info = extractMediaInfo(message.reply_to_message);
    if (!info.fileUniqueId) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "喵？这似乎不是一个标准的图片或视频记录哦！" }, env);
    }

    const media = await env.D1.prepare(`SELECT id, category_name FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(info.fileUniqueId, chatId).first();
    if (!media) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "呜呜，籽青在数据库里找不到它的真身，可能早就被删除了喵~" }, env);
    }

    if (text.startsWith('/d')) {
      await env.D1.batch([
        env.D1.prepare(`DELETE FROM media_library WHERE id = ?`).bind(media.id),
        env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(media.id),
        env.D1.prepare(`DELETE FROM user_favorites WHERE media_id = ?`).bind(media.id),
        env.D1.prepare(`DELETE FROM user_history WHERE media_id = ?`).bind(media.id),
        env.D1.prepare(`DELETE FROM group_history WHERE media_id = ?`).bind(media.id)
      ]);
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.reply_to_message.message_id, text: "🗑️ 抹除成功！这个媒体已经被籽青彻底销毁啦喵！" }, env);
    }

    if (text.startsWith('/mv')) {
      const { results } = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE chat_id = ? AND category_name != 'output'`).bind(chatId).all();
      if (!results || results.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "本群还没绑定其他分类呢喵~" }, env);
      }
      
      const keyboard = results.map(r => [{ text: `🔀 转移至: ${r.category_name}`, callback_data: `mvcat_${media.id}|${r.category_name}` }]);
      keyboard.push([{ text: "❌ 取消操作", callback_data: "cancel_action" }]);
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.reply_to_message.message_id, text: `请选择要把这个记录转移到哪个分类喵：\n(当前分类: ${media.category_name})`, reply_markup: { inline_keyboard: keyboard } }, env);
    }
  }

  // 🌟 V5.7: 模式A — /d <N|all> 按数量批量删除（无 reply 时触发）
  if (!message.reply_to_message && /^\/d\s+(all|\d+)$/.test(text)) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 只有管理员才能使用批量删除哦！" }, env);
    }
    const topicCat = await env.D1.prepare(`SELECT category_name FROM config_topics WHERE chat_id = ? AND topic_id = ? AND category_name != 'output' LIMIT 1`).bind(chatId, topicId).first();
    if (!topicCat) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "当前话题没有绑定分类喵，无法批量操作～" }, env);
    const category = topicCat.category_name;
    const arg = text.split(/\s+/)[1];
    const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ? AND category_name = ?`).bind(chatId, category).first();
    const total = totalRes?.c || 0;
    if (total === 0) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `【${category}】分类下没有任何记录喵～` }, env);
    const count = arg === 'all' ? total : Math.min(parseInt(arg), total);
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `⚠️ 即将删除【${category}】分类的 ${count} 条记录${arg === 'all' ? '（全部）' : '（最近）'}，确认吗喵？`, reply_markup: { inline_keyboard: [[{ text: "✅ 确认删除", callback_data: `bdc_${count}` }, { text: "❌ 取消", callback_data: "cancel_action" }]] } }, env);
  }

  // 🌟 V5.7: 模式A — /mv <N|all> <分类名> 按数量批量转移（无 reply 时触发）
  if (!message.reply_to_message && /^\/mv\s+(all|\d+)\s+.+$/.test(text)) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 只有管理员才能使用批量转移哦！" }, env);
    }
    const topicCat = await env.D1.prepare(`SELECT category_name FROM config_topics WHERE chat_id = ? AND topic_id = ? AND category_name != 'output' LIMIT 1`).bind(chatId, topicId).first();
    if (!topicCat) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "当前话题没有绑定分类喵，无法批量操作～" }, env);
    const category = topicCat.category_name;
    const parts = text.split(/\s+/);
    const arg = parts[1];
    const targetCategory = parts.slice(2).join(' ');
    // 验证目标分类存在
    const targetExists = await env.D1.prepare(`SELECT 1 FROM config_topics WHERE chat_id = ? AND category_name = ? LIMIT 1`).bind(chatId, targetCategory).first();
    if (!targetExists) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `找不到【${targetCategory}】分类喵，请检查名称～` }, env);
    if (targetCategory === category) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "源分类和目标分类相同喵，不需要转移～" }, env);
    const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ? AND category_name = ?`).bind(chatId, category).first();
    const total = totalRes?.c || 0;
    if (total === 0) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `【${category}】分类下没有任何记录喵～` }, env);
    const count = arg === 'all' ? total : Math.min(parseInt(arg), total);
    // 将目标分类暂存到 batch_sessions，回调时读取
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
    await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, ?, ?)`).bind(chatId, userId, `bmv_quick:${targetCategory}`, JSON.stringify({ count, category })).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `⚠️ 即将把【${category}】的 ${count} 条记录${arg === 'all' ? '（全部）' : '（最近）'}转移到【${targetCategory}】，确认吗喵？`, reply_markup: { inline_keyboard: [[{ text: "✅ 确认转移", callback_data: `bmc_cfm` }, { text: "❌ 取消", callback_data: "cancel_action" }]] } }, env);
  }

  if (text.startsWith('/bind ')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    const category = text.replace('/bind ', '').trim();
    if (!category) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, category, userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `绑定成功喵！籽青已将当前话题与分类【${category}】绑定啦！(๑•̀ㅂ•́)و✧` }, env);
    return;
  }

  if (text.startsWith('/bind_output')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, 'output', userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `设置成功喵！籽青以后就在这里发图啦~ QwQ` }, env);
    return;
  }

  // ==== 完整恢复的内置 JSON 解析功能 ====
  if (message.document && message.document.file_name && message.document.file_name.endsWith('.json') && text.startsWith('/import ')) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🚨 呜呜,只有管理员主人才可以给籽青投喂文件哦！` }, env);
    }
    
    const category = text.replace('/import ', '').trim();
    if (!category) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `喵？请在文件说明里写上正确格式,比如：\`/import 分类名\` 哦！` }, env);

    if (message.document.file_size > 5242880) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🚨 呜呜... 这个包裹太大了（超过 5MB）,籽青的肚子装不下会撑爆的！请使用 Python 脚本进行外部导入喵 QwQ` }, env);
    }

    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `📥 收到包裹！籽青正在努力吃掉这个文件,请稍等喵...` }, env);

    try {
      const fileRes = await tgAPI('getFile', { file_id: message.document.file_id }, env);
      const fileData = await fileRes.json();
      if (!fileData.ok) throw new Error("无法从 TG 服务器拉取文件");
      const downloadUrl = `https://api.telegram.org/file/bot${env.BOT_TOKEN_ENV}/${fileData.result.file_path}`;

      const jsonRes = await fetch(downloadUrl);
      const jsonData = await jsonRes.json();
      const messages = jsonData.messages || [];
      
      let validMedia = [];
      for (const msg of messages) {
        if (msg.type !== 'message') continue;
        let mediaType = null;
        if (msg.photo) mediaType = 'photo';
        else if (msg.media_type === 'video_file') mediaType = 'video';
        else if (msg.media_type === 'animation') mediaType = 'animation';
        else if (msg.media_type) mediaType = 'document';

        if (!mediaType) continue;

        let caption = "";
        if (Array.isArray(msg.text)) {
          caption = msg.text.map(t => typeof t === 'string' ? t : (t.text || '')).join('');
        } else if (typeof msg.text === 'string') {
          caption = msg.text;
        }

        validMedia.push({
          message_id: msg.id,
          chat_id: chatId,
          topic_id: null,
          category_name: category,
          file_unique_id: `import_${chatId}_${msg.id}`,
          file_id: '',
          media_type: mediaType,
          caption: caption.substring(0, 100),
          duration: Number.isInteger(msg.duration_seconds) ? msg.duration_seconds : (Number.isInteger(msg.duration) ? msg.duration : null)
        });
      }

      if (validMedia.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `❓ 哎呀,籽青在这个文件里没有找到任何图片或视频记录喵。` }, env);
      }

      let successCount = 0;
      for (let i = 0; i < validMedia.length; i += 50) {
        const batch = validMedia.slice(i, i + 50);
        const stmts = batch.map(item => {
          return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
            .bind(item.message_id, item.chat_id, item.topic_id, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption, item.duration ?? null);
        });
        await env.D1.batch(stmts);
        successCount += batch.length;
      }

      await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🎉 嗝~ 吃饱啦！成功从文件里导入了 ${successCount} 条【${category}】的记录喵！` }, env);
    } catch (err) {
      await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `❌ 呜呜,籽青吃坏肚子了,导入失败喵：${err.message}` }, env);
    }
    return; 
  }

  // 🌟 V5.9: 过滤器文本输入会话捕获（在批量收录拦截器之前）
  if (message.text && typeof message.text === 'string' && message.text.trim()) {
    const filterSession = await env.D1.prepare(
      `SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode IN ('filter_date_custom', 'filter_dur_input') LIMIT 1`
    ).bind(chatId, userId).first();

    if (filterSession) {
      const input = message.text.trim();

      // 超时检查（5分钟）
      if (Date.now() - new Date(filterSession.created_at + 'Z').getTime() > 300000) {
        await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run();
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "⏰ 筛选输入会话已超时喵，请重新打开筛选器设置～" }, env);
      }

      // 取消操作
      if (input === '/cancel' || input === '取消') {
        await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run();
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "✅ 已取消筛选输入喵～" }, env);
      }

      let payload = {};
      try { payload = JSON.parse(filterSession.collected_ids || '{}'); } catch (e) { payload = {}; }
      const sourceChatId = typeof payload.sourceChatId === 'number' ? payload.sourceChatId : chatId;

      if (filterSession.mode === 'filter_dur_input') {
        // 时长：仅允许非负整数
        if (!/^(0|[1-9]\d*)$/.test(input)) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请输入非负整数秒数（如 30、120、0），或发送 /cancel 取消喵～"
          }, env);
        }
        const maxSec = parseInt(input, 10);
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'duration_mode', 'custom', env),
          upsertUserFilter(userId, sourceChatId, 'duration_max', String(maxSec), env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时长筛选已设置：0~${maxSec} 秒内的视频喵～`, env);
      }

      if (filterSession.mode === 'filter_date_custom') {
        // 日期：YYYY-MM-DD YYYY-MM-DD（空格分隔）
        const parts = input.split(/\s+/).filter(Boolean);
        if (parts.length !== 2 || !FILTER_DATE_RE.test(parts[0]) || !FILTER_DATE_RE.test(parts[1])) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请按 `YYYY-MM-DD YYYY-MM-DD` 格式输入（空格分隔），或发送 /cancel 取消喵～",
            parse_mode: 'Markdown'
          }, env);
        }
        const [fromDate, toDate] = parts;
        if (fromDate > toDate) {
          return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "⚠️ 起始日期不能晚于结束日期喵，请重新输入～" }, env);
        }
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'date_mode', 'custom', env),
          upsertUserFilter(userId, sourceChatId, 'date_from', fromDate, env),
          upsertUserFilter(userId, sourceChatId, 'date_to', toDate, env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时间筛选已设置：${fromDate} ~ ${toDate}喵～`, env);
      }
    }
  }

  // 🌟 V5.7: 批量会话媒体收集拦截器（在日常收录之前）
  let mediaInfo = extractMediaInfo(message);
  if (mediaInfo.fileUniqueId) {
    const batchSession = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode IN ('bd', 'bmv')`).bind(chatId, userId).first();
    if (batchSession) {
      // 检查超时（5分钟）
      if (Date.now() - new Date(batchSession.created_at + 'Z').getTime() > 300000) {
        await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(batchSession.id).run();
        await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "⏰ 批量会话已超时，本条媒体将正常收录喵～" }, env);
        // 不 return，继续走正常收录
      } else {
        // 收集模式：匹配数据库
        const dbMedia = await env.D1.prepare(`SELECT id FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(mediaInfo.fileUniqueId, chatId).first();
        if (!dbMedia) {
          return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "⚠️ 该媒体不在数据库中，已跳过喵～" }, env);
        }
        // 原子操作：用 SQL json 函数在数据库层面追加，避免并发竞态覆盖
        // 先检查去重（在 SQL 层面用 INSTR 检查）
        const existing = await env.D1.prepare(
          `SELECT INSTR(collected_ids, ?) as found FROM batch_sessions WHERE id = ?`
        ).bind(`${dbMedia.id}`, batchSession.id).first();
        if (existing && existing.found > 0) {
          // 静默跳过重复，不回复
          return;
        }
        // 原子追加：用 json_insert + json_array_length 在 SQL 层面追加元素
        await env.D1.prepare(
          `UPDATE batch_sessions SET collected_ids = json_insert(collected_ids, '$[#]', ?), collected_msg_ids = json_insert(collected_msg_ids, '$[#]', ?) WHERE id = ?`
        ).bind(dbMedia.id, message.message_id, batchSession.id).run();
        // 查询最新计数
        const updated = await env.D1.prepare(`SELECT json_array_length(collected_ids) as cnt FROM batch_sessions WHERE id = ?`).bind(batchSession.id).first();
        const cnt = updated?.cnt || 1;
        // 静默收集：只在每 5 条和第 1 条时回复，减少刷屏
        if (cnt === 1 || cnt % 5 === 0) {
          const modeText = batchSession.mode === 'bd' ? '/bd end' : '/bmv end';
          await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `📦 已收集 ${cnt} 条，完成后发送 ${modeText} 喵～` }, env);
        }
        return;
      }
    }
  }

  // ==== 日常媒体收录拦截 (恢复 dup_notify 逻辑) ====
  if (mediaInfo.fileUniqueId) {
    const query = await env.D1.prepare(`SELECT category_name FROM config_topics WHERE chat_id = ? AND (topic_id = ? OR topic_id IS NULL) AND category_name != 'output' LIMIT 1`).bind(chatId, topicId).first();
    if (query && query.category_name) {
      const existing = await env.D1.prepare(`SELECT id, duration FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(mediaInfo.fileUniqueId, chatId).first();
      if (existing) {
        // 🌟 V5.9: 渐进式补全 duration（如果原记录没有，从新消息中获取）
        if (existing.duration === null && mediaInfo.duration !== null) {
          ctx.waitUntil(
            env.D1.prepare(`UPDATE media_library SET duration = ? WHERE id = ?`).bind(mediaInfo.duration, existing.id).run()
          );
        }
        const notify = await getSetting(chatId, 'dup_notify', env);
        if (notify === 'true') {
          await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "哎呀,籽青发现这个内容之前已经收录过啦喵~" }, env);
        }
        return;
      }
      await env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .bind(message.message_id, chatId, topicId, query.category_name, mediaInfo.fileUniqueId, mediaInfo.fileId, mediaInfo.type, message.caption || '', mediaInfo.duration ?? null).run();
    }
  }
}

function extractMediaInfo(message) {
  let info = { fileUniqueId: null, fileId: null, type: null, duration: null };
  if (message.photo && message.photo.length > 0) {
    const p = message.photo[message.photo.length - 1];
    info = { fileUniqueId: p.file_unique_id, fileId: p.file_id, type: 'photo', duration: null };
  } else if (message.video) {
    info = { fileUniqueId: message.video.file_unique_id, fileId: message.video.file_id, type: 'video',
             duration: Number.isInteger(message.video.duration) ? message.video.duration : null };
  } else if (message.document) {
    info = { fileUniqueId: message.document.file_unique_id, fileId: message.document.file_id, type: 'document', duration: null };
  } else if (message.animation) {
    info = { fileUniqueId: message.animation.file_unique_id, fileId: message.animation.file_id, type: 'animation',
             duration: Number.isInteger(message.animation.duration) ? message.animation.duration : null };
  }
  return info;
}

/* =========================================================================
 * 回调交互处理
 * ========================================================================= */
async function handleCallback(callback, env, ctx) {
  const data = callback.data;
  const userId = callback.from.id;
  const chatId = callback.message.chat.id;
  const msgId = callback.message.message_id;
  const topicId = callback.message.message_thread_id || null;
  const cbId = callback.id;

  if (data === 'main_menu') {
    await Promise.all([
      editMainMenu(chatId, msgId, env, userId),
      tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env)
    ]);
  } else if (data === 'main_menu_new') {
    await Promise.all([
      sendMainMenu(chatId, topicId, env, userId),
      tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env)
    ]);
  } else if (data === 'start_random') {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await showCategories(chatId, msgId, env, userId);
  } 

  // 🌟 处理历史回退
  else if (data.startsWith('prev_')) {
    const params = data.replace('prev_', '').split('|');
    await sendHistoricalMedia(userId, chatId, msgId, topicId, params[0], parseInt(params[1]), parseInt(params[2]), env, cbId);
  }

  else if (data.startsWith('random_') || data.startsWith('next_')) {
    const action = data.startsWith('random_') ? 'random_' : 'next_';
    const params = data.replace(action, '').split('|');
    const category = params[0];
    const sourceChatId = params.length > 1 ? parseInt(params[1]) : chatId;

    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "籽青正在为你抽取喵..." }, env);
    await sendRandomMedia(userId, chatId, msgId, topicId, category, sourceChatId, action === 'next_', env, ctx, cbId);
  }

  // 🌟 分类转移指令处理
  else if (data.startsWith('mvcat_')) {
    if (!(await isAdmin(chatId, userId, env))) return;
    const params = data.replace('mvcat_', '').split('|');
    await env.D1.prepare(`UPDATE media_library SET category_name = ? WHERE id = ?`).bind(params[1], parseInt(params[0])).run();
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "空间转移成功喵！" }, env);
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `✅ 转移成功！该记录已安全转移到【${params[1]}】分类下喵~` }, env);
  } else if (data === 'cancel_action') {
    await tgAPI('deleteMessage', { chat_id: chatId, message_id: msgId }, env);
  }

  // 🌟 V5.7: 批量操作回调处理
  else if (data.startsWith('bdc_')) {
    // 模式A: 按数量批量删除确认
    if (!(await isAdmin(chatId, userId, env))) return;
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "正在批量删除喵..." }, env);
    const count = parseInt(data.replace('bdc_', ''));
    // 从消息文本中提取分类名（格式：即将删除【分类名】）
    const msgText = callback.message.text || '';
    const catMatch = msgText.match(/【(.+?)】/);
    if (!catMatch) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "❌ 无法识别分类信息，请重新操作喵～" }, env);
    const category = catMatch[1];
    const beforeRes = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ? AND category_name = ?`).bind(chatId, category).first();
    const before = beforeRes?.c || 0;
    const { results } = await env.D1.prepare(`SELECT id FROM media_library WHERE chat_id = ? AND category_name = ? ORDER BY id DESC LIMIT ?`).bind(chatId, category, count).all();
    if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "该分类已经没有记录了喵～" }, env);
    const deleted = await batchDeleteMediaByIds(results.map(r => r.id), env);
    const after = before - deleted;
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `🗑️ 批量删除完成！已从【${category}】中抹除 ${deleted} 条记录喵！\n📊 ${before} 条 → ${after} 条` }, env);
  }

  else if (data === 'bmc_cfm') {
    // 模式A: 按数量批量转移确认
    if (!(await isAdmin(chatId, userId, env))) return;
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "正在批量转移喵..." }, env);
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode LIKE 'bmv_quick:%'`).bind(chatId, userId).first();
    if (!session) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "❌ 会话已过期，请重新操作喵～" }, env);
    const targetCategory = session.mode.replace('bmv_quick:', '');
    const { count, category } = JSON.parse(session.collected_ids);
    const { results } = await env.D1.prepare(`SELECT id FROM media_library WHERE chat_id = ? AND category_name = ? ORDER BY id DESC LIMIT ?`).bind(chatId, category, count).all();
    if (!results || results.length === 0) {
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
      return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "该分类已经没有记录了喵～" }, env);
    }
    const moved = await batchMoveMediaByIds(results.map(r => r.id), targetCategory, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `✅ 批量转移完成！已将 ${moved} 条记录从【${category}】转移到【${targetCategory}】喵！` }, env);
  }

  else if (data === 'bs_cfm_d') {
    // 模式B: 会话批量删除确认
    if (!(await isAdmin(chatId, userId, env))) return;
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "正在批量删除喵..." }, env);
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bd'`).bind(chatId, userId).first();
    if (!session) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "❌ 会话已过期，请重新操作喵～" }, env);
    const ids = JSON.parse(session.collected_ids || '[]');
    const beforeRes = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ?`).bind(chatId).first();
    const before = beforeRes?.c || 0;
    const deleted = await batchDeleteMediaByIds(ids, env);
    const after = before - deleted;
    // 保留 session 用于清理转发消息，改 mode 为 cleanup
    await env.D1.prepare(`UPDATE batch_sessions SET mode = 'cleanup' WHERE id = ?`).bind(session.id).run();
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `🗑️ 批量删除完成！已抹除 ${deleted} 条记录喵！\n📊 本群媒体总量: ${before} 条 → ${after} 条\n\n是否同时删除刚才转发的那些消息？`, reply_markup: { inline_keyboard: [[{ text: "🧹 是，清理掉", callback_data: "bs_clean_yes" }, { text: "📌 不用了", callback_data: "bs_clean_no" }]] } }, env);
  }

  else if (data.startsWith('bs_mv_')) {
    // 模式B: 会话批量转移 — 选择目标分类
    if (!(await isAdmin(chatId, userId, env))) return;
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "正在批量转移喵..." }, env);
    const targetCategory = data.replace('bs_mv_', '');
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bmv'`).bind(chatId, userId).first();
    if (!session) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "❌ 会话已过期，请重新操作喵～" }, env);
    const ids = JSON.parse(session.collected_ids || '[]');
    const moved = await batchMoveMediaByIds(ids, targetCategory, env);
    // 保留 session 用于清理转发消息
    await env.D1.prepare(`UPDATE batch_sessions SET mode = 'cleanup' WHERE id = ?`).bind(session.id).run();
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `✅ 批量转移完成！已将 ${moved} 条记录转移到【${targetCategory}】喵！\n\n是否同时删除刚才转发的那些消息？`, reply_markup: { inline_keyboard: [[{ text: "🧹 是，清理掉", callback_data: "bs_clean_yes" }, { text: "📌 不用了", callback_data: "bs_clean_no" }]] } }, env);
  }

  else if (data === 'bs_clean_yes') {
    // 清理转发消息
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'cleanup'`).bind(chatId, userId).first();
    if (session) {
      const msgIds = JSON.parse(session.collected_msg_ids || '[]');
      for (const mid of msgIds) {
        await tgAPI('deleteMessage', { chat_id: chatId, message_id: mid }, env).catch(() => {});
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
    }
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🧹 转发的消息已清理完毕，操作全部完成喵！" }, env);
  }

  else if (data === 'bs_clean_no') {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'cleanup'`).bind(chatId, userId).run();
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "✅ 操作全部完成喵！转发的消息已保留。" }, env);
  }

  else if (data === 'bs_cancel') {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "已取消批量操作喵～" }, env);
  }

  else if (data.startsWith('fav_add_')) {
    await handleAddFavorite(userId, cbId, parseInt(data.replace('fav_add_', '')), env);
  } else if (data === 'favorites' || data.startsWith('fav_page_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    const page = data === 'favorites' ? 0 : parseInt(data.replace('fav_page_', ''));
    await showFavoritesList(chatId, msgId, userId, page, env);
  } else if (data.startsWith('fav_view_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await viewFavorite(chatId, topicId, parseInt(data.replace('fav_view_', '')), env);
  } else if (data.startsWith('fav_del_')) {
    await env.D1.prepare(`DELETE FROM user_favorites WHERE user_id = ? AND media_id = ?`).bind(userId, parseInt(data.replace('fav_del_', ''))).run();
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已从收藏夹移除喵！" }, env);
    await showFavoritesList(chatId, msgId, userId, 0, env);
  }
  
  else if (data === 'history' || data.startsWith('hist_page_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    const page = data === 'history' ? 0 : parseInt(data.replace('hist_page_', ''));
    await showHistoryList(chatId, msgId, userId, page, env);
  } else if (data.startsWith('hist_view_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    await viewFavorite(chatId, topicId, parseInt(data.replace('hist_view_', '')), env);
  } else if (data.startsWith('hist_del_')) {
    const parts = data.replace('hist_del_', '').split('_'); 
    const type = parts[0];
    const recordId = parseInt(parts[1]);
    
    if (type === 'u') {
      await env.D1.prepare(`DELETE FROM user_history WHERE id = ? AND user_id = ?`).bind(recordId, userId).run();
    } else {
      await env.D1.prepare(`DELETE FROM group_history WHERE id = ? AND chat_id = ?`).bind(recordId, chatId).run();
    }
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "唰！足迹已经抹除啦喵！" }, env);
    await showHistoryList(chatId, msgId, userId, 0, env);
  }

  else if (data === 'leaderboard' || data.startsWith('leader_page_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    const page = data === 'leaderboard' ? 0 : parseInt(data.replace('leader_page_', ''));
    await showLeaderboard(chatId, msgId, page, env);
  }

  else if (data.startsWith('set_')) {
    if (chatId > 0) return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵！只能在群组内使用设置面板哦！", show_alert: true }, env);
    if (!(await isAdmin(chatId, userId, env))) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "呜呜,只有管理员才能调整籽青哦！", show_alert: true }, env);
      return;
    }

    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);

    if (data === 'set_main') await showSettingsMain(chatId, msgId, env);
    else if (data === 'set_toggle_mode') await toggleSetting('display_mode', env, chatId, msgId, ['A', 'B']);
    else if (data === 'set_toggle_repeat') await toggleSetting('anti_repeat', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_jump') await toggleSetting('auto_jump', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_dup') await toggleSetting('dup_notify', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_success') await toggleSetting('show_success', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_toggle_nextmode') await toggleSetting('next_mode', env, chatId, msgId, ['replace', 'new']);
    else if (data === 'set_toggle_strict') await toggleSetting('strict_skip', env, chatId, msgId, ['true', 'false']);
    else if (data === 'set_stats') await showStats(chatId, msgId, env);
    else if (data === 'set_unbind_list') await showUnbindList(chatId, msgId, env);
    else if (data.startsWith('set_unbind_do_')) {
      await env.D1.prepare(`DELETE FROM config_topics WHERE id = ? AND chat_id = ?`).bind(parseInt(data.replace('set_unbind_do_', '')), chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "解绑成功喵！", show_alert: true }, env);
      await showUnbindList(chatId, msgId, env);
    }

    else if (data === 'set_danger_zone') {
      const text = "⚠️ **危险操作区**\n\n这里的操作仅对当前群组生效,且不可逆喵！";
      const keyboard = [[{ text: "🧨 清空本群数据统计", callback_data: "set_clear_stats_1" }], [{ text: "🚨 彻底清空本群媒体库", callback_data: "set_clear_media_1" }], [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
    }
    else if (data === 'set_clear_stats_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "⚠️ 确定仅清空本群统计数据吗喵？", reply_markup: { inline_keyboard: [[{ text: "🔴 确认清空 (第1次)", callback_data: "set_clear_stats_2" }], [{ text: "⬅️ 返回", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_stats_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🧨 **最后警告**：即将清空本群浏览量喵！", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "☠️ 彻底清空！", callback_data: "set_clear_stats_do" }], [{ text: "⬅️ 算了", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_stats_do') {
      await env.D1.prepare(`UPDATE media_library SET view_count = 0 WHERE chat_id = ?`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM group_history WHERE chat_id = ?`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM user_history WHERE chat_id = ?`).bind(chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前群组统计重置完毕喵！", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
    else if (data === 'set_clear_media_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🚨 **高危警告**\n\n即将清空【本群收录的所有媒体】喵！", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "🩸 我确定要删除本群全部媒体", callback_data: "set_clear_media_2" }], [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_media_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🌋 **最终警告**\n\n一旦按下无法恢复喵！真的要清空吗？", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "💥 毁天灭地！", callback_data: "set_clear_media_do" }], [{ text: "⬅️ 放弃操作", callback_data: "set_main" }]] } }, env);
    } else if (data === 'set_clear_media_do') {
      await env.D1.prepare(`DELETE FROM user_favorites WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM media_library WHERE chat_id = ?`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM group_history WHERE chat_id = ?`).bind(chatId).run();
      await env.D1.prepare(`DELETE FROM user_history WHERE chat_id = ?`).bind(chatId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前群组媒体库已被彻底清空喵！", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
  }

  // 🌟 V5.9: 过滤器回调路由
  else if (data === 'filter_open') {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    if (chatId > 0) {
      return tgAPI('sendMessage', { chat_id: chatId, text: "🔍 请在群组中打开筛选器设置喵！（在群组内点击主菜单的筛选器按钮）" }, env);
    }
    await showFilterPanel(userId, chatId, msgId, chatId, env);
  }

  else if (data.startsWith('filter_')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    // 解析 action|sourceChatId 格式
    const pipeIdx = data.indexOf('|');
    if (pipeIdx === -1) return; // 格式不对，静默忽略
    const action = data.substring(0, pipeIdx);         // e.g. "filter_media"
    const sc = parseInt(data.substring(pipeIdx + 1));   // sourceChatId
    if (!sc || isNaN(sc)) return;

    // —— 过滤器主面板 ——
    if (action === 'filter_panel') {
      await showFilterPanel(userId, chatId, msgId, sc, env);
    }

    // —— 媒体类型循环切换 ——
    else if (action === 'filter_media') {
      const f = await getUserFiltersBatch(userId, sc, env);
      const nextType = FILTER_MEDIA_CYCLE[f.media_type] || 'all';
      await upsertUserFilter(userId, sc, 'media_type', nextType, env);
      await showFilterPanel(userId, chatId, msgId, sc, env);
    }

    // —— 时间子面板 ——
    else if (action === 'filter_time_panel') {
      await showFilterTimePanel(userId, chatId, msgId, sc, env);
    }

    // —— 时间预设设置 ——
    else if (['filter_time_all','filter_time_today','filter_time_d7','filter_time_d30','filter_time_year'].includes(action)) {
      const val = action.replace('filter_time_', '');
      // 防抖：当前已是该值，静默提示
      const fCur = await getUserFiltersBatch(userId, sc, env);
      if (fCur.date_mode === val && fCur.date_from === '' && fCur.date_to === '') {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前已是该选项喵～", show_alert: false }, env);
      }
      await Promise.all([
        upsertUserFilter(userId, sc, 'date_mode', val, env),
        upsertUserFilter(userId, sc, 'date_from', '', env),
        upsertUserFilter(userId, sc, 'date_to',   '', env)
      ]);
      await showFilterTimePanel(userId, chatId, msgId, sc, env);
    }

    // —— 自定义时间段（ForceReply）——
    else if (action === 'filter_time_custom') {
      // 检查是否有冲突的批量会话
      const conflictSession = await env.D1.prepare(
        `SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`
      ).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "请先结束当前的批量操作会话，再设置筛选器喵～" }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, 'filter_date_custom', ?)`).bind(chatId, userId, JSON.stringify({ sourceChatId: sc })).run();
      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: "📅 **设置自定义收录时间**\n请回复本条消息输入起止日期喵～\n\n📌 格式：`YYYY-MM-DD YYYY-MM-DD`（空格分隔）\n💡 示例：`2024-01-01 2024-12-31`\n\n发送 /cancel 取消",
        parse_mode: 'Markdown',
        reply_markup: { force_reply: true, selective: true }
      }, env);
    }

    // —— 时长子面板 ——
    else if (action === 'filter_dur_panel') {
      await showFilterDurPanel(userId, chatId, msgId, sc, env);
    }

    // —— 时长预设设置 ——
    else if (['filter_dur_all','filter_dur_s30','filter_dur_s60','filter_dur_s120','filter_dur_s300'].includes(action)) {
      const val = action.replace('filter_dur_', '');
      // 防抖：当前已是该值，静默提示
      const fCur = await getUserFiltersBatch(userId, sc, env);
      if (fCur.duration_mode === val && fCur.duration_max === '') {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前已是该选项喵～", show_alert: false }, env);
      }
      await Promise.all([
        upsertUserFilter(userId, sc, 'duration_mode', val, env),
        upsertUserFilter(userId, sc, 'duration_max',  '', env)
      ]);
      await showFilterDurPanel(userId, chatId, msgId, sc, env);
    }

    // —— 自定义时长（ForceReply）——
    else if (action === 'filter_dur_custom') {
      const conflictSession = await env.D1.prepare(
        `SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`
      ).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "请先结束当前的批量操作会话，再设置筛选器喵～" }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, 'filter_dur_input', ?)`).bind(chatId, userId, JSON.stringify({ sourceChatId: sc })).run();
      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: "⏱ **设置自定义视频时长**\n请回复本条消息输入最大秒数喵～\n\n📌 示例：`30` 表示仅抽取 0~30 秒内的视频\n\n发送 /cancel 取消",
        parse_mode: 'Markdown',
        reply_markup: { force_reply: true, selective: true }
      }, env);
    }

    // —— 重置所有过滤器 ——
    else if (action === 'filter_reset') {
      await resetUserFilters(userId, sc, env);
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "✅ 筛选器已重置！", show_alert: false }, env);
      await showFilterPanel(userId, chatId, msgId, sc, env);
    }
  }
}

/* =========================================================================
 * UI 流转逻辑
 * ========================================================================= */
async function sendMainMenu(chatId, topicId, env, userId) {
  if (chatId > 0) {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length === 0) {
      await tgAPI('sendMessage', { chat_id: chatId, text: "⛔ 喵呜... 籽青查了一下,你目前还没有加入任何授权群组呢,不能给你看图库哦 QwQ", parse_mode: 'HTML' }, env);
      return;
    }
  }
  const hasFilter = chatId < 0 ? isFilterActive(await getUserFiltersBatch(userId, chatId, env)) : false;
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "你好呀！我是籽青喵 (≧∇≦) 请问今天想看点什么呢？", reply_markup: getMainMenuMarkup(hasFilter) }, env);
}

async function editMainMenu(chatId, msgId, env, userId) {
  if (chatId > 0) {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length === 0) {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "⛔ 喵... 你好像退群了呢,籽青已经把菜单收回去了哦！" }, env);
      return;
    }
  }
  const hasFilter = chatId < 0 ? isFilterActive(await getUserFiltersBatch(userId, chatId, env)) : false;
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "这是籽青的主菜单,请选择喵：", reply_markup: getMainMenuMarkup(hasFilter) }, env);
}

function getMainMenuMarkup(hasFilter = false) {
  const filterBtn = hasFilter ? "🔍 筛选器 🟢" : "🔍 筛选器 🔴";
  return { inline_keyboard: [
    [{ text: "🎲 开始随机", callback_data: "start_random" }, { text: filterBtn, callback_data: "filter_open" }],
    [{ text: "🏆 本群排行", callback_data: "leaderboard" }, { text: "📁 收藏夹", callback_data: "favorites" }],
    [{ text: "📜 历史足迹", callback_data: "history" }, { text: "⚙️ 籽青设置 (限管理)", callback_data: "set_main" }]
  ]};
}

async function showCategories(chatId, msgId, env, userId) {
  let keyboard = [];
  
  if (chatId < 0) {
    const localRes = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE category_name != 'output' AND chat_id = ?`).bind(chatId).all();
    if (localRes.results) {
      localRes.results.forEach(row => keyboard.push([{ text: `📂 ${row.category_name}`, callback_data: `random_${row.category_name}|${chatId}` }]));
    }
  } else {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length > 0) {
      const placeholders = allowedGroups.map(() => '?').join(', ');
      const { results } = await env.D1.prepare(
        `SELECT DISTINCT chat_id, chat_title, category_name FROM config_topics WHERE category_name != 'output' AND chat_id IN (${placeholders}) ORDER BY chat_title, category_name`
      ).bind(...allowedGroups).all();
      for (const row of (results || [])) {
        keyboard.push([{ text: `📂 [${row.chat_title}] ${row.category_name}`, callback_data: `random_${row.category_name}|${row.chat_id}` }]);
      }
    }
  }

  if (keyboard.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "呜呜,当前群组还没有绑定任何分类喵，管理员请使用 /bind 绑定哦！", reply_markup: getBackMarkup() }, env);

  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  const text = chatId < 0 ? "请选择您感兴趣的分类喵：" : "👇 以下是您所在群组的专属图库喵：";
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: text, reply_markup: { inline_keyboard: keyboard } }, env);
}

// 🌟 V5.9: 过滤器 UI 函数 ============================================================

// 媒体类型循环顺序
const FILTER_MEDIA_CYCLE = { all: 'photo', photo: 'video', video: 'animation', animation: 'all' };
const FILTER_MEDIA_LABEL = { all: '全部', photo: '仅图片 🖼️', video: '仅视频 🎬', animation: '仅动图 🎠' };

// 过滤器主面板
async function showFilterPanel(userId, chatId, msgId, sourceChatId, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const dateLabel = f.date_mode === 'custom'
    ? `${f.date_from}~${f.date_to}`
    : ({ all: '不限', today: '今天', d7: '近7天', d30: '近30天', year: '今年' }[f.date_mode] || '不限');
  const durLabel = f.duration_mode === 'custom'
    ? `≤${f.duration_max}s`
    : ({ all: '不限', s30: '≤30s', s60: '≤60s', s120: '≤120s', s300: '≤5分钟' }[f.duration_mode] || '不限');
  const sc = sourceChatId;
  const text = `🔍 **随机抽取筛选器**\n`
    + `（仅影响当前群组的随机抽取功能）\n\n`
    + `🎨 媒体类型：${FILTER_MEDIA_LABEL[f.media_type] || '全部'}\n`
    + `📅 收录时间：${dateLabel}\n`
    + `⏱ 视频时长：${durLabel}`;
  const keyboard = {
    inline_keyboard: [
      [{ text: `🎨 类型：${FILTER_MEDIA_LABEL[f.media_type]} 🔄`, callback_data: `filter_media|${sc}` }],
      [
        { text: `📅 时间：${dateLabel} ➡️`, callback_data: `filter_time_panel|${sc}` },
        { text: `⏱ 时长：${durLabel} ➡️`,  callback_data: `filter_dur_panel|${sc}` }
      ],
      [
        { text: "🗑️ 清除所有筛选", callback_data: `filter_reset|${sc}` },
        { text: "🏠 返回主菜单",   callback_data: "main_menu" }
      ]
    ]
  };
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: keyboard }, env);
}

// ForceReply 成功后发送新的过滤器面板（sendMessage 版本，用于保持沉浸体验）
async function sendFilterPanelNew(userId, chatId, topicId, sourceChatId, successText, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const dateLabel = f.date_mode === 'custom'
    ? `${f.date_from}~${f.date_to}`
    : ({ all: '不限', today: '今天', d7: '近7天', d30: '近30天', year: '今年' }[f.date_mode] || '不限');
  const durLabel = f.duration_mode === 'custom'
    ? `≤${f.duration_max}s`
    : ({ all: '不限', s30: '≤30s', s60: '≤60s', s120: '≤120s', s300: '≤5分钟' }[f.duration_mode] || '不限');
  const sc = sourceChatId;
  const text = `${successText}\n\n`
    + `🔍 **当前筛选状态**\n`
    + `🎨 类型：${FILTER_MEDIA_LABEL[f.media_type] || '全部'}\n`
    + `📅 时间：${dateLabel}\n`
    + `⏱ 时长：${durLabel}`;
  const keyboard = {
    inline_keyboard: [
      [{ text: `🎨 类型：${FILTER_MEDIA_LABEL[f.media_type]} 🔄`, callback_data: `filter_media|${sc}` }],
      [
        { text: `📅 时间：${dateLabel} ➡️`, callback_data: `filter_time_panel|${sc}` },
        { text: `⏱ 时长：${durLabel} ➡️`,  callback_data: `filter_dur_panel|${sc}` }
      ],
      [
        { text: "🗑️ 清除所有筛选", callback_data: `filter_reset|${sc}` },
        { text: "🏠 返回主菜单",   callback_data: "main_menu_new" }
      ]
    ]
  };
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text, parse_mode: 'Markdown', reply_markup: keyboard }, env);
}

// 收录时间子面板
async function showFilterTimePanel(userId, chatId, msgId, sourceChatId, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const ck = (val) => f.date_mode === val ? '✅ ' : '';
  const ckC = f.date_mode === 'custom' ? '✅ ' : '';
  const sc = sourceChatId;
  const keyboard = {
    inline_keyboard: [
      [{ text: `${ck('all')}不限`,   callback_data: `filter_time_all|${sc}` },  { text: `${ck('today')}今天`,  callback_data: `filter_time_today|${sc}` }],
      [{ text: `${ck('d7')}近7天`,   callback_data: `filter_time_d7|${sc}` },   { text: `${ck('d30')}近30天`,  callback_data: `filter_time_d30|${sc}` }],
      [{ text: `${ck('year')}今年`,   callback_data: `filter_time_year|${sc}` }],
      [{ text: `${ckC}✏️ 自定义时间段`, callback_data: `filter_time_custom|${sc}` }],
      [{ text: "⬅️ 返回筛选器",     callback_data: `filter_panel|${sc}` }]
    ]
  };
  const label = f.date_mode === 'custom' ? `${f.date_from}~${f.date_to}` : ({ all:'不限', today:'今天', d7:'近7天', d30:'近30天', year:'今年' }[f.date_mode] || '不限');
  await tgAPI('editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: `📅 **收录时间筛选**\n当前：${label}\n\n请选择时间范围：`,
    parse_mode: 'Markdown', reply_markup: keyboard
  }, env);
}

// 视频时长子面板
async function showFilterDurPanel(userId, chatId, msgId, sourceChatId, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const ck = (val) => f.duration_mode === val ? '✅ ' : '';
  const ckC = f.duration_mode === 'custom' ? '✅ ' : '';
  const sc = sourceChatId;
  const keyboard = {
    inline_keyboard: [
      [{ text: `${ck('all')}不限`,      callback_data: `filter_dur_all|${sc}` },  { text: `${ck('s30')}≤30秒`,   callback_data: `filter_dur_s30|${sc}` }],
      [{ text: `${ck('s60')}≤60秒`,     callback_data: `filter_dur_s60|${sc}` },  { text: `${ck('s120')}≤120秒`, callback_data: `filter_dur_s120|${sc}` }],
      [{ text: `${ck('s300')}≤5分钟`,   callback_data: `filter_dur_s300|${sc}` }],
      [{ text: `${ckC}✏️ 自定义秒数`,   callback_data: `filter_dur_custom|${sc}` }],
      [{ text: "⬅️ 返回筛选器",        callback_data: `filter_panel|${sc}` }]
    ]
  };
  const durLabel = f.duration_mode === 'custom'
    ? `≤${f.duration_max}s`
    : ({ all:'不限', s30:'≤30s', s60:'≤60s', s120:'≤120s', s300:'≤5分钟' }[f.duration_mode] || '不限');
  await tgAPI('editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: `⏱ **视频时长筛选**\n当前：${durLabel}\n\n输入 N 秒后，仅显示 0~N 秒的视频：`,
    parse_mode: 'Markdown', reply_markup: keyboard
  }, env);
}

// ====================================================================================
async function sendHistoricalMedia(userId, chatId, msgId, topicId, category, sourceChatId, offset, env, cbId) {
  let outChatId = chatId; let outTopicId = topicId;
  if (chatId < 0) {
    const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' AND chat_id = ? LIMIT 1`).bind(chatId).first();
    if (output) { outChatId = output.chat_id; outTopicId = output.topic_id; }
  }
  
  const settings = await getSettingsBatch(sourceChatId, ['display_mode', 'next_mode'], env);
  const mode = settings.display_mode;
  const nextMode = settings.next_mode || 'replace';

  // 根据偏移量拉取用户历史
  const media = await env.D1.prepare(`
    SELECT m.* FROM user_history h 
    JOIN media_library m ON h.media_id = m.id 
    WHERE h.user_id = ? AND h.chat_id = ? AND m.category_name = ?
    ORDER BY h.viewed_at DESC LIMIT 1 OFFSET ?
  `).bind(userId, sourceChatId, category, offset).first();

  if (!media) return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵... 时空尽头啦，前面没有更多记录了哦！", show_alert: true }, env);
  
  await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "时光倒流喵~ ⏪" }, env);

  if (nextMode === 'replace') {
    try { await tgAPI('deleteMessage', { chat_id: outChatId, message_id: msgId }, env); } catch(e){}
  }

  // 拼接回退控制键盘
  const actionKeyboard = [
    [ { text: "⏪ 继退", callback_data: `prev_${category}|${sourceChatId}|${offset + 1}` }, { text: "⏭️ 换新", callback_data: `next_${category}|${sourceChatId}` } ],
    [ { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` } ]
  ];

  if (mode === 'A') {
    const res = await tgAPI('forwardMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
    const data = await res.json();
    if(data.ok) {
      actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
      await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: data.result.message_id, text: "👆 (历史回忆) 可以点这里操作喵：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
    }
  } else {
    actionKeyboard.unshift([{ text: "🔗 去原记录围观", url: makeDeepLink(media.chat_id, media.message_id) }]);
    actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
    await tgAPI('copyMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
  }
}

// ==== 核心抽取与展现逻辑 ====
async function sendRandomMedia(userId, chatId, msgId, topicId, category, sourceChatId, isNext, env, ctx, cbId) {
  if (chatId > 0) {
    const inGroup = await isUserInGroup(sourceChatId, userId, env);
    if (!inGroup) {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🚨 喵！大骗子！籽青发现你已经退群啦,休想再拿之前的菜单偷看！(｀・ω・´)" }, env);
      return;
    }
  }

  let outChatId = chatId;
  let outTopicId = topicId;

  if (chatId < 0) {
    const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' AND chat_id = ? LIMIT 1`).bind(chatId).first();
    if (!output) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `喵？管理员还没设置本群输出话题呢,请用 /bind_output 设置！` }, env);
    outChatId = output.chat_id;
    outTopicId = output.topic_id;
  }

  // P1: 批量读取所有设置，同时读取用户过滤器
  const [settings, filters] = await Promise.all([
    getSettingsBatch(sourceChatId, ['display_mode', 'anti_repeat', 'auto_jump', 'show_success', 'next_mode', 'strict_skip'], env),
    getUserFiltersBatch(userId, sourceChatId, env)
  ]);
  const filterActive = isFilterActive(filters);
  const filterStatus = filterActive ? renderFilterStatus(filters) : null;
  const mode = settings.display_mode;
  const useAntiRepeat = settings.anti_repeat === 'true';
  const autoJump = settings.auto_jump === 'true';
  const showSuccess = settings.show_success === 'true';
  const nextMode = settings.next_mode || 'replace';
  const strictSkip = settings.strict_skip === 'true'; 
  const now = Date.now();

  let excludeMediaId = null;

  // 连点防刷退回逻辑 & 提取排除 ID
  if (isNext) {
    const last = await env.D1.prepare(`SELECT * FROM last_served WHERE user_id = ?`).bind(userId).first();
    if (last) {
      excludeMediaId = last.last_media_id; 
      
      if ((now - last.served_at) < 30000) {
        if (strictSkip) {
          ctx.waitUntil(
            env.D1.prepare(`UPDATE media_library SET view_count = MAX(0, view_count - 1) WHERE id = ?`).bind(excludeMediaId).run()
          );
        } else {
          ctx.waitUntil(Promise.all([
            env.D1.prepare(`UPDATE media_library SET view_count = MAX(0, view_count - 1) WHERE id = ?`).bind(excludeMediaId).run(),
            useAntiRepeat ? env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(excludeMediaId).run() : Promise.resolve()
          ]));
        }
      }
    }
  }

  let attempts = 0;
  let foundValid = false;
  let media = null;
  let newSentMessageId = null;

  while (attempts < 3 && !foundValid) {
    attempts++;

    media = await selectRandomMedia(category, sourceChatId, useAntiRepeat, excludeMediaId, filters, env);

    if (!media && useAntiRepeat) {
      const { sql: fSql, binds: fBinds } = buildFilterWhereClause(filters, 'm');
      const totalCheck = await env.D1.prepare(
        `SELECT count(*) as c FROM media_library m WHERE m.category_name = ? AND m.chat_id = ?${fSql}`
      ).bind(category, sourceChatId, ...fBinds).first();
      if (totalCheck && totalCheck.c > 0) {
        await env.D1.prepare(
          `DELETE FROM served_history WHERE media_id IN (SELECT m.id FROM media_library m WHERE m.category_name = ? AND m.chat_id = ?${fSql})`
        ).bind(category, sourceChatId, ...fBinds).run();
        const resetMsg = filterActive
          ? `🎉 哇哦,【${category}】在当前筛选条件下已全看光！防重库已重置喵~\n🔍 ${filterStatus}`
          : `🎉 哇哦,【${category}】的内容全看光了！籽青已重置防重库喵~`;
        await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, text: resetMsg }, env);
        media = await selectRandomMedia(category, sourceChatId, false, excludeMediaId, filters, env);
      }
    }

    if (!media) {
      const noMediaMsg = filterActive
        ? `呜呜,当前筛选条件下没有可抽取内容喵~\n🔍 ${filterStatus}`
        : `呜呜,该分类里还没有内容呢喵~`;
      await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: noMediaMsg }, env);
      return;
    }

    if (isNext && nextMode === 'replace' && attempts === 1) {
      try { await tgAPI('deleteMessage', { chat_id: outChatId, message_id: msgId }, env); } catch (e) {}
    }

    // 🌟 双排控制按钮 (带上 ⏪ 上一个)
    const actionKeyboard = [
      [ { text: "⏪ 上一个", callback_data: `prev_${category}|${sourceChatId}|1` }, { text: "⏭️ 换一个喵", callback_data: `next_${category}|${sourceChatId}` } ],
      [ { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` } ]
    ];

    const originalDeepLink = makeDeepLink(media.chat_id, media.message_id);

    let res, data;
    if (mode === 'A') {
      res = await tgAPI('forwardMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
      data = await res.json();
      if(data.ok) {
        newSentMessageId = data.result.message_id;
        actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
        await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: newSentMessageId, text: "👆 可以点这里操作喵：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
      }
    } else {
      actionKeyboard.unshift([{ text: "🔗 去原记录围观", url: originalDeepLink }]);
      actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
      res = await tgAPI('copyMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
      data = await res.json();
      if(data.ok) newSentMessageId = data.result.message_id;
    }

    if (data.ok) {
      foundValid = true;
    } else {
      const errDesc = data.description || '';
      console.error("探活报错喵:", errDesc);

      if (errDesc.includes('chat not found') || errDesc.includes('bot was kicked') || errDesc.includes('channel not found')) {
        await env.D1.prepare(`DELETE FROM media_library WHERE chat_id = ?`).bind(media.chat_id).run();
        await env.D1.prepare(`DELETE FROM config_topics WHERE chat_id = ?`).bind(media.chat_id).run();
      } else {
        await env.D1.prepare(`DELETE FROM media_library WHERE id = ?`).bind(media.id).run();
      }
    }
  }

  if (!foundValid) {
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🧹 呼... 连续抽到好多失效图片,籽青已经把坏数据打扫干净啦,请主人再点一次重抽喵！" }, env);
  }

  ctx.waitUntil(Promise.all([
    useAntiRepeat ? env.D1.prepare(`INSERT OR IGNORE INTO served_history (media_id) VALUES (?)`).bind(media.id).run() : Promise.resolve(),
    env.D1.prepare(`INSERT INTO last_served (user_id, last_media_id, served_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_media_id=excluded.last_media_id, served_at=excluded.served_at`).bind(userId, media.id, now).run(),
    env.D1.prepare(`UPDATE media_library SET view_count = view_count + 1 WHERE id = ?`).bind(media.id).run(),
    env.D1.prepare(`INSERT INTO user_history (user_id, chat_id, media_id) VALUES (?, ?, ?)`).bind(userId, sourceChatId, media.id).run(),
    env.D1.prepare(`INSERT INTO group_history (chat_id, media_id) VALUES (?, ?)`).bind(sourceChatId, media.id).run()
  ]));

  // 🌟 完全恢复 auto_jump 跳转功能！
  if (!isNext && chatId < 0) {
    if (showSuccess) {
      const jumpToOutputLink = newSentMessageId ? makeDeepLink(outChatId, newSentMessageId) : null;
      const jumpKeyboard = jumpToOutputLink && autoJump
        ? [[{ text: "🚀 飞去看看", url: jumpToOutputLink }], [{ text: "🏠 返回", callback_data: "main_menu" }]]
        : [[{ text: "🏠 返回", callback_data: "main_menu" }]];
      const successText = filterActive
        ? `🎉 抽取成功啦喵！已发送至输出话题。\n🔍 ${filterStatus}`
        : `🎉 抽取成功啦喵！已发送至输出话题。`;
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: successText, reply_markup: { inline_keyboard: jumpKeyboard } }, env);
    } else {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: filterActive ? `抽取成功喵！(筛选器已开启)` : "抽取成功喵！" }, env);
    }
  }
}

// 🌟 带有防崩溃 HTML 转义的排行榜
async function showLeaderboard(chatId, msgId, page, env) {
  if (chatId > 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "喵,私聊模式暂不支持查看群排行哦", reply_markup: getBackMarkup() }, env);
  const limit = 5, offset = page * limit;
  const [leaderData, totalRes] = await Promise.all([
    env.D1.prepare(`SELECT chat_id, message_id, category_name, view_count, caption FROM media_library WHERE view_count > 0 AND chat_id = ? ORDER BY view_count DESC LIMIT ? OFFSET ?`).bind(chatId, limit, offset).all(),
    env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE view_count > 0 AND chat_id = ?`).bind(chatId).first()
  ]);
  
  const escapeHTML = (str) => String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  let text = "🏆 <b>本群浏览量排行榜喵</b>\n\n";
  if (!leaderData.results || leaderData.results.length === 0) {
    text += "当前群组还没有产生播放数据呢~";
  } else {
    leaderData.results.forEach((row, idx) => { 
      const safeCaption = escapeHTML(row.caption ? row.caption.substring(0, 15) : '记录');
      text += `${offset + idx + 1}. [${escapeHTML(row.category_name)}] <a href="${makeDeepLink(row.chat_id, row.message_id)}">${safeCaption}</a> - 浏览: ${row.view_count}\n`; 
    });
  }

  const keyboard = []; const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `leader_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `leader_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: { inline_keyboard: keyboard } }, env);
}

async function handleAddFavorite(userId, cbId, mediaId, env) {
  try { await env.D1.prepare(`INSERT INTO user_favorites (user_id, media_id) VALUES (?, ?)`).bind(userId, mediaId).run(); await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "收藏成功喵！籽青帮你记下来啦~ ❤️", show_alert: true }, env); } catch (e) { await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵？你已经收藏过这个啦~", show_alert: true }, env); }
}

async function showFavoritesList(chatId, msgId, userId, page, env) {
  const limit = 5;
  const offset = page * limit;
  const { results } = await env.D1.prepare(`SELECT f.media_id, m.media_type, m.caption FROM user_favorites f LEFT JOIN media_library m ON f.media_id = m.id WHERE f.user_id = ? ORDER BY f.saved_at DESC LIMIT ? OFFSET ?`).bind(userId, limit, offset).all();
  const totalRes = await env.D1.prepare(`SELECT count(*) as c FROM user_favorites WHERE user_id = ?`).bind(userId).first();
  
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "你的收藏夹空空如也哦喵~", reply_markup: getBackMarkup() }, env);
  
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
  
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `📁 **主人的私有收藏夹** (共 ${totalRes.c} 条)`, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function showHistoryList(chatId, msgId, userId, page, env) {
  const limit = 5, offset = page * limit; let results, totalRes, title;
  
  if (chatId > 0) { 
    results = (await env.D1.prepare(`SELECT h.id as hist_id, m.id as media_id, m.media_type, m.caption FROM user_history h LEFT JOIN media_library m ON h.media_id = m.id WHERE h.user_id = ? ORDER BY h.viewed_at DESC LIMIT ? OFFSET ?`).bind(userId, limit, offset).all()).results;
    totalRes = await env.D1.prepare(`SELECT count(*) as c FROM user_history WHERE user_id = ?`).bind(userId).first();
    title = "🐾 主人的全局历史足迹";
  } else { 
    results = (await env.D1.prepare(`SELECT h.id as hist_id, m.id as media_id, m.media_type, m.caption FROM group_history h LEFT JOIN media_library m ON h.media_id = m.id WHERE h.chat_id = ? ORDER BY h.viewed_at DESC LIMIT ? OFFSET ?`).bind(chatId, limit, offset).all()).results;
    totalRes = await env.D1.prepare(`SELECT count(*) as c FROM group_history WHERE chat_id = ?`).bind(chatId).first();
    title = "🐾 本群的历史足迹";
  }
  
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "这里干干净净的，还没有留下任何足迹喵~", reply_markup: getBackMarkup() }, env);
  
  const keyboard = results.map((r) => {
    const typeIcon = r.media_type === 'video' ? '🎬' : (r.media_type === 'photo' ? '🖼️' : '📁');
    const caption = r.caption ? r.caption.substring(0, 15) : '已看记录';
    const typePrefix = chatId > 0 ? 'u' : 'g'; 
    return [
      { text: `${typeIcon} ${caption}`, callback_data: `hist_view_${r.media_id}` }, 
      { text: `❌ 抹除`, callback_data: `hist_del_${typePrefix}_${r.hist_id}` }
    ];
  });

  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `hist_page_${page - 1}` });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: `hist_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `${title} (共 ${totalRes.c} 条)`, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}


async function viewFavorite(chatId, topicId, mediaId, env) {
  const media = await env.D1.prepare(`SELECT * FROM media_library WHERE id = ?`).bind(mediaId).first();
  if (media) await tgAPI('copyMessage', { chat_id: chatId, message_thread_id: topicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
}

// ==== V5.5 专属设置看板 ====
async function showSettingsMain(chatId, msgId, env) {
  const settings = await getSettingsBatch(chatId, ['display_mode', 'anti_repeat', 'auto_jump', 'dup_notify', 'show_success', 'next_mode', 'strict_skip'], env);
  const mode = settings.display_mode;
  const repeat = settings.anti_repeat;
  const jump = settings.auto_jump;
  const dup = settings.dup_notify;
  const showSuccess = settings.show_success;
  const nextMode = settings.next_mode;
  const strictSkip = settings.strict_skip;
  
  const text = "⚙️ **本群的独立控制面板喵**\n\n请主人调整下方的功能开关：";
  const keyboard = [
    [{ text: `🔀 展现形式: ${mode === 'A' ? 'A(原生转发)' : 'B(复制+链接)'}`, callback_data: "set_toggle_mode" }],
    [{ text: `🔁 防重库机制: ${repeat === 'true' ? '✅ 已开启' : '❌ 未开启'}`, callback_data: "set_toggle_repeat" }],
    [{ text: `⏱️ 快划跳过模式: ${strictSkip === 'true' ? '🔥 严格消耗(强制防重)' : '♻️ 稍后再看(正常防重)'}`, callback_data: "set_toggle_strict" }], 
    [{ text: `🔕 重复收录提示: ${dup === 'true' ? '📢 消息提醒' : '🔇 静默拦截'}`, callback_data: "set_toggle_dup" }],
    [{ text: `🔄 '换一个'模式: ${nextMode === 'replace' ? '🖼️ 原地替换(删旧发新)' : '💬 发新消息(保留历史)'}`, callback_data: "set_toggle_nextmode" }],
    [{ text: `🔔 抽取成功提示: ${showSuccess === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_success" }],
    [{ text: `🚀 抽取后生成跳转: ${jump === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_jump" }],
    [{ text: "🗑️ 管理本群解绑", callback_data: "set_unbind_list" }, { text: "📊 本群超级数据看板", callback_data: "set_stats" }],
    [{ text: "⚠️ 危险操作区 (清空本群数据)", callback_data: "set_danger_zone" }],
    [{ text: "🏠 返回主菜单", callback_data: "main_menu" }]
  ];
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function toggleSetting(key, env, chatId, msgId, values) {
  const current = await getSetting(chatId, key, env);
  const valCurrent = current === null ? values[0] : current;
  const next = valCurrent === values[0] ? values[1] : values[0];
  
  await env.D1.prepare(`INSERT INTO chat_settings (chat_id, key, value) VALUES (?, ?, ?) ON CONFLICT(chat_id, key) DO UPDATE SET value=excluded.value`).bind(chatId, key, next).run();
  
  await showSettingsMain(chatId, msgId, env);
}

async function showUnbindList(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT id, chat_title, category_name FROM config_topics WHERE chat_id = ?`).bind(chatId).all();
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "本群目前没有绑定任何记录喵~", reply_markup: { inline_keyboard: [[{text: "返回设置", callback_data: "set_main"}]] } }, env);
  const keyboard = results.map(r => [{ text: `🗑️ 解绑 [${r.category_name}]`, callback_data: `set_unbind_do_${r.id}` }]);
  keyboard.push([{ text: "⬅️ 返回设置", callback_data: "set_main" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "点击对应按钮解除本群的话题绑定喵：", reply_markup: { inline_keyboard: keyboard } }, env);
}

// 🌟 究极防弹版：增强版全知数据看板 (自带时间戳刷新与全类型安全转换)
async function showStats(chatId, msgId, env) {
  try {
    const [mediaRes, viewRes, catRes, userRes, antiRes, recentAntiRes] = await Promise.all([
      env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ?`).bind(chatId).first(),
      env.D1.prepare(`SELECT sum(view_count) as v FROM media_library WHERE chat_id = ?`).bind(chatId).first(),
      env.D1.prepare(`SELECT category_name, count(*) as c FROM media_library WHERE chat_id = ? GROUP BY category_name`).bind(chatId).all(),
      // 这里的表名已经彻底确认为 user_history
      env.D1.prepare(`SELECT u.user_id, r.first_name, count(*) as c FROM user_history u LEFT JOIN user_roster r ON u.user_id = r.user_id WHERE u.chat_id = ? GROUP BY u.user_id ORDER BY c DESC LIMIT 3`).bind(chatId).all(),
      env.D1.prepare(`SELECT count(*) as c FROM served_history sh JOIN media_library m ON sh.media_id = m.id WHERE m.chat_id = ?`).bind(chatId).first(),
      env.D1.prepare(`SELECT m.caption, m.media_type FROM served_history sh JOIN media_library m ON sh.media_id = m.id WHERE m.chat_id = ? ORDER BY sh.media_id DESC LIMIT 5`).bind(chatId).all()
    ]);

    // 究极安全的 HTML 转义工具，防止 null 或纯数字搞崩系统
    const escapeHTML = (str) => {
      if (str === null || str === undefined) return '';
      return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    };

    let text = `📊 <b>本群超级数据看板喵</b>\n\n`;
    text += `📦 <b>总收录</b>: ${mediaRes?.c || 0} 条\n`;
    text += `👀 <b>总浏览</b>: ${viewRes?.v || 0} 次\n`;
    text += `🛡️ <b>防重库</b>: 拦截了 ${antiRes?.c || 0} 条\n\n`;
    
    text += `📂 <b>分类统计</b>:\n`;
    if (catRes.results && catRes.results.length > 0) {
      catRes.results.forEach(r => text += `- ${escapeHTML(r.category_name)}: ${r.c} 条\n`);
    } else {
      text += `- 暂无分类\n`;
    }
    
    text += `\n🔥 <b>群内最活跃大佬 (Top 3)</b>:\n`;
    if (userRes.results && userRes.results.length > 0) {
      userRes.results.forEach((r, idx) => { 
        const safeName = escapeHTML(r.first_name || `神秘人(${r.user_id})`);
        text += `${idx+1}. <a href="tg://user?id=${r.user_id}">${safeName}</a> (抽图 ${r.c} 次)\n`; 
      });
    } else {
      text += `- 暂无数据\n`;
    }
    
    text += `\n🛡️ <b>最近被打入冷宫的记录</b>:\n`;
    if (recentAntiRes.results && recentAntiRes.results.length > 0) {
      recentAntiRes.results.forEach(r => { 
        // 强制转换为字符串，防止纯数字配文导致 substring 报错
        const capStr = String(r.caption || '');
        const safeCaption = escapeHTML(capStr ? capStr.substring(0, 10) : '无配文');
        text += `- ${r.media_type === 'video' ? '🎬' : '🖼️'} ${safeCaption}\n`; 
      });
    } else {
      text += `- 防重库为空喵\n`;
    }

    // 🌟 杀手锏：强制加入微秒级时间戳！
    // 这样保证每次点击时，发给 Telegram 的文字都是 100% 不同的，彻底解决 message is not modified 不刷新的问题！
    const timeStr = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
    text += `\n<i>(数据更新于: ${timeStr})</i>`;

    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{text: "⬅️ 返回设置", callback_data: "set_main"}]] } }, env);
  } catch (e) {
    console.error("看板报错:", e.message);
    // 同样给报错信息套上防弹转义，确诊连 Telegram 都不敢吞报错
    const errStr = String(e.message || '未知错误').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const errText = `🚨 <b>面板崩溃啦！</b>\n\n详细报错信息：\n<code>${errStr}</code>`;
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: errText, parse_mode: 'HTML', reply_markup: { inline_keyboard: [[{text: "⬅️ 返回设置", callback_data: "set_main"}]] } }, env);
  }
}

function getBackMarkup() {
  return { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "main_menu" }]] };
}

/* =========================================================================
 * Telegram Web App (小程序) 前端 UI 与 后端 API 模块
 * ========================================================================= */
function getWebAppHTML() {
  return `
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
      <title>籽青控制台</title>
      <script src="https://telegram.org/js/telegram-web-app.js"></script>
      <style>
        :root {
          --tg-theme-bg-color: #f3f4f6;
          --tg-theme-text-color: #222222;
          --tg-theme-button-color: #ff758c;
          --tg-theme-button-text-color: #ffffff;
          --tg-theme-secondary-bg-color: #e5e7eb;
          --tg-theme-hint-color: rgba(34, 34, 34, 0.6);
          --tg-theme-border-color: rgba(0, 0, 0, 0.08);
          --tg-theme-destructive-color: #ff4d4f;
          --safe-area-bottom: env(safe-area-inset-bottom, 0px);
          --page-padding: 20px;
          --card-radius: 18px;
          --chip-radius: 999px;
          --shadow-soft: 0 8px 24px rgba(0, 0, 0, 0.08);
        }
        * { box-sizing: border-box; }
        body {
          font-family: system-ui, -apple-system, sans-serif;
          background-color: var(--tg-theme-bg-color);
          color: var(--tg-theme-text-color);
          margin: 0;
          padding: 0 0 calc(88px + var(--safe-area-bottom));
          transition: background-color 0.3s, color 0.3s;
        }
        .header {
          padding: 24px var(--page-padding) 18px;
          background: linear-gradient(135deg, var(--tg-theme-button-color) 0%, #ff7eb3 100%);
          color: #ffffff;
          border-bottom-left-radius: 24px;
          border-bottom-right-radius: 24px;
          box-shadow: 0 10px 30px rgba(255, 117, 140, 0.25);
        }
        .header h1 { margin: 0; font-size: 24px; font-weight: 800; }
        .header p { margin: 6px 0 0; opacity: 0.92; font-size: 14px; }
        .hero-chips { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 14px; }
        .hero-chip {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 6px 12px;
          border-radius: var(--chip-radius);
          background: rgba(255, 255, 255, 0.18);
          backdrop-filter: blur(6px);
          font-size: 12px;
          font-weight: 700;
        }
        .tab-content { display: none; padding: var(--page-padding); animation: fadeIn 0.25s ease; }
        .tab-content.active { display: block; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }
        @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }
        .card {
          background-color: var(--tg-theme-secondary-bg-color);
          border-radius: var(--card-radius);
          padding: 16px;
          margin-bottom: 16px;
          box-shadow: var(--shadow-soft);
        }
        .card h3 {
          margin: 0;
          font-size: 16px;
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 8px;
        }
        .section-title { display: flex; align-items: center; gap: 8px; }
        .section-subtitle { margin-top: 6px; color: var(--tg-theme-hint-color); font-size: 12px; line-height: 1.5; }
        .stats-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }
        .stat-item { padding: 14px 12px; border-radius: 14px; background: rgba(255, 255, 255, 0.35); text-align: center; }
        .stat-item b { display: block; font-size: 22px; line-height: 1.1; margin-bottom: 6px; }
        .stat-item span { font-size: 12px; color: var(--tg-theme-hint-color); font-weight: 700; }
        .leaderboard-list { margin: 14px 0 0; padding: 0; list-style: none; }
        .leaderboard-item { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 10px 0; border-bottom: 1px dashed var(--tg-theme-border-color); }
        .leaderboard-item:last-child { border-bottom: none; }
        .leaderboard-user { display: flex; align-items: center; gap: 10px; min-width: 0; }
        .leaderboard-rank { width: 30px; height: 30px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; background: rgba(255,255,255,0.4); font-size: 14px; }
        .leaderboard-name { font-size: 14px; font-weight: 700; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .leaderboard-count { font-size: 13px; font-weight: 800; }
        .section-header { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 12px; }
        .section-count { color: var(--tg-theme-hint-color); font-size: 12px; font-weight: 700; }
        .gallery-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
        .history-list { display: flex; flex-direction: column; gap: 12px; }
        .media-card { background: rgba(255, 255, 255, 0.35); border-radius: 14px; padding: 12px; box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04); transition: transform 0.2s ease, opacity 0.2s ease, margin 0.2s ease; overflow: hidden; }
        .media-card.is-removing { opacity: 0; transform: scale(0.95); margin-top: 0; margin-bottom: 0; }
        .media-card-top { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 10px; }
        .media-icon { width: 40px; height: 40px; border-radius: 12px; background: rgba(255,255,255,0.5); display: inline-flex; align-items: center; justify-content: center; font-size: 22px; flex-shrink: 0; }
        .media-body { min-width: 0; flex: 1; }
        .media-title { font-size: 13px; font-weight: 800; line-height: 1.4; margin: 0; word-break: break-word; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
        .media-meta { margin-top: 6px; font-size: 11px; color: var(--tg-theme-hint-color); display: flex; flex-wrap: wrap; gap: 6px; }
        .meta-badge { display: inline-flex; align-items: center; padding: 3px 8px; border-radius: var(--chip-radius); background: rgba(255,255,255,0.5); }
        .media-actions { display: flex; gap: 8px; }
        .media-btn { flex: 1; border: none; border-radius: 10px; padding: 8px 10px; font-size: 12px; font-weight: 800; cursor: pointer; transition: transform 0.15s ease, opacity 0.15s ease; }
        .media-btn:active { transform: scale(0.97); opacity: 0.86; }
        .media-btn-primary { background: var(--tg-theme-button-color); color: var(--tg-theme-button-text-color); }
        .media-btn-danger { flex: 0 0 42px; background: var(--tg-theme-destructive-color); color: #ffffff; }
        .setting-list { display: flex; flex-direction: column; gap: 10px; margin-top: 14px; }
        .setting-item { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 14px; border-radius: 14px; background: rgba(255, 255, 255, 0.35); }
        .setting-item strong { display: block; font-size: 14px; }
        .setting-item span { display: block; margin-top: 4px; font-size: 12px; color: var(--tg-theme-hint-color); }
        .setting-value { color: var(--tg-theme-text-color); font-size: 12px; font-weight: 800; text-align: right; }
        .state-block { grid-column: 1 / -1; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; min-height: 160px; text-align: center; color: var(--tg-theme-hint-color); padding: 16px; border-radius: 14px; background: rgba(255, 255, 255, 0.3); }
        .state-icon { font-size: 36px; }
        .state-title { font-size: 14px; font-weight: 800; color: var(--tg-theme-text-color); }
        .state-desc { font-size: 12px; line-height: 1.6; }
        .skeleton-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
        .skeleton-list { display: flex; flex-direction: column; gap: 12px; }
        .skeleton-card, .skeleton-row { border-radius: 14px; background: linear-gradient(90deg, rgba(255,255,255,0.25) 25%, rgba(255,255,255,0.55) 50%, rgba(255,255,255,0.25) 75%); background-size: 200% 100%; animation: shimmer 1.4s infinite linear; }
        .skeleton-card { height: 120px; }
        .skeleton-row { height: 74px; }
        .bottom-nav { position: fixed; bottom: 0; left: 0; right: 0; height: calc(70px + var(--safe-area-bottom)); padding-bottom: var(--safe-area-bottom); background-color: var(--tg-theme-secondary-bg-color); display: flex; justify-content: space-around; align-items: center; border-top-left-radius: 22px; border-top-right-radius: 22px; box-shadow: 0 -2px 15px rgba(0,0,0,0.06); z-index: 1000; }
        .nav-item { display: flex; flex-direction: column; align-items: center; justify-content: center; width: 25%; height: 100%; color: var(--tg-theme-text-color); opacity: 0.62; text-decoration: none; font-size: 12px; font-weight: 800; transition: all 0.2s; }
        .nav-item.active { opacity: 1; color: var(--tg-theme-button-color); transform: translateY(-2px); }
        .nav-icon { font-size: 22px; margin-bottom: 4px; }
        @media (max-width: 360px) { .gallery-grid { grid-template-columns: 1fr; } }
      </style>
    </head>
    <body>
      <div class="header">
        <h1>🐾 籽青控制台</h1>
        <p id="welcome-text">正在连接神经元...</p>
        <div class="hero-chips">
          <div class="hero-chip">Telegram WebApp</div>
          <div class="hero-chip" id="hero-theme-chip">主题同步中</div>
          <div class="hero-chip" id="hero-env-chip">等待识别环境</div>
        </div>
      </div>

      <div id="tab-dashboard" class="tab-content active">
        <div class="card">
          <h3><span class="section-title">📊 全局核心数据</span></h3>
          <div class="section-subtitle">这里展示全局收录体量、浏览热度与防重统计喵～</div>
          <div class="stats-grid">
            <div class="stat-item"><b id="stat-media">--</b><span>收录</span></div>
            <div class="stat-item"><b id="stat-views">--</b><span>浏览</span></div>
            <div class="stat-item"><b id="stat-anti">--</b><span>防重拦截</span></div>
            <div class="stat-item"><b id="stat-groups">--</b><span>群组</span></div>
          </div>
        </div>
        <div class="card">
          <h3><span class="section-title">🏆 全局最高活跃排名</span></h3>
          <div class="section-subtitle">最近的高活跃用户会出现在这里喵～</div>
          <ul id="top-users-list" class="leaderboard-list">
            <li class="leaderboard-item"><span>正在拉取排行喵...</span></li>
          </ul>
        </div>
      </div>

      <div id="tab-settings" class="tab-content">
        <div class="card">
          <h3><span class="section-title">⚙️ 控制台信息</span></h3>
          <div class="section-subtitle">这里展示当前账户、界面环境与数据摘要，不会修改任何设置喵～</div>
          <div id="settings-container" class="setting-list"></div>
        </div>
      </div>

      <div id="tab-gallery" class="tab-content">
        <div class="card">
          <div class="section-header">
            <div>
              <h3><span class="section-title">🖼️ 我的私人画廊</span></h3>
              <div class="section-subtitle">双列卡片更适合快速浏览收藏内容喵～</div>
            </div>
            <div id="gallery-count" class="section-count">--</div>
          </div>
          <div id="gallery-container" class="gallery-grid"></div>
        </div>
      </div>

      <div id="tab-history" class="tab-content">
        <div class="card">
          <div class="section-header">
            <div>
              <h3><span class="section-title">📜 我的全局足迹</span></h3>
              <div class="section-subtitle">单列时间线更方便回看最近浏览记录喵～</div>
            </div>
            <div id="history-count" class="section-count">--</div>
          </div>
          <div id="history-container" class="history-list"></div>
        </div>
      </div>

      <div class="bottom-nav">
        <div class="nav-item active" onclick="switchTab('dashboard', this)">
          <div class="nav-icon">📊</div><span>看板</span>
        </div>
        <div class="nav-item" onclick="switchTab('settings', this)">
          <div class="nav-icon">⚙️</div><span>设置</span>
        </div>
        <div class="nav-item" onclick="switchTab('gallery', this)">
          <div class="nav-icon">🖼️</div><span>画廊</span>
        </div>
        <div class="nav-item" onclick="switchTab('history', this)">
          <div class="nav-icon">📜</div><span>足迹</span>
        </div>
      </div>

      <script>
        const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
        const docEl = document.documentElement;
        const viewState = { favorites: [], history: [] };

        function ensureTelegramCapability() {
          return {
            hasTg: !!tg,
            canOpenLink: !!(tg && tg.openTelegramLink),
            canHaptic: !!(tg && tg.HapticFeedback),
            canAlert: !!(tg && tg.showAlert)
          };
        }

        const capabilities = ensureTelegramCapability();
        if (capabilities.hasTg) {
          tg.expand();
          tg.ready();
        }

        docEl.style.setProperty('--tg-theme-bg-color', tg && tg.themeParams.bg_color || '#f3f4f6');
        docEl.style.setProperty('--tg-theme-text-color', tg && tg.themeParams.text_color || '#222222');
        docEl.style.setProperty('--tg-theme-button-color', tg && tg.themeParams.button_color || '#ff758c');
        docEl.style.setProperty('--tg-theme-button-text-color', tg && tg.themeParams.button_text_color || '#ffffff');
        docEl.style.setProperty('--tg-theme-secondary-bg-color', tg && tg.themeParams.secondary_bg_color || '#e5e7eb');
        docEl.style.setProperty('--tg-theme-hint-color', tg && tg.themeParams.hint_color || 'rgba(34, 34, 34, 0.6)');
        docEl.style.setProperty('--tg-theme-border-color', 'rgba(0, 0, 0, 0.08)');

        const els = {
          welcomeText: document.getElementById('welcome-text'),
          heroThemeChip: document.getElementById('hero-theme-chip'),
          heroEnvChip: document.getElementById('hero-env-chip'),
          statMedia: document.getElementById('stat-media'),
          statViews: document.getElementById('stat-views'),
          statAnti: document.getElementById('stat-anti'),
          statGroups: document.getElementById('stat-groups'),
          topUsersList: document.getElementById('top-users-list'),
          settingsContainer: document.getElementById('settings-container'),
          galleryContainer: document.getElementById('gallery-container'),
          historyContainer: document.getElementById('history-container'),
          galleryCount: document.getElementById('gallery-count'),
          historyCount: document.getElementById('history-count')
        };

        function escapeHTML(value) {
          return String(value ?? '').replace(/[&<>"']/g, function(ch) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch] || ch;
          });
        }

        function safeText(value, fallback) {
          const text = String(value ?? '').trim();
          return text || fallback;
        }

        function getMediaIcon(mediaType) {
          if (mediaType === 'video') return '🎬';
          if (mediaType === 'photo') return '🖼️';
          if (mediaType === 'animation') return '✨';
          return '📁';
        }

        function getMediaLabel(mediaType) {
          if (mediaType === 'video') return '视频';
          if (mediaType === 'photo') return '图片';
          if (mediaType === 'animation') return '动图';
          return '文件';
        }

        function buildTelegramDeepLink(chatId, messageId) {
          if (!chatId || !messageId) return '';
          return 'https://t.me/c/' + String(chatId).replace('-100', '') + '/' + messageId;
        }

        function setImpact(type) {
          if (!capabilities.canHaptic) return;
          if (type === 'selection' && tg.HapticFeedback.selectionChanged) {
            tg.HapticFeedback.selectionChanged();
            return;
          }
          if (type === 'success' && tg.HapticFeedback.notificationOccurred) {
            tg.HapticFeedback.notificationOccurred('success');
            return;
          }
          if (type === 'error' && tg.HapticFeedback.notificationOccurred) {
            tg.HapticFeedback.notificationOccurred('error');
            return;
          }
          if (tg.HapticFeedback.impactOccurred) tg.HapticFeedback.impactOccurred('medium');
        }

        function showAlert(message) {
          if (capabilities.canAlert) {
            tg.showAlert(message);
          } else {
            alert(message);
          }
        }

        function buildStateBlock(icon, title, desc) {
          return '<div class="state-block">' +
            '<div class="state-icon">' + escapeHTML(icon) + '</div>' +
            '<div class="state-title">' + escapeHTML(title) + '</div>' +
            '<div class="state-desc">' + escapeHTML(desc) + '</div>' +
          '</div>';
        }

        function buildSkeletonMarkup(kind) {
          if (kind === 'history') {
            return '<div class="skeleton-list"><div class="skeleton-row"></div><div class="skeleton-row"></div><div class="skeleton-row"></div></div>';
          }
          return '<div class="skeleton-grid"><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div></div>';
        }

        function setSectionState(container, state, options) {
          const opts = options || {};
          if (state === 'loading') {
            container.innerHTML = buildSkeletonMarkup(opts.kind);
            return;
          }
          if (state === 'error') {
            container.innerHTML = buildStateBlock('⚠️', opts.title || '连接失败了喵', opts.desc || '请稍后再试试');
            return;
          }
          container.innerHTML = buildStateBlock(opts.icon || '📭', opts.title || '这里还是空的喵', opts.desc || '暂时没有可显示的数据');
        }

        function normalizeMediaItem(item, kind) {
          const recordId = kind === 'favorite' ? (item.media_id ?? item.id) : (item.hist_id ?? item.id);
          return {
            kind,
            recordId: recordId ?? 0,
            cardId: (kind === 'favorite' ? 'fav-item-' : 'hist-item-') + (recordId ?? 0),
            caption: safeText(item.caption, kind === 'favorite' ? '已收藏记录' : '已看记录'),
            mediaType: safeText(item.media_type, 'file'),
            mediaLabel: getMediaLabel(item.media_type),
            icon: getMediaIcon(item.media_type),
            chatId: item.chat_id,
            messageId: item.message_id,
            deepLink: buildTelegramDeepLink(item.chat_id, item.message_id)
          };
        }

        function normalizeWebAppData(raw) {
          const favorites = Array.isArray(raw && raw.favorites) ? raw.favorites.map(function(item) { return normalizeMediaItem(item, 'favorite'); }) : [];
          const history = Array.isArray(raw && raw.history) ? raw.history.map(function(item) { return normalizeMediaItem(item, 'history'); }) : [];
          return {
            dashboard: raw && raw.dashboard ? raw.dashboard : {},
            topUsers: Array.isArray(raw && raw.top_users) ? raw.top_users : [],
            favorites,
            history
          };
        }

        function renderDashboard(dashboard) {
          els.statMedia.innerText = dashboard.total_media ?? 0;
          els.statViews.innerText = dashboard.total_views ?? 0;
          els.statGroups.innerText = dashboard.total_groups ?? 0;
          els.statAnti.innerText = dashboard.total_anti ?? 0;
        }

        function renderTopUsers(users) {
          if (!users.length) {
            els.topUsersList.innerHTML = '<li class="leaderboard-item"><span>暂无数据喵</span></li>';
            return;
          }
          els.topUsersList.innerHTML = users.map(function(u, i) {
            const medals = ['🥇', '🥈', '🥉'];
            const rank = medals[i] || ('#' + (i + 1));
            return '<li class="leaderboard-item">' +
              '<div class="leaderboard-user">' +
                '<span class="leaderboard-rank">' + escapeHTML(rank) + '</span>' +
                '<span class="leaderboard-name">' + escapeHTML(safeText(u.first_name, '神秘人')) + '</span>' +
              '</div>' +
              '<b class="leaderboard-count">' + escapeHTML(String(u.c ?? 0)) + ' 次</b>' +
            '</li>';
          }).join('');
        }

        function updateSectionMeta(section, count) {
          const text = count + ' 条';
          if (section === 'gallery') els.galleryCount.innerText = text;
          if (section === 'history') els.historyCount.innerText = text;
        }

        function renderSettingsView(payload) {
          const rows = [
            { title: '账户', desc: safeText(payload.userName, '未识别 Telegram 用户') + ' · ID ' + safeText(payload.userId, '--'), value: payload.source },
            { title: '界面', desc: '主题色、按钮色与背景色都已同步到当前客户端', value: payload.themeLabel },
            { title: '数据', desc: '收藏 ' + payload.favoriteCount + ' 条 · 足迹 ' + payload.historyCount + ' 条', value: payload.refreshedAt },
            { title: '说明', desc: '这里只展示当前环境和摘要信息，不会直接修改设置', value: payload.envLabel }
          ];
          els.settingsContainer.innerHTML = rows.map(function(row) {
            return '<div class="setting-item">' +
              '<div><strong>' + escapeHTML(row.title) + '</strong><span>' + escapeHTML(row.desc) + '</span></div>' +
              '<div class="setting-value">' + escapeHTML(row.value) + '</div>' +
            '</div>';
          }).join('');
        }

        function renderMediaCard(item) {
          const action = item.kind === 'favorite'
            ? 'removeFav(' + item.recordId + ', this)'
            : 'removeHist(' + item.recordId + ', this)';
          const openAction = item.deepLink
            ? 'openMediaLink(&#39;' + item.deepLink.replace(/'/g, '&#39;') + '&#39;)'
            : 'showAlert(&#39;当前记录缺少跳转链接喵&#39;)';
          const meta = [
            '<span class="meta-badge">' + escapeHTML(item.mediaLabel) + '</span>',
            '<span class="meta-badge">ID ' + escapeHTML(String(item.recordId)) + '</span>'
          ].join('');
          return '<div class="media-card" id="' + escapeHTML(item.cardId) + '">' +
            '<div class="media-card-top">' +
              '<div class="media-icon">' + escapeHTML(item.icon) + '</div>' +
              '<div class="media-body">' +
                '<p class="media-title">' + escapeHTML(item.caption) + '</p>' +
                '<div class="media-meta">' + meta + '</div>' +
              '</div>' +
            '</div>' +
            '<div class="media-actions">' +
              '<button class="media-btn media-btn-primary" onclick="' + openAction + '">👀 围观</button>' +
              '<button class="media-btn media-btn-danger" onclick="' + action + '">🗑️</button>' +
            '</div>' +
          '</div>';
        }

        function renderMediaSection(container, items, options) {
          if (!items.length) {
            setSectionState(container, 'empty', options.emptyState);
            return;
          }
          container.innerHTML = items.map(renderMediaCard).join('');
        }

        function toggleButtonLoading(btnElement, isLoading) {
          if (!btnElement) return;
          if (isLoading) {
            btnElement.dataset.originalText = btnElement.innerText;
            btnElement.innerText = '...';
            btnElement.disabled = true;
            return;
          }
          btnElement.innerText = btnElement.dataset.originalText || '🗑️';
          btnElement.disabled = false;
        }

        function animateRemoveCard(cardId) {
          const card = document.getElementById(cardId);
          if (!card) return;
          card.classList.add('is-removing');
          setTimeout(function() { card.remove(); }, 180);
        }

        function refreshEmptyState(kind) {
          if (kind === 'favorite' && !viewState.favorites.length) {
            setSectionState(els.galleryContainer, 'empty', { icon: '📭', title: '收藏夹空空如也喵~', desc: '等主人收藏一些喜欢的媒体后，这里就会热闹起来啦' });
          }
          if (kind === 'history' && !viewState.history.length) {
            setSectionState(els.historyContainer, 'empty', { icon: '🐾', title: '这里干干净净的', desc: '还没有留下任何浏览足迹喵~' });
          }
        }

        async function submitRemoval(config) {
          const btnElement = config.btnElement;
          toggleButtonLoading(btnElement, true);
          setImpact('medium');
          try {
            const res = await fetch(config.endpoint, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify(config.payload)
            });
            const data = await res.json();
            if (!data.success) throw new Error(data.error || '未知错误');
            animateRemoveCard(config.cardId);
            if (config.kind === 'favorite') {
              viewState.favorites = viewState.favorites.filter(function(item) { return item.recordId !== config.recordId; });
              updateSectionMeta('gallery', viewState.favorites.length);
            } else {
              viewState.history = viewState.history.filter(function(item) { return item.recordId !== config.recordId; });
              updateSectionMeta('history', viewState.history.length);
            }
            setTimeout(function() { refreshEmptyState(config.kind); }, 200);
            setImpact('success');
          } catch (e) {
            toggleButtonLoading(btnElement, false);
            setImpact('error');
            showAlert((config.failPrefix || '操作失败：') + e.message);
          }
        }

        function openMediaLink(url) {
          if (!url) {
            showAlert('当前记录缺少跳转链接喵');
            return;
          }
          if (capabilities.canOpenLink) {
            tg.openTelegramLink(url);
            return;
          }
          window.open(url, '_blank', 'noopener,noreferrer');
        }

        function switchTab(tabId, el) {
          document.querySelectorAll('.tab-content').forEach(function(tab) { tab.classList.remove('active'); });
          document.querySelectorAll('.nav-item').forEach(function(nav) { nav.classList.remove('active'); });
          document.getElementById('tab-' + tabId).classList.add('active');
          el.classList.add('active');
          setImpact('selection');
        }

        function renderInitialStates() {
          setSectionState(els.galleryContainer, 'loading', { kind: 'gallery' });
          setSectionState(els.historyContainer, 'loading', { kind: 'history' });
        }

        async function fetchAppData(userId) {
          try {
            const response = await fetch('/api/webapp/data', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ user_id: userId })
            });
            if (!response.ok) throw new Error('网络响应异常');
            const raw = await response.json();
            const data = normalizeWebAppData(raw);

            viewState.favorites = data.favorites;
            viewState.history = data.history;

            renderDashboard(data.dashboard);
            renderTopUsers(data.topUsers);
            updateSectionMeta('gallery', data.favorites.length);
            updateSectionMeta('history', data.history.length);
            renderMediaSection(els.galleryContainer, data.favorites, {
              emptyState: { icon: '📭', title: '收藏夹空空如也喵~', desc: '等主人收藏一些喜欢的媒体后，这里就会热闹起来啦' }
            });
            renderMediaSection(els.historyContainer, data.history, {
              emptyState: { icon: '🐾', title: '这里干干净净的', desc: '还没有留下任何浏览足迹喵~' }
            });
            renderSettingsView({
              userName: user && user.first_name,
              userId: user && user.id,
              source: user ? 'Telegram' : '受限模式',
              themeLabel: tg && tg.colorScheme ? (tg.colorScheme === 'dark' ? '深色主题' : '浅色主题') : '跟随系统',
              favoriteCount: data.favorites.length,
              historyCount: data.history.length,
              refreshedAt: new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' }),
              envLabel: capabilities.hasTg ? '客户端已连接' : '浏览器访问'
            });
          } catch (err) {
            console.error('获取数据失败:', err);
            setSectionState(els.galleryContainer, 'error', { title: '收藏拉取失败', desc: '呜呜，连接数据库失败了喵...' });
            setSectionState(els.historyContainer, 'error', { title: '足迹拉取失败', desc: '呜呜，连接数据库失败了喵...' });
          }
        }

        async function removeHist(histId, btnElement) {
          if (!user) {
            showAlert('请在 Telegram 客户端内打开后再操作喵！');
            return;
          }
          await submitRemoval({
            endpoint: '/api/webapp/remove_hist',
            payload: { user_id: user.id, hist_id: histId },
            cardId: 'hist-item-' + histId,
            recordId: histId,
            btnElement,
            kind: 'history',
            failPrefix: '抹除失败：'
          });
        }

        async function removeFav(mediaId, btnElement) {
          if (!user) {
            showAlert('请在 Telegram 客户端内打开后再操作喵！');
            return;
          }
          await submitRemoval({
            endpoint: '/api/webapp/remove_fav',
            payload: { user_id: user.id, media_id: mediaId },
            cardId: 'fav-item-' + mediaId,
            recordId: mediaId,
            btnElement,
            kind: 'favorite',
            failPrefix: '移除失败：'
          });
        }

        const user = tg && tg.initDataUnsafe ? tg.initDataUnsafe.user : null;
        els.heroThemeChip.innerText = tg && tg.colorScheme ? ('主题：' + (tg.colorScheme === 'dark' ? '深色' : '浅色')) : '主题：默认';
        els.heroEnvChip.innerText = capabilities.hasTg ? '环境：Telegram' : '环境：浏览器';
        renderInitialStates();

        if (user) {
          els.welcomeText.innerText = '欢迎回来, ' + (user.first_name || '主人') + ' 喵！';
          fetchAppData(user.id);
        } else {
          els.welcomeText.innerText = '请在 Telegram 客户端内打开喵！';
          renderSettingsView({
            userName: '未识别用户',
            userId: '--',
            source: '受限模式',
            themeLabel: '默认主题',
            favoriteCount: 0,
            historyCount: 0,
            refreshedAt: '--:--',
            envLabel: '浏览器访问'
          });
          setSectionState(els.galleryContainer, 'empty', { icon: '🔒', title: '环境异常，无法获取身份信息', desc: '请从 Telegram 客户端中打开 WebApp 喵～' });
          setSectionState(els.historyContainer, 'empty', { icon: '🔒', title: '环境异常，无法获取身份信息', desc: '请从 Telegram 客户端中打开 WebApp 喵～' });
          updateSectionMeta('gallery', 0);
          updateSectionMeta('history', 0);
        }
      </script>
    </body>
    </html>
  `;
}

async function handleWebAppData(request, env) {
  if (request.method !== 'POST') return new Response('Method Not Allowed', { status: 405 });

  try {
    const body = await request.json();
    const userId = body.user_id; 

    if (!userId) {
      return new Response(JSON.stringify({ error: "未获取到用户身份" }), { status: 400 });
    }

        // 并发查询 (已修正表名为 user_history)
    const [mediaRes, viewRes, groupRes, favRes, histRes, antiRes, userRes] = await Promise.all([
      env.D1.prepare(`SELECT count(*) as c FROM media_library`).first(),
      env.D1.prepare(`SELECT SUM(view_count) as v FROM media_library`).first(),
      env.D1.prepare(`SELECT COUNT(DISTINCT chat_id) as g FROM config_topics WHERE chat_id < 0`).first(),
      env.D1.prepare(`
        SELECT f.media_id as id, m.media_type, m.caption, m.chat_id, m.message_id 
        FROM user_favorites f LEFT JOIN media_library m ON f.media_id = m.id 
        WHERE f.user_id = ? ORDER BY f.saved_at DESC LIMIT 20
      `).bind(userId).all(),
      env.D1.prepare(`
        SELECT h.id as id, m.media_type, m.caption, m.chat_id, m.message_id 
        FROM user_history h LEFT JOIN media_library m ON h.media_id = m.id 
        WHERE h.user_id = ? ORDER BY h.viewed_at DESC LIMIT 50
      `).bind(userId).all(),
      env.D1.prepare(`SELECT count(*) as c FROM served_history`).first(),
      // 🐛 修复核心：这里也必须换成 user_history
      env.D1.prepare(`SELECT u.user_id, r.first_name, count(*) as c FROM user_history u LEFT JOIN user_roster r ON u.user_id = r.user_id GROUP BY u.user_id ORDER BY c DESC LIMIT 5`).all()
    ]);


    const responseData = {
      dashboard: {
        total_media: mediaRes?.c || 0,
        total_views: viewRes?.v || 0,
        total_groups: groupRes?.g || 0,
        total_anti: antiRes?.c || 0
      },
      top_users: userRes.results || [],
      favorites: favRes.results || [],
      history: histRes.results || []
    };

    return new Response(JSON.stringify(responseData), { 
      headers: { 'Content-Type': 'application/json' } 
    });

  } catch (err) {
    console.error('Web App API Error:', err);
    return new Response(JSON.stringify({ error: "服务器内部错误" }), { status: 500 });
  }
}

async function handleWebAppRemoveFav(request, env) {
  if (request.method !== 'POST') return new Response('Method Not Allowed', { status: 405 });

  try {
    const body = await request.json();
    const userId = body.user_id; 
    const mediaId = body.media_id;

    if (!userId || !mediaId) {
      return new Response(JSON.stringify({ success: false, error: "参数不完整" }), { status: 400 });
    }

    await env.D1.prepare(`DELETE FROM user_favorites WHERE user_id = ? AND media_id = ?`).bind(userId, mediaId).run();

    return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    console.error('Web App Remove Fav Error:', err);
    return new Response(JSON.stringify({ success: false, error: "服务器内部错误" }), { status: 500 });
  }
}

/* =========================================================================
 * 工具、API 与 身份鉴权拦截
 * ========================================================================= */
async function getUserAllowedGroups(userId, env) {
  const { results } = await env.D1.prepare(`SELECT DISTINCT chat_id FROM config_topics WHERE chat_id < 0`).all();
  if (!results || results.length === 0) return [];

  const checks = results.map(row =>
    isUserInGroup(row.chat_id, userId, env).then(inGroup => inGroup ? row.chat_id : null)
  );
  return (await Promise.all(checks)).filter(id => id !== null);
}

async function isUserInGroup(groupId, userId, env) {
  const cacheKey = `${groupId}:${userId}`;
  const now = Date.now();
  const cached = groupMembershipCache.get(cacheKey);
  if (cached && cached.expiresAt > now) return cached.value;

  const res = await tgAPI('getChatMember', { chat_id: groupId, user_id: userId }, env);
  const data = await res.json();
  const inGroup = data.ok && ['creator', 'administrator', 'member', 'restricted'].includes(data.result.status);

  if (groupMembershipCache.size >= GROUP_MEMBER_CACHE_MAX) {
    groupMembershipCache.delete(groupMembershipCache.keys().next().value);
  }
  groupMembershipCache.set(cacheKey, { value: inGroup, expiresAt: now + GROUP_MEMBER_CACHE_TTL_MS });

  return inGroup;
}

async function handleExternalImport(dataBatch, env) {
  if (!dataBatch || !Array.isArray(dataBatch)) return;
  const stmts = dataBatch.map(item => {
    return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(item.message_id, item.chat_id || 0, item.topic_id || null, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption || '', Number.isInteger(item.duration) ? item.duration : null);
  });
  if (stmts.length > 0) await env.D1.batch(stmts);
}

async function tgAPI(method, payload, env) {
  return fetch(`https://api.telegram.org/bot${env.BOT_TOKEN_ENV}/${method}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload)
  });
}

async function getSetting(chatId, key, env) {
  const res = await env.D1.prepare(`SELECT value FROM chat_settings WHERE chat_id = ? AND key = ?`).bind(chatId, key).first();
  if (res) return res.value;
  return SETTING_DEFAULTS[key] ?? null;
}

// P1: 批量读取多个设置
async function getSettingsBatch(chatId, keys, env) {
  const uniqueKeys = [...new Set(keys)];
  const placeholders = uniqueKeys.map(() => '?').join(', ');
  const { results } = await env.D1.prepare(
    `SELECT key, value FROM chat_settings WHERE chat_id = ? AND key IN (${placeholders})`
  ).bind(chatId, ...uniqueKeys).all();
  const out = {};
  for (const k of uniqueKeys) out[k] = SETTING_DEFAULTS[k] ?? null;
  for (const row of (results || [])) out[row.key] = row.value;
  return out;
}

// 🌟 V5.9: 过滤器工具函数 ============================================================

// 白名单校验 + 降级处理
function normalizeFilters(raw = {}) {
  const out = Object.assign({}, FILTER_DEFAULTS, raw);
  if (!FILTER_MEDIA_TYPES.has(out.media_type))    out.media_type    = FILTER_DEFAULTS.media_type;
  if (!FILTER_DATE_MODES.has(out.date_mode))      out.date_mode     = FILTER_DEFAULTS.date_mode;
  if (!FILTER_DURATION_MODES.has(out.duration_mode)) out.duration_mode = FILTER_DEFAULTS.duration_mode;
  out.date_from = FILTER_DATE_RE.test(String(out.date_from || '')) ? String(out.date_from) : '';
  out.date_to   = FILTER_DATE_RE.test(String(out.date_to   || '')) ? String(out.date_to)   : '';
  const maxVal  = String(out.duration_max ?? '').trim();
  out.duration_max = /^(0|[1-9]\d*)$/.test(maxVal) ? maxVal : '';
  if (out.date_mode === 'custom' && (!out.date_from || !out.date_to || out.date_from > out.date_to)) {
    out.date_mode = 'all'; out.date_from = ''; out.date_to = '';
  }
  if (out.duration_mode === 'custom' && out.duration_max === '') out.duration_mode = 'all';
  return out;
}

// 判断过滤器是否被激活（任意维度非默认即激活）
function isFilterActive(filters) {
  const f = normalizeFilters(filters);
  return f.media_type !== 'all' || f.date_mode !== 'all' || f.duration_mode !== 'all';
}

// 构建安全 SQL WHERE 子句（所有用户值全部 bind，绝不拼接）
function buildFilterWhereClause(filters, alias = 'm') {
  const f = normalizeFilters(filters);
  const clauses = [];
  const binds = [];

  if (f.media_type !== 'all') {
    clauses.push(`AND ${alias}.media_type = ?`);
    binds.push(f.media_type);
  }
  switch (f.date_mode) {
    case 'today':
      clauses.push(`AND date(${alias}.added_at) = date('now')`); break;
    case 'd7':
      clauses.push(`AND datetime(${alias}.added_at) >= datetime('now', '-7 days')`); break;
    case 'd30':
      clauses.push(`AND datetime(${alias}.added_at) >= datetime('now', '-30 days')`); break;
    case 'year':
      clauses.push(`AND strftime('%Y', ${alias}.added_at) = strftime('%Y', 'now')`); break;
    case 'custom':
      clauses.push(`AND date(${alias}.added_at) >= date(?) AND date(${alias}.added_at) <= date(?)`);
      binds.push(f.date_from, f.date_to); break;
  }
  let durationMax = null;
  if (f.duration_mode === 'custom') {
    durationMax = parseInt(f.duration_max, 10);
  } else if (Object.prototype.hasOwnProperty.call(FILTER_DURATION_PRESET_MAP, f.duration_mode)) {
    durationMax = FILTER_DURATION_PRESET_MAP[f.duration_mode];
  }
  if (Number.isInteger(durationMax) && durationMax >= 0) {
    clauses.push(`AND ${alias}.duration IS NOT NULL AND ${alias}.duration <= ?`);
    binds.push(durationMax);
  }
  return { sql: clauses.length ? ` ${clauses.join(' ')}` : '', binds, normalized: f };
}

// 生成人类可读过滤状态文本
function renderFilterStatus(filters) {
  const f = normalizeFilters(filters);
  const mediaLabel = { all:'全部', photo:'仅图片', video:'仅视频', animation:'仅动图' }[f.media_type] || '全部';
  const dateLabel = f.date_mode === 'custom'
    ? `${f.date_from}~${f.date_to}`
    : ({ all:'不限', today:'今天', d7:'近7天', d30:'近30天', year:'今年' }[f.date_mode] || '不限');
  const durLabel = f.duration_mode === 'custom'
    ? `≤${f.duration_max}s`
    : ({ all:'不限', s30:'≤30s', s60:'≤60s', s120:'≤120s', s300:'≤5分钟' }[f.duration_mode] || '不限');
  return `类型:${mediaLabel} | 时间:${dateLabel} | 时长:${durLabel}`;
}

// 读取用户过滤器（仿 getSettingsBatch）
async function getUserFiltersBatch(userId, chatId, env) {
  const keys = Object.keys(FILTER_DEFAULTS);
  const placeholders = keys.map(() => '?').join(', ');
  const { results } = await env.D1.prepare(
    `SELECT key, value FROM user_filters WHERE user_id = ? AND chat_id = ? AND key IN (${placeholders})`
  ).bind(userId, chatId, ...keys).all();
  const out = Object.assign({}, FILTER_DEFAULTS);
  for (const row of (results || [])) {
    if (Object.prototype.hasOwnProperty.call(out, row.key)) out[row.key] = row.value ?? '';
  }
  return normalizeFilters(out);
}

// 写入用户过滤器（单键 upsert）
async function upsertUserFilter(userId, chatId, key, value, env) {
  if (!Object.prototype.hasOwnProperty.call(FILTER_DEFAULTS, key)) throw new Error(`Invalid filter key: ${key}`);
  const v = value == null ? '' : String(value);
  await env.D1.prepare(
    `INSERT INTO user_filters (user_id, chat_id, key, value) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, chat_id, key) DO UPDATE SET value = excluded.value`
  ).bind(userId, chatId, key, v).run();
}

// 删除用户的所有过滤器（重置）
async function resetUserFilters(userId, chatId, env) {
  await env.D1.prepare(`DELETE FROM user_filters WHERE user_id = ? AND chat_id = ?`).bind(userId, chatId).run();
}

// ====================================================================================

// 🌟 V5.7: 批量删除工具函数（每批 20 条 × 5 表 = 100 语句，不超 D1.batch 上限）
async function batchDeleteMediaByIds(ids, env) {
  let deleted = 0;
  for (let i = 0; i < ids.length; i += 20) {
    const chunk = ids.slice(i, i + 20);
    const stmts = chunk.flatMap(id => [
      env.D1.prepare(`DELETE FROM media_library WHERE id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM user_favorites WHERE media_id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM user_history WHERE media_id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM group_history WHERE media_id = ?`).bind(id)
    ]);
    await env.D1.batch(stmts);
    deleted += chunk.length;
  }
  return deleted;
}

// 🌟 V5.7: 批量转移工具函数（每批 50 条 UPDATE）
async function batchMoveMediaByIds(ids, targetCategory, env) {
  let moved = 0;
  for (let i = 0; i < ids.length; i += 50) {
    const chunk = ids.slice(i, i + 50);
    const stmts = chunk.map(id =>
      env.D1.prepare(`UPDATE media_library SET category_name = ? WHERE id = ?`).bind(targetCategory, id)
    );
    await env.D1.batch(stmts);
    moved += chunk.length;
  }
  return moved;
}

// 终极随机策略：内存映射随机（彻底解决 ID 断层导致的概率黑洞）
async function selectRandomMedia(category, sourceChatId, useAntiRepeat, excludeId, filters, env) {
  const antiClause  = useAntiRepeat ? `AND NOT EXISTS (SELECT 1 FROM served_history sh WHERE sh.media_id = m.id)` : '';
  const excludeClause = excludeId ? `AND m.id != ?` : '';
  const { sql: filterSql, binds: filterBinds } = buildFilterWhereClause(filters, 'm');
  const binds = [category, sourceChatId];
  if (excludeId) binds.push(excludeId);
  binds.push(...filterBinds);

  const { results } = await env.D1.prepare(
    `SELECT m.id FROM media_library m WHERE m.category_name = ? AND m.chat_id = ? ${antiClause} ${excludeClause}${filterSql}`
  ).bind(...binds).all();

  if (!results || results.length === 0) return null;

  const randomIdx = Math.floor(Math.random() * results.length);
  const targetId = results[randomIdx].id;

  return await env.D1.prepare(
    `SELECT * FROM media_library WHERE id = ?`
  ).bind(targetId).first();
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

async function handleWebAppRemoveHist(request, env) {
  if (request.method !== 'POST') return new Response('Method Not Allowed', { status: 405 });
  try {
    const body = await request.json();
    if (!body.user_id || !body.hist_id) return new Response(JSON.stringify({ success: false, error: "参数不完整" }), { status: 400 });
    
    await env.D1.prepare(`DELETE FROM user_history WHERE user_id = ? AND id = ?`).bind(body.user_id, body.hist_id).run();
    return new Response(JSON.stringify({ success: true }), { headers: { 'Content-Type': 'application/json' } });
  } catch (err) {
    return new Response(JSON.stringify({ success: false, error: "服务器内部错误" }), { status: 500 });
  }
}

async function fetchWithRetry(url, options, retries = 3, backoff = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      const controller = new AbortController();
      // 设定 5 秒超时，如果 Telegram 卡住不理人，就强行打断
      const timeoutId = setTimeout(() => controller.abort(), 5000);
      const response = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timeoutId);

      if (response.ok) {
        return response; // 成功啦！
      }
      
      // 如果触发了 Telegram 的限频限制 (429 Too Many Requests)
      if (response.status === 429) {
        const retryAfter = response.headers.get('Retry-After') || 5;
        const delay = parseInt(retryAfter) * 1000;
        console.warn(`⚠️ 触发 TG 限流，籽青乖乖等待 ${delay}ms 后重试喵...`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      
      throw new Error(`HTTP 报错状态码: ${response.status}`);
    } catch (error) {
      if (i === retries - 1) throw error; // 如果最后一次也失败了，就真的报错
      
      // 指数退避策略：失败后等待时间翻倍 (1秒 -> 2秒 -> 4秒...)
      const waitTime = backoff * Math.pow(2, i);
      console.warn(`⚠️ 请求失败 (${error.message})，籽青将在 ${waitTime}ms 后进行第 ${i + 1} 次冲锋喵！`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
  throw new Error(`呜呜，在 ${retries} 次努力后还是失败了喵：${url}`);
}