/**
 * Cloudflare Workers (Pages) - Telegram Bot Entry Point (V5.15)
 * 核心升级：跨群分类合并 + 源群成员校验开关 + /promote 跨群归档 + 附近媒体浏览。
 *   - 同名分类合并为单按钮( Map 聚合去重),源群选择按来源区分
 *   - 展示群可配置关闭源群成员校验(分发模式,B群陌生人可看A源,默认安全)
 *   - /promote 回复B群媒体归档到A群(forwardMessage保留第三方转发来源)
 *   - 命令缩写并存(/bout /bsrc /ubsrc /prom /ptgt /ij)
 *   - 显示群(B/C)可经 /bind_source 拉取源群(A)媒体,用户多源筛选(只看A/A+C等)
 *   - 投票达阈值按展示群隐藏(media_hide),不破坏源群数据;管理员可选源库删除
 *   - 共享群自身亦可收录资源,既是展示群也是源群
 *   - V5.15: 抽取/历史回退键盘常驻「📍 查看附近5个」(同源群·同分类 message_id 邻域)
 * V5.13 零新增表/索引/触发器,仅 chat_settings 新增 source_membership_check/promote_target 逻辑键。
 * V5.14 零新增表/索引/触发器,仅 chat_settings 新增 expose_forward_source 逻辑键；extractForwardSourceDeepLink helper。
 * V5.15 零新增表/索引/触发器；queryNearbyMedia + showNearbyMedia + near| 回调。
 */

/* =========================================================================
 * 模块级常量与缓存（Cloudflare Worker 实例级别,跨请求共享）
 * ========================================================================= */

// 🌟 V5.9: 随机抽取过滤器默认值
// 🌟 V5.10: 新增 sender_user_id 维度
const FILTER_DEFAULTS = Object.freeze({
  media_type:    'all',   // all | photo | video | animation
  date_mode:     'all',   // all | today | d7 | d30 | year | after | before | custom
  date_from:     '',      // YYYY-MM-DD（date_mode=custom/after 有效）
  date_to:       '',      // YYYY-MM-DD（date_mode=custom/before 有效）
  duration_mode: 'all',   // all | s30 | s60 | s120 | s300 | gt | lt | range | custom
  duration_min:  '',      // 整数秒字符串（duration_mode=gt/range 有效）
  duration_max:  '',      // 整数秒字符串（duration_mode=lt/range/custom 有效）
  sender_user_id: ''      // '' (不限) | 正整数 (限定发送者) | 'null' (默认资源/历史数据)
});
const FILTER_DATE_RE = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/;
const FILTER_MEDIA_TYPES     = new Set(['all', 'photo', 'video', 'animation']);
const FILTER_DATE_MODES      = new Set(['all', 'today', 'd7', 'd30', 'year', 'after', 'before', 'custom']);
const FILTER_DURATION_MODES  = new Set(['all', 's30', 's60', 's120', 's300', 'gt', 'lt', 'range', 'custom']);
const FILTER_DURATION_PRESET_MAP = Object.freeze({ s30: 30, s60: 60, s120: 120, s300: 300 });

const SETTING_DEFAULTS = Object.freeze({
  display_mode: 'B',
  anti_repeat: 'true',
  auto_jump: 'true',
  dup_notify: 'false',
  show_success: 'true',
  next_mode: 'replace',
  strict_skip: 'false', // 🌟 默认不是严格模式（放回池子）
  // 🌟 V5.11: 跨群共享 + 投票相关设置（均以展示群 chat_id 为键）
  vote_enabled: 'false',   // 投票按钮开关，默认关闭（新功能不影响现有群）
  vote_threshold: '5',     // 投票移除阈值（正整数字符串）
  vote_scope: 'hide',      // 'hide'(按展示群隐藏,默认安全) | 'delete'(源库物理删除,危险开关)
  ingest_enabled: 'true',  // 本群是否收录投递媒体，true=双向(默认向后兼容) false=仅展示
  // 🌟 V5.13: 跨群归档 + 源群成员校验开关（均以展示群 chat_id 为键）
  source_membership_check: 'true',  // 源群成员校验(展示群) true=用户须在源群(默认安全) false=无需在源群(分发模式,高危)
  promote_target: '',                // /promote 默认目标A群ID（空=未设置，用 /promote_target 绑定）
  // 🌟 V5.14: 暴露第三方转发来源深链开关（展示群）false=默认安全不暴露来源 true=B模式显示「看看来源」按钮
  expose_forward_source: 'false'
});

// 成员资格 TTL 缓存（60秒）
const GROUP_MEMBER_CACHE_TTL_MS = 60_000;
const GROUP_MEMBER_CACHE_MAX = 4096;
const groupMembershipCache = new Map();

// Bot 自身 username 缓存（实例级，跨请求共享；username 不会变更，命中后长期有效）
// 用于解析群组命令的 @机器人名 后缀，避免抢答其他机器人的指令
let botUsernameCache = null;
// 🌟 V5.11: Bot 自身 user_id 缓存（用于 /bind_source 校验 bot 是否在源群）
let botUserIdCache = null;

function isCancelInput(input, botUsername) {
  if (!input) return false;
  const trimmed = input.trim();
  if (trimmed === '取消') return true;

  const match = trimmed.match(/^\/cancel(?:@([A-Za-z0-9_]+))?$/i);
  if (!match) return false;

  const mentionedBot = match[1];
  if (!mentionedBot) return true;
  if (!botUsername) return true;
  return mentionedBot.toLowerCase() === botUsername.toLowerCase();
}

// 🌟 V5.13: commandName 支持字符串或别名数组（缩写并存），如 ['bind_output', 'bout']
function isAddressedBotCommand(text, commandName, botUsername) {
  if (!text || !commandName) return false;

  const match = text.match(/^\/([A-Za-z0-9_]+)(?:@([A-Za-z0-9_]+))?(?=\s|$)/);
  if (!match) return false;

  const command = match[1].toLowerCase();
  const names = Array.isArray(commandName) ? commandName : [commandName];
  if (!names.some(n => n.toLowerCase() === command)) return false;

  const mentionedBot = match[2];
  if (!mentionedBot) return true;
  // 无法确认 @目标是否为自己时保守忽略，避免抢答其他机器人的指令。
  if (!botUsername) return false;
  return mentionedBot.toLowerCase() === botUsername.toLowerCase();
}

// 精确匹配无参数命令：/cmd 或 /cmd@bot（结尾必须是 $，不允许后续参数）
// 用于 /bd /bmv 这类带子命令的命令，避免把 /bd end 误判为 /bd
// 🌟 V5.13: commandName 支持字符串或别名数组
function matchesBotCommandExact(text, commandName, botUsername) {
  if (!text || !commandName) return false;

  const match = text.match(/^\/([A-Za-z0-9_]+)(?:@([A-Za-z0-9_]+))?$/i);
  if (!match) return false;
  const command = match[1].toLowerCase();
  const names = Array.isArray(commandName) ? commandName : [commandName];
  if (!names.some(n => n.toLowerCase() === command)) return false;

  const mentionedBot = match[2];
  if (!mentionedBot) return true;
  if (!botUsername) return false;
  return mentionedBot.toLowerCase() === botUsername.toLowerCase();
}

// 匹配带固定子命令的命令：/cmd arg、/cmd@bot arg（arg 为字面子命令如 'end'/'cancel'）
// 🌟 V5.13: commandName 支持字符串或别名数组
function matchesBotCommandWithArg(text, commandName, arg, botUsername) {
  if (!text || !commandName || arg == null) return false;
  const escArg = String(arg).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const names = Array.isArray(commandName) ? commandName : [commandName];
  const altPattern = names.map(n => n.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  const match = text.match(new RegExp('^\\/(?:' + altPattern + ')(?:@([A-Za-z0-9_]+))?\\s+' + escArg + '$', 'i'));
  if (!match) return false;
  const mentionedBot = match[1];
  if (!mentionedBot) return true;
  if (!botUsername) return false;
  return mentionedBot.toLowerCase() === botUsername.toLowerCase();
}

// 获取 Bot 自身 username：优先环境变量 BOT_USERNAME，其次实例缓存，最后 getMe 拉取并缓存
async function getBotUsername(env) {
  if (env.BOT_USERNAME) return env.BOT_USERNAME;
  if (botUsernameCache) return botUsernameCache;
  try {
    const res = await tgAPI('getMe', {}, env);
    const data = await res.json();
    if (data?.ok && data?.result?.username) {
      botUsernameCache = data.result.username;
      // 🌟 V5.11: 顺带缓存 bot user_id
      if (data?.result?.id) botUserIdCache = data.result.id;
      return botUsernameCache;
    }
  } catch (e) {
    console.warn('getMe 获取 bot username 失败:', e?.message);
  }
  return null;
}

// 🌟 V5.11: 获取 Bot 自身 user_id（用于校验 bot 是否在源群，/bind_source 绑定时校验）
async function getBotUserId(env) {
  if (botUserIdCache) return botUserIdCache;
  try {
    const res = await tgAPI('getMe', {}, env);
    const data = await res.json();
    if (data?.ok && data?.result?.id) {
      botUserIdCache = data.result.id;
      if (data?.result?.username) botUsernameCache = data.result.username;
      return botUserIdCache;
    }
  } catch (e) {
    console.warn('getMe 获取 bot user_id 失败:', e?.message);
  }
  return null;
}

function safeJSONStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (err) {
    console.warn('JSON 序列化失败:', err?.message);
    return null;
  }
}

function safeJSONParse(text) {
  if (!text || typeof text !== 'string') return null;
  try {
    return JSON.parse(text);
  } catch (err) {
    console.warn('JSON 解析失败:', err?.message);
    return null;
  }
}

function deepMergeJSONValue(currentValue, nextValue) {
  if (nextValue == null) return currentValue ?? null;
  if (currentValue == null) return nextValue;

  if (Array.isArray(nextValue)) {
    return nextValue.length > 0 ? nextValue : (Array.isArray(currentValue) ? currentValue : nextValue);
  }
  if (Array.isArray(currentValue)) return currentValue.length > 0 ? currentValue : nextValue;

  if (typeof currentValue !== 'object' || typeof nextValue !== 'object') {
    if (typeof nextValue === 'string' && nextValue === '' && typeof currentValue === 'string' && currentValue !== '') {
      return currentValue;
    }
    return nextValue;
  }

  const merged = { ...currentValue };
  for (const [key, value] of Object.entries(nextValue)) {
    merged[key] = key in merged ? deepMergeJSONValue(merged[key], value) : value;
  }
  return merged;
}

function serializeStoredMediaPayload(source, messageLike) {
  if (!messageLike || typeof messageLike !== 'object') return null;
  return safeJSONStringify({ source, message: messageLike });
}

function normalizeStoredMediaPayload(rawPayload, fallbackSource = 'external_import') {
  if (rawPayload == null) return null;
  if (typeof rawPayload === 'string') {
    const parsed = safeJSONParse(rawPayload);
    return parsed ? safeJSONStringify(parsed) : safeJSONStringify({ source: fallbackSource, message: rawPayload });
  }
  return safeJSONStringify(rawPayload);
}

function mergeStoredMediaPayload(currentPayload, nextPayload) {
  const currentParsed = safeJSONParse(currentPayload);
  const nextParsed = safeJSONParse(nextPayload);
  if (!currentParsed) return nextPayload || currentPayload || null;
  if (!nextParsed) return currentPayload || nextPayload || null;
  return safeJSONStringify(deepMergeJSONValue(currentParsed, nextParsed)) || nextPayload || currentPayload || null;
}

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

      // 🌟 V5.9+: 批量回填历史媒体元数据（duration / raw_message_json）
      if (request.method === 'POST' && url.pathname === '/api/update_duration') {
        const secret = request.headers.get('Authorization');
        if (env.ADMIN_SECRET && secret !== env.ADMIN_SECRET) return new Response('Unauthorized', { status: 401 });
        const payload = await request.json();
        const updates = payload.updates || [];
        if (!Array.isArray(updates) || updates.length === 0) {
          return new Response(JSON.stringify({ status: 'error', message: 'No updates provided' }), { status: 400 });
        }
        let updatedCount = 0;
        for (let i = 0; i < updates.length; i += 50) {
          const batch = updates.slice(i, i + 50);
          const stmts = batch.map(item => {
            const messageId = Number(item.message_id);
            const chatId = Number(item.chat_id);
            const duration = Number.isInteger(item.duration) ? item.duration : null;
            const normalizedRawMessageJson = normalizeStoredMediaPayload(
              item.raw_message_json ?? item.raw_message ?? item.raw_message_data ?? item.telegram_message,
              'telegram_desktop_export'
            );
            if (!Number.isInteger(messageId) || !Number.isInteger(chatId)) return null;
            if (duration === null && normalizedRawMessageJson === null) return null;
            return env.D1.prepare(
              `UPDATE media_library
               SET duration = COALESCE(duration, ?),
                   raw_message_json = COALESCE(raw_message_json, ?)
               WHERE message_id = ? AND chat_id = ?`
            ).bind(duration, normalizedRawMessageJson, messageId, chatId);
          }).filter(Boolean);
          if (stmts.length === 0) continue;
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
      `CREATE TABLE IF NOT EXISTS media_library (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER, chat_id INTEGER, topic_id INTEGER, category_name TEXT, view_count INTEGER DEFAULT 0, file_unique_id TEXT, file_id TEXT, media_type TEXT, caption TEXT, duration INTEGER DEFAULT NULL, raw_message_json TEXT, added_at DATETIME DEFAULT CURRENT_TIMESTAMP);`,
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
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_batch_session_user ON batch_sessions (chat_id, user_id);`,

      // 🌟 V5.11: 跨群共享 — 管理员源群白名单（显示群可拉取哪些源群）
      `CREATE TABLE IF NOT EXISTS group_sources (display_chat_id INTEGER NOT NULL, source_chat_id INTEGER NOT NULL, source_chat_title TEXT, added_by INTEGER, added_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(display_chat_id, source_chat_id));`,
      `CREATE INDEX IF NOT EXISTS idx_group_sources_display ON group_sources (display_chat_id);`,
      `CREATE INDEX IF NOT EXISTS idx_group_sources_source ON group_sources (source_chat_id);`,

      // 🌟 V5.11: 用户在显示群内的源群多选（独立于 user_filters，因为是集合而非标量维度）
      `CREATE TABLE IF NOT EXISTS user_source_selection (user_id INTEGER NOT NULL, display_chat_id INTEGER NOT NULL, source_chat_id INTEGER NOT NULL, selected_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, display_chat_id, source_chat_id));`,
      `CREATE INDEX IF NOT EXISTS idx_user_source_sel_user_display ON user_source_selection (user_id, display_chat_id);`,

      // 🌟 V5.11: 投票记录（防重复投票 + 审计）
      `CREATE TABLE IF NOT EXISTS media_votes (media_id INTEGER NOT NULL, user_id INTEGER NOT NULL, voted_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(media_id, user_id));`,
      `CREATE INDEX IF NOT EXISTS idx_media_votes_media ON media_votes (media_id);`,

      // 🌟 V5.11: 按展示群隐藏标记（B群投票只隐藏B群抽取，源群数据完整保留）
      `CREATE TABLE IF NOT EXISTS media_hide (display_chat_id INTEGER NOT NULL, media_id INTEGER NOT NULL, hidden_at DATETIME DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(display_chat_id, media_id));`,
      `CREATE INDEX IF NOT EXISTS idx_media_hide_display ON media_hide (display_chat_id);`,
      `CREATE INDEX IF NOT EXISTS idx_media_hide_media ON media_hide (media_id);`,

      // 🌟 V5.11: 投票计数触发器（维护 media_library.vote_count 退化列，O(1)阈值检查）
      `CREATE TRIGGER IF NOT EXISTS trg_vote_inc AFTER INSERT ON media_votes BEGIN UPDATE media_library SET vote_count = vote_count + 1 WHERE id = NEW.media_id; END;`,
      `CREATE TRIGGER IF NOT EXISTS trg_vote_dec AFTER DELETE ON media_votes BEGIN UPDATE media_library SET vote_count = MAX(0, vote_count - 1) WHERE id = OLD.media_id; END;`
    ];

    for (const sql of initSQL) await env.D1.prepare(sql).run();

    // 🌟 V5.9: 幂等列迁移（PRAGMA 检查 + try/catch 双保险）
    const migrateColumns = [
      { name: 'file_unique_id', type: 'TEXT' },
      { name: 'file_id',        type: 'TEXT' },
      { name: 'media_type',     type: 'TEXT' },
      { name: 'caption',        type: 'TEXT' },
      { name: 'duration',       type: 'INTEGER DEFAULT NULL' },
      { name: 'raw_message_json', type: 'TEXT' },
      { name: 'sender_user_id',   type: 'INTEGER DEFAULT NULL' },
      // 🌟 V5.11: 投票计数退化列（触发器维护，用于 O(1) 阈值检查与按钮文案）
      { name: 'vote_count',      type: 'INTEGER DEFAULT 0' }
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

    // 🌟 V5.10: sender_user_id 列相关索引（必须在列迁移之后创建）
    try {
      await env.D1.prepare(`CREATE INDEX IF NOT EXISTS idx_media_chat_sender ON media_library (chat_id, sender_user_id);`).run();
    } catch (e) {
      console.warn('sender_user_id 索引创建跳过:', e?.message);
    }

    // 🌟 V5.11: vote_count 列相关索引（必须在列迁移之后创建）
    try {
      await env.D1.prepare(`CREATE INDEX IF NOT EXISTS idx_media_chat_cat_vote ON media_library (chat_id, category_name, vote_count);`).run();
    } catch (e) {
      console.warn('vote_count 索引创建跳过:', e?.message);
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
          <h1>🎉 籽青 V5.15 满血上线！</h1>
          <p>跨群分类合并 + /promote 归档 + 分发模式开关喵～<br>Webhook 已经帮主人狠狠地绑死啦：</p>
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

  // 解析本机 username（getMe 缓存），用于群组命令的 @后缀 路由
  const botUsername = await getBotUsername(env);

  if (isAddressedBotCommand(text, 'start', botUsername)) return sendMainMenu(chatId, topicId, env, userId);

  if (isAddressedBotCommand(text, 'help', botUsername)) {
    const helpText = `<b>🐱 籽青 V5.15 — 说明手册</b>

<b>━━━ 🎲 基础 ━━━</b>
/start — 呼出主控制面板

<b>━━━ ⚙️ 管理员 ━━━</b>
<code>/bind</code> &lt;分类名&gt; — 绑定当前话题为采集库
/bout — 设置当前话题为输出展示窗口
/bsrc — 绑定源群白名单（回复源消息 或 /bsrc &lt;ID|@用户名&gt;）
/ubsrc — 解绑源群（同上格式）
/ij — 获取历史导入帮助

<b>━━━ 📤 跨群归档 V5.13 ━━━</b>
/ptgt &lt;A群&gt; — 设置提升目标储存群
/prom — 回复B群媒体，提升归档到A群（保留转发来源）

<b>━━━ 🖼️ 快捷管理（回复媒体+命令） ━━━</b>
/d — 彻底抹除
/mv — 转移到其他分类
/list — 查看收录信息与来源链路
/list debug — 查看回填与原始元数据

<b>━━━ 📋 批量操作 ━━━</b>
<code>/d</code> &lt;数量|all&gt; — 批量删除最近N条
<code>/mv</code> &lt;数量|all&gt; &lt;分类&gt; — 批量转移
/bd — 精确批量删除（转发选择）
/bmv — 精确批量转移（转发选择）

<b>━━━ 📡 跨群共享 V5.11 ━━━</b>
展示群经 /bsrc 拉取源群资源，筛选器切换源群
· 分享群可双向收录，投票移除在设置面板开启
· 分类列表自动合并同名，源群可按来源灵活切换
· 可关闭源群成员校验（分发模式），陌生人可看A源`;

    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: helpText, parse_mode: 'HTML' }, env);
    return;
  }

  if (isAddressedBotCommand(text, ['import_json', 'ij'], botUsername)) {
    const importHelp = `📥 **关于导入历史数据喵**\n\n籽青有两种方法可以吃掉历史数据哦：\n\n1. **直接投喂 (适合 5MB 以内的小包裹)**：直接把 \`.json\` 文件发给籽青,并在文件的说明(Caption)里写上 \`/import 分类名\` 即可！\n2. **脚本投喂 (适合大包裹)**：在电脑上运行配套的 Python 导入脚本,慢慢喂给籽青！QwQ`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: importHelp, parse_mode: 'Markdown' }, env);
    return;
  }

  // 🌟 V5.7: /bd 批量删除会话模式（必须在 /bind 之前，精确匹配，支持 @机器人名 后缀）
  if (matchesBotCommandExact(text, 'bd', botUsername)) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 只有管理员才能使用批量模式哦！" }, env);
    }
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
    await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode) VALUES (?, ?, 'bd')`).bind(chatId, userId).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🗑️ 已进入**批量删除模式**喵！\n\n请把要删除的媒体转发给籽青～\n每收到一条籽青会确认收集。\n\n完成后发送 `/bd end` 确认删除\n取消请发送 `/bd cancel`\n⏰ 5分钟后自动过期", parse_mode: 'Markdown' }, env);
  }

  if (matchesBotCommandWithArg(text, 'bd', 'end', botUsername)) {
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

  if (matchesBotCommandWithArg(text, 'bd', 'cancel', botUsername)) {
    const session = await env.D1.prepare(`SELECT id FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bd'`).bind(chatId, userId).first();
    if (!session) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "当前没有进行中的批量删除操作喵～" }, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "已退出批量删除模式喵～" }, env);
  }

  // 🌟 V5.7: /bmv 批量转移会话模式（精确匹配，支持 @机器人名 后缀）
  if (matchesBotCommandExact(text, 'bmv', botUsername)) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 只有管理员才能使用批量模式哦！" }, env);
    }
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
    await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode) VALUES (?, ?, 'bmv')`).bind(chatId, userId).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🔀 已进入**批量转移模式**喵！\n\n请把要转移的媒体转发给籽青～\n\n完成后发送 `/bmv end` 选择目标分类\n取消请发送 `/bmv cancel`\n⏰ 5分钟后自动过期", parse_mode: 'Markdown' }, env);
  }

  if (matchesBotCommandWithArg(text, 'bmv', 'end', botUsername)) {
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

  if (matchesBotCommandWithArg(text, 'bmv', 'cancel', botUsername)) {
    const session = await env.D1.prepare(`SELECT id FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode = 'bmv'`).bind(chatId, userId).first();
    if (!session) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "当前没有进行中的批量转移操作喵～" }, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(session.id).run();
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "已退出批量转移模式喵～" }, env);
  }

  // 🌟 V5.8+: /list — 查询回复媒体的收录记录（支持 /list debug 与 @机器人名 后缀）
  if (message.reply_to_message && isAddressedBotCommand(text, 'list', botUsername)) {
    const isListDebug = /^\/list(?:@[^\s]+)?\s+debug$/i.test(text.trim());
    const replyMsg = message.reply_to_message;
    const info = extractMediaInfo(replyMsg);
    const mediaSelectSQL = `SELECT id, chat_id, message_id, topic_id, category_name, media_type, duration, view_count, added_at, raw_message_json, file_unique_id FROM media_library`;
    let mediaRecords = [];
    let lookupMode = 'file_unique_id';

    if (info.fileUniqueId) {
      // 🌟 V5.13: 跨源查询——展示群+绑定的远程源群（B群/list也能看到A群收录记录）
      const displaySources = await getDisplaySources(chatId, env, userId);
      const srcIds = displaySources.map(r => r.source_chat_id);
      const srcPlaceholders = srcIds.map(() => '?').join(', ');
      const lookupByFile = await env.D1.prepare(
        `${mediaSelectSQL} WHERE file_unique_id = ? AND chat_id IN (${srcPlaceholders}) ORDER BY added_at ASC`
      ).bind(info.fileUniqueId, ...srcIds).all();
      mediaRecords = lookupByFile.results || [];
    }

    if (mediaRecords.length === 0 && Number.isInteger(replyMsg.message_id)) {
      const srcIds = mediaRecords.length === 0 ? (await getDisplaySources(chatId, env, userId)).map(r => r.source_chat_id) : [];
      if (srcIds.length > 0) {
        const srcPlaceholders = srcIds.map(() => '?').join(', ');
        const lookupByMessage = await env.D1.prepare(
          `${mediaSelectSQL} WHERE message_id = ? AND chat_id IN (${srcPlaceholders}) ORDER BY added_at ASC`
        ).bind(replyMsg.message_id, ...srcIds).all();
        mediaRecords = lookupByMessage.results || [];
      }
      if (mediaRecords.length > 0) lookupMode = 'message_id';
    }

    if (!info.fileUniqueId && mediaRecords.length === 0) {
      return tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id,
        text: "喵？这不是一条媒体消息哦，请回复一张图片或视频再试试！"
      }, env);
    }

    if (mediaRecords.length === 0) {
      return tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id,
        text: "呜呜，籽青在库里找不到这个媒体的收录记录喵，可能从未被收录过，或历史导入记录还没对上当前消息 ID 哦～"
      }, env);
    }

    const uniqueTopicIds = [...new Set(mediaRecords.map(r => r.topic_id).filter(t => t != null))];
    // 🌟 V5.13: 跨源场景 — 构建 chat_id→群名 映射 + topic_id→分类 映射
    const uniqueChatIds = [...new Set(mediaRecords.map(r => r.chat_id))];
    const chatTitleMap = {};
    const topicNameMap = {};
    // 并行查群名和话题分类
    if (uniqueChatIds.length > 0) {
      const chPh = uniqueChatIds.map(() => '?').join(',');
      const { results: chatRows } = await env.D1.prepare(
        `SELECT chat_id, chat_title FROM config_topics WHERE chat_id IN (${chPh}) GROUP BY chat_id`
      ).bind(...uniqueChatIds).all();
      for (const row of (chatRows || [])) chatTitleMap[row.chat_id] = row.chat_title || `群${row.chat_id}`;
    }
    if (uniqueTopicIds.length > 0) {
      const ph = uniqueTopicIds.map(() => '?').join(',');
      const { results: topicRows } = await env.D1.prepare(
        `SELECT topic_id, category_name FROM config_topics WHERE topic_id IN (${ph}) AND category_name != 'output' LIMIT 50`
      ).bind(...uniqueTopicIds).all();
      for (const row of (topicRows || [])) topicNameMap[row.topic_id] = row.category_name;
    }

    // 🌟 V5.13: 构建链式关系 — 按 file_unique_id + chat_id 分组，标注收录链路
    //   同一 file_unique_id 出现在多个群 = "源群收录 → 展示群收录" 或 "/promote 归档" 关系
    const chainLabel = (rec, idx) => {
      const chatLabel = chatTitleMap[rec.chat_id] || `群${rec.chat_id}`;
      if (mediaRecords.length === 1) return `📌 ${chatLabel}`;  // 仅一处的收录=当前查看的表的单条
      // 多群记录：按 added_at 排序，最早的是原始收录，后面的是回音/归记录
      const sorted = [...mediaRecords].sort((a, b) => new Date(a.added_at) - new Date(b.added_at));
      const first = sorted[0];
      const isOrigin = rec.id === first.id;
      const recChatLabel = chatTitleMap[rec.chat_id] || `群${rec.chat_id}`;
      const originChatLabel = chatTitleMap[first.chat_id] || (
        first.chat_id < 0 ? `群${first.chat_id}` : '私聊'
      );
      if (isOrigin) return `🎯 ${recChatLabel} （原始收录）`;
      // 后续记录：标注与原始的关系
      if (rec.chat_id !== first.chat_id) {
        return `📤 ${recChatLabel} ← 归档自 ${originChatLabel}`;
      }
      return `📌 ${recChatLabel}`;
    };

    const topicBound = (rec) => rec.topic_id ? (topicNameMap[rec.topic_id] || '未知话题') : '无话题';

    let fileSize = null;
    if (replyMsg.video?.file_size) fileSize = replyMsg.video.file_size;
    else if (replyMsg.animation?.file_size) fileSize = replyMsg.animation.file_size;
    else if (replyMsg.document?.file_size) fileSize = replyMsg.document.file_size;
    else if (replyMsg.photo?.length > 0) fileSize = replyMsg.photo[replyMsg.photo.length - 1].file_size;

    const formatSize = (bytes) => {
      if (!bytes) return null;
      if (bytes < 1024) return `${bytes} B`;
      if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
      if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
      return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
    };

    const formatDuration = (sec) => {
      if (sec == null) return null;
      const m = Math.floor(sec / 60);
      const s = sec % 60;
      return m > 0 ? `${m}分${s}秒` : `${s}秒`;
    };

    const chatIdNum = String(chatId).replace(/^-100/, '');
    const typeLabel = { photo: '🖼️ 图片', video: '🎬 视频', animation: '🎠 GIF', document: '📄 文件' };
    const firstRec = mediaRecords[0];
    const mediaType = typeLabel[firstRec.media_type] || firstRec.media_type || '未知';
    const sizePart = fileSize ? `📦 大小：${formatSize(fileSize)}` : '';
    const durationPart = firstRec.duration != null ? `⏱ 时长：${formatDuration(firstRec.duration)}` : '';
    const totalViews = mediaRecords.reduce((sum, r) => sum + (r.view_count || 0), 0);

    let summaryLine = `📊 *媒体概要*\n${mediaType}`;
    if (sizePart) summaryLine += ` | ${sizePart}`;
    if (durationPart) summaryLine += ` | ${durationPart}`;
    summaryLine += `\n👁 总浏览：${totalViews} 次`;

    if (isListDebug) {
      const rawPayload = safeJSONParse(firstRec.raw_message_json);
      const rawMessage = rawPayload && typeof rawPayload === 'object' ? rawPayload.message : null;
      const rawSource = rawPayload && typeof rawPayload === 'object' ? rawPayload.source : null;
      const debugLines = [
        '🔍 籽青 Debug 已展开喵～',
        '',
        `媒体概要: ${mediaType}${sizePart ? ` | ${sizePart}` : ''}${durationPart ? ` | ${durationPart}` : ''}`,
        `总浏览: ${totalViews} 次`,
        '',
        '🧪 回填调试',
        `- 查找方式: ${lookupMode}`,
        `- DB记录ID: ${firstRec.id}`,
        `- message_id: ${firstRec.message_id}`,
        `- 当前回复消息ID: ${replyMsg.message_id}`,
        `- file_unique_id: ${firstRec.file_unique_id || info.fileUniqueId || 'unknown'}`,
        `- duration列: ${firstRec.duration != null ? `${firstRec.duration} 秒` : 'NULL'}`,
        `- raw_message_json: ${firstRec.raw_message_json ? '已存在' : '缺失'}`,
        `- 原始来源: ${rawSource || 'unknown'}`,
        `- 原始消息类型: ${rawMessage?.media_type || (rawMessage?.photo ? 'photo' : 'unknown')}`,
        `- 原始时长: ${rawMessage?.duration_seconds != null ? `${rawMessage.duration_seconds} 秒` : (rawMessage?.duration != null ? `${rawMessage.duration} 秒` : 'NULL')}`,
        `- 原始文件大小: ${rawMessage?.file_size != null ? rawMessage.file_size : 'NULL'}`,
        `- 原始分辨率: ${rawMessage?.width && rawMessage?.height ? `${rawMessage.width}x${rawMessage.height}` : 'NULL'}`
      ];
      return tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id,
        text: debugLines.join('\n'),
        disable_web_page_preview: true
      }, env);
    }

    const lines = mediaRecords.map((rec, idx) => {
      const addedAtLine = rec.added_at ? String(rec.added_at).replace('T', ' ').substring(0, 16) : '未知时间';
      const link = rec.message_id
        ? (rec.topic_id
            ? `https://t.me/c/${String(rec.chat_id).replace(/^-100/, '')}/${rec.topic_id}/${rec.message_id}`
            : `https://t.me/c/${String(rec.chat_id).replace(/^-100/, '')}/${rec.message_id}`)
        : null;
      const linkPart = link ? ` [📎](${link})` : '';
      const chainPart = chainLabel(rec, idx);  // 🌟 V5.13: 链式关系标注
      const viewPart = rec.view_count > 0 ? ` | 👁 ${rec.view_count}` : '';
      return `*${idx + 1}.* ${chainPart}\n　　\`${rec.category_name}\` → ${topicBound(rec)}${viewPart}\n　　${addedAtLine}${linkPart}`;
    });

    return tgAPI('sendMessage', {
      chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id,
      text: `🔍 *籽青找到了 ${mediaRecords.length} 条收录记录喵～*\n\n${summaryLine}\n\n━━━━━━━━━━━━━━━━\n📋 *收录详情*\n${lines.join('\n')}`,
      parse_mode: 'Markdown', disable_web_page_preview: true
    }, env);
  }

  // 🌟 快捷回复管理魔法 (/d 和 /mv) — 单条回复模式
  // 排除批量格式：/d <数字|all> 和 /mv <数字|all> <分类>，让它们落到后面的批量路由
  const isBatchDFormat = /^\/d(?:@[A-Za-z0-9_]+)?\s+(all|\d+)$/i.test(text);
  const isBatchMvFormat = /^\/mv(?:@[A-Za-z0-9_]+)?\s+(all|\d+)\s+.+$/i.test(text);
  const isSingleD = isAddressedBotCommand(text, 'd', botUsername);
  const isSingleMv = isAddressedBotCommand(text, 'mv', botUsername);
  if (message.reply_to_message && (isSingleD || isSingleMv) && !isBatchDFormat && !isBatchMvFormat) {
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

  // 🌟 V5.7: 模式A — /d <N|all> 按数量批量删除（无 reply 时触发，支持 @机器人名 后缀）
  if (!message.reply_to_message && /^\/d(?:@[A-Za-z0-9_]+)?\s+(all|\d+)$/i.test(text)) {
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

  // 🌟 V5.7: 模式A — /mv <N|all> <分类名> 按数量批量转移（无 reply 时触发，支持 @机器人名 后缀）
  if (!message.reply_to_message && /^\/mv(?:@[A-Za-z0-9_]+)?\s+(all|\d+)\s+.+$/i.test(text)) {
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

  const bindMatch = text.match(/^\/bind(?:@([A-Za-z0-9_]+))?\s+(.+)$/i);
  if (bindMatch && (!bindMatch[1] || (botUsername && bindMatch[1].toLowerCase() === botUsername.toLowerCase()))) {
    // 🌟 V5.12: 拒绝私聊绑定（私聊正ID会污染 config_topics，绑定是群组话题概念）
    if (chatId > 0) return tgAPI('sendMessage', { chat_id: chatId, text: "📂 /bind 只能在群组话题内执行喵~（请在目标群里绑定分类）" }, env);
    if (!(await isAdmin(chatId, userId, env))) return;
    const category = bindMatch[2].trim();
    if (!category) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, category, userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `绑定成功喵！籽青已将当前话题与分类【${category}】绑定啦！(๑•̀ㅂ•́)و✧` }, env);
    return;
  }

  if (isAddressedBotCommand(text, ['bind_output', 'bout'], botUsername)) {
    // 🌟 V5.12: 拒绝私聊绑定输出话题（同理防止正ID脏数据）
    if (chatId > 0) return tgAPI('sendMessage', { chat_id: chatId, text: "📂 /bind_output 只能在群组话题内执行喵~（请在目标群里设置输出窗口）" }, env);
    if (!(await isAdmin(chatId, userId, env))) return;
    await env.D1.prepare(`INSERT INTO config_topics (chat_id, chat_title, topic_id, category_name, bound_by) VALUES (?, ?, ?, ?, ?)`)
      .bind(chatId, message.chat.title || 'Private', topicId, 'output', userId).run();
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `设置成功喵！籽青以后就在这里发图啦~ QwQ` }, env);
    return;
  }

  // 🌟 V5.11: /bind_source — 将本群(展示群)绑定到源群白名单
  // 用法1(主): 从源群转发任意媒体到本群 → 回复该消息发 /bind_source
  // 用法2(备): /bind_source -100xxx 或 /bind_source @channelusername
  // 🌟 V5.13: 缩写 /bsrc 与原命令并存
  const bindSourceMatch = text.match(/^\/(?:bind_source|bsrc)(?:@([A-Za-z0-9_]+))?(?:\s+(.+))?$/i);
  if (bindSourceMatch && (!bindSourceMatch[1] || (botUsername && bindSourceMatch[1].toLowerCase() === botUsername.toLowerCase()))) {
    if (!(await isAdmin(chatId, userId, env))) return;
    if (chatId > 0) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "📡 请在展示群(群组)内执行 /bind_source 喵~" }, env);

    const target = await resolveSourceTarget(message, bindSourceMatch[2], env);
    if (!target) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: "📡 **绑定源群**\n\n两种方式任选：\n1️⃣ 从源群转发任意媒体到本群,回复该消息发 `/bind_source`\n2️⃣ `/bind_source -100xxx` 或 `/bind_source @用户名`\n\n💡 私有群用方式1,零查ID成本喵~" }, env);
    }

    // 自引用（绑定自己）允许但提示
    if (target.id === chatId) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "💡 本群自身始终是默认源群,无需绑定自己喵~（直接收录即可展示）" }, env);
    }

    // 绑定时校验：bot 须在源群（否则跨群 copyMessage 会失败）
    const botInSource = await isBotInGroup(target.id, env);
    if (!botInSource) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 绑定失败喵！籽青还没被拉进该源群,或没有读取历史消息的权限。\n请先把本机器人邀请进源群并授予权限,再重新执行 /bind_source 喵~" }, env);
    }

    await env.D1.prepare(
      `INSERT INTO group_sources (display_chat_id, source_chat_id, source_chat_title, added_by) VALUES (?, ?, ?, ?) ON CONFLICT(display_chat_id, source_chat_id) DO UPDATE SET source_chat_title = excluded.source_chat_title`
    ).bind(chatId, target.id, target.title, userId).run();
    const titleLabel = target.title ? `【${target.title}】` : `(${target.id})`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `✅ 绑定成功喵！本群已可拉取源群 ${titleLabel} 的资源~\n用户可在筛选器→源群中切换要看的源喵 (๑•̀ㅂ•́)و✧` }, env);
    return;
  }

  // 🌟 V5.11: /unbind_source — 从本群源群白名单移除源群
  // 🌟 V5.13: 缩写 /ubsrc 与原命令并存
  const unbindSourceMatch = text.match(/^\/(?:unbind_source|ubsrc)(?:@([A-Za-z0-9_]+))?(?:\s+(.+))?$/i);
  if (unbindSourceMatch && (!unbindSourceMatch[1] || (botUsername && unbindSourceMatch[1].toLowerCase() === botUsername.toLowerCase()))) {
    if (!(await isAdmin(chatId, userId, env))) return;
    if (chatId > 0) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "📡 请在展示群(群组)内执行 /unbind_source 喵~" }, env);

    const target = await resolveSourceTarget(message, unbindSourceMatch[2], env);
    if (!target) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: "📡 **解绑源群**\n\n回复源群转发消息发 `/unbind_source`,或 `/unbind_source -100xxx` / `/unbind_source @用户名`" }, env);
    }

    await env.D1.prepare(
      `DELETE FROM group_sources WHERE display_chat_id = ? AND source_chat_id = ?`
    ).bind(chatId, target.id).run();
    // 同步清理该源的用户选择（避免残留）
    await env.D1.prepare(
      `DELETE FROM user_source_selection WHERE display_chat_id = ? AND source_chat_id = ?`
    ).bind(chatId, target.id).run();
    const titleLabel = target.title ? `【${target.title}】` : `(${target.id})`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `✅ 已解绑源群 ${titleLabel} 喵~本群不再拉取它的资源。` }, env);
    return;
  }

  // 🌟 V5.13: /promote_target — 设置 /promote 的默认目标A群（储存备份群）
  // 用法: /promote_target <A群ID|@用户名> 或 回复A群转发消息发 /promote_target
  // 缩写 /ptgt 与原命令并存
  const promoteTargetMatch = text.match(/^\/(?:promote_target|ptgt)(?:@([A-Za-z0-9_]+))?(?:\s+(.+))?$/i);
  if (promoteTargetMatch && (!promoteTargetMatch[1] || (botUsername && promoteTargetMatch[1].toLowerCase() === botUsername.toLowerCase()))) {
    if (chatId > 0) return tgAPI('sendMessage', { chat_id: chatId, text: "📤 /promote_target 只能在展示群(群组)内执行喵~" }, env);
    if (!(await isAdmin(chatId, userId, env))) return;

    const target = await resolveSourceTarget(message, promoteTargetMatch[2], env);
    if (!target) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: "📤 **设置提升目标群**\n\n两种方式任选：\n1️⃣ 从A群转发任意媒体到本群,回复该消息发 `/promote_target`\n2️⃣ `/promote_target -100xxx` 或 `/promote_target @用户名`\n\n💡 A群是储存备份群,提升后的媒体会归档到A群对应分类喵~" }, env);
    }
    if (target.id === chatId) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "💡 目标群就是当前展示群,无需提升到自己喵~（请指定储存备份A群）" }, env);
    }
    // 校验 bot 须在A群（否则 forwardMessage 到A群会失败）
    const botInA = await isBotInGroup(target.id, env);
    if (!botInA) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 设置失败喵！籽青还没被拉进目标A群,或没有发消息的权限。\n请先把本机器人邀请进A群并授予权限,再重新执行 /promote_target 喵~" }, env);
    }
    await upsertChatSetting(chatId, 'promote_target', String(target.id), env);
    const titleLabel = target.title ? `【${target.title}】` : `(${target.id})`;
    await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `✅ 已设置提升目标群 ${titleLabel} 喵！\n现在回复B群媒体发 \`/promote\` 即可归档到A群~ (๑•̀ㅂ•́)و✧`, parse_mode: 'Markdown' }, env);
    return;
  }

  // 🌟 V5.13: /promote — 回复B群媒体,提升归档到A群（保留原始转发来源）
  // 用法: 回复媒体 + /promote  或  /promote <A群>  或  /promote <A群> <分类名>
  // 缩写 /prom 与原命令并存
  const promoteMatch = text.match(/^\/(?:promote|prom)(?:@([A-Za-z0-9_]+))?(?:\s+(-?\d+|@[A-Za-z0-9_]+))?(?:\s+(.+))?$/i);
  if (promoteMatch && (!promoteMatch[1] || (botUsername && promoteMatch[1].toLowerCase() === botUsername.toLowerCase()))) {
    if (chatId > 0) return tgAPI('sendMessage', { chat_id: chatId, text: "📤 /promote 只能在展示群(群组)内回复媒体执行喵~" }, env);
    if (!(await isAdmin(chatId, userId, env))) return;
    const reply = message.reply_to_message;
    if (!reply) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: "📤 请回复要提升的媒体消息发 `/promote` 喵~\n用法: 回复媒体 + `/promote` 或 `/promote <A群> <分类>`", }, env);
    }

    // 1) 定目标A群：参数 > 默认 promote_target 设置
    let aChatId, aChatTitle = null;
    if (promoteMatch[2]) {
      const t = await resolveSourceTarget(message, promoteMatch[2], env);
      if (!t) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 无法解析目标A群,请用 -100xxx 或 @用户名 喵" }, env);
      aChatId = t.id; aChatTitle = t.title;
    } else {
      const saved = await getSetting(chatId, 'promote_target', env);
      if (!saved) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: "📤 未设置提升目标群喵~\n请先 `/promote_target <A群>` 绑定,或 `/promote <A群>` 临时指定" }, env);
      aChatId = parseInt(saved);
    }
    if (aChatId === chatId) return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "💡 目标群就是当前展示群,无需提升到自己喵~" }, env);

    // 2) bot 须在A群
    if (!(await isBotInGroup(aChatId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🚨 籽青还没被拉进目标A群,无法转发喵~请先邀请 bot 进A群并授权,或用 /promote_target 重新设置" }, env);
    }

    // 3) 提取被回复媒体元数据
    const bInfo = extractMediaInfo(reply);
    if (!bInfo.fileUniqueId) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "喵？这似乎不是标准的图片/视频/动画/文档哦！" }, env);
    }
    if (reply.media_group_id) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "📤 媒体组暂不支持批量提升,请对单张媒体执行 /promote 喵~" }, env);
    }

    // 4) 定分类：参数指定 > B库记录分类 > null
    let category = promoteMatch[3] ? promoteMatch[3].trim() : null;
    if (!category) {
      const bRow = await env.D1.prepare(`SELECT category_name FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(bInfo.fileUniqueId, chatId).first();
      category = bRow?.category_name || null;
    }

    // 5) 查A群该分类 topic 绑定
    let aTopicRow = null;
    if (category) aTopicRow = await env.D1.prepare(`SELECT topic_id FROM config_topics WHERE chat_id = ? AND category_name = ? AND category_name != 'output' LIMIT 1`).bind(aChatId, category).first();
    if (!aTopicRow) {
      if (category) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: `📂 目标A群未绑定分类【${category}】喵~请先在A群对应话题 \`/bind ${category}\`,或 \`/promote <A群> <其他分类>\` 指定A群已有分类` }, env);
      }
      // B库无分类且用户未指定 → 列A群已有分类让用户选（batch_sessions 暂存上下文）
      const { results: aCats } = await env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE chat_id = ? AND category_name != 'output'`).bind(aChatId).all();
      if (!aCats || aCats.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, parse_mode: 'Markdown', text: "📂 目标A群还没绑定任何分类喵~请先在A群 `/bind <分类>`" }, env);
      }
      // 暂存提升上下文到 batch_sessions（callback 无 reply_to_message，需暂存 bInfo 等）
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, ?, ?)`).bind(
        chatId, userId, 'promote_pending',
        JSON.stringify({ aChatId, bInfo, replyMsgId: reply.message_id, bChatId: chatId, bTopicId: topicId, cmdMsgId: message.message_id, replyCaption: reply.caption || '' })
      ).run();
      const kb = aCats.map(r => [{ text: `📂 ${r.category_name}`, callback_data: `promote_cat|${r.category_name}` }]);
      kb.push([{ text: "❌ 取消", callback_data: "cancel_action" }]);
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: `请选择提升到A群哪个分类喵：`, reply_markup: { inline_keyboard: kb } }, env);
    }

    // 6) 直接执行提升
    return doPromote(reply, bInfo, aChatId, aTopicRow.topic_id, category, chatId, topicId, message.message_id, env);
  }

  // ==== 完整恢复的内置 JSON 解析功能 ====
  const importMatch = text.match(/^\/import(?:@([A-Za-z0-9_]+))?\s+(.+)$/i);
  const importForSelf = importMatch && (!importMatch[1] || (botUsername && importMatch[1].toLowerCase() === botUsername.toLowerCase()));
  if (message.document && message.document.file_name && message.document.file_name.endsWith('.json') && importForSelf) {
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `🚨 呜呜,只有管理员主人才可以给籽青投喂文件哦！` }, env);
    }

    const category = importMatch[2].trim();
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
          duration: Number.isInteger(msg.duration_seconds) ? msg.duration_seconds : (Number.isInteger(msg.duration) ? msg.duration : null),
          raw_message_json: serializeStoredMediaPayload('telegram_desktop_export', msg)
        });
      }

      if (validMedia.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `❓ 哎呀,籽青在这个文件里没有找到任何图片或视频记录喵。` }, env);
      }

      let successCount = 0;
      for (let i = 0; i < validMedia.length; i += 50) {
        const batch = validMedia.slice(i, i + 50);
        const stmts = batch.map(item => {
          return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration, raw_message_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
            .bind(item.message_id, item.chat_id, item.topic_id, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption, item.duration ?? null, item.raw_message_json ?? null);
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
      `SELECT * FROM batch_sessions WHERE chat_id = ? AND user_id = ? AND mode IN ('filter_date_custom', 'filter_date_after', 'filter_date_before', 'filter_dur_input', 'filter_dur_gt', 'filter_dur_lt', 'filter_dur_range', 'filter_dur_custom', 'set_vote_threshold') LIMIT 1`
    ).bind(chatId, userId).first();

    if (filterSession) {
      const input = message.text.trim();

      // 超时检查（5分钟）
      if (Date.now() - new Date(filterSession.created_at + 'Z').getTime() > 300000) {
        await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run();
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "⏰ 筛选输入会话已超时喵，请重新打开筛选器设置～" }, env);
      }

      // 取消操作
      if (isCancelInput(input, botUsername)) {
        await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run();
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "✅ 已取消筛选输入喵～" }, env);
      }

      let payload = {};
      try { payload = JSON.parse(filterSession.collected_ids || '{}'); } catch (e) { payload = {}; }
      const sourceChatId = typeof payload.sourceChatId === 'number' ? payload.sourceChatId : chatId;

      // —— 时长：大于（gt）——
      if (filterSession.mode === 'filter_dur_gt') {
        if (!/^(0|[1-9]\d*)$/.test(input)) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请输入非负整数秒数（如 30、120），或发送 /cancel 取消喵～"
          }, env);
        }
        const minSec = parseInt(input, 10);
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'duration_mode', 'gt', env),
          upsertUserFilter(userId, sourceChatId, 'duration_min', String(minSec), env),
          upsertUserFilter(userId, sourceChatId, 'duration_max', '', env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时长筛选已设置：>${minSec} 秒的视频喵～`, env);
      }

      // —— 时长：小于（lt）——
      else if (filterSession.mode === 'filter_dur_lt') {
        if (!/^(0|[1-9]\d*)$/.test(input)) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请输入非负整数秒数（如 30、120），或发送 /cancel 取消喵～"
          }, env);
        }
        const maxSec = parseInt(input, 10);
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'duration_mode', 'lt', env),
          upsertUserFilter(userId, sourceChatId, 'duration_min', '', env),
          upsertUserFilter(userId, sourceChatId, 'duration_max', String(maxSec), env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时长筛选已设置：<${maxSec} 秒的视频喵～`, env);
      }

      // —— 时长：区间（range）——
      else if (filterSession.mode === 'filter_dur_range') {
        const rangeMatch = input.match(/^(\d+)\s*[-~]\s*(\d+)$/);
        if (!rangeMatch) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请输入区间（如 `30-120` 或 `30~120`），或发送 /cancel 取消喵～"
          }, env);
        }
        const minSec = parseInt(rangeMatch[1], 10);
        const maxSec = parseInt(rangeMatch[2], 10);
        if (minSec >= maxSec) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 最小值必须小于最大值喵～ 请重新输入或发送 /cancel 取消"
          }, env);
        }
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'duration_mode', 'range', env),
          upsertUserFilter(userId, sourceChatId, 'duration_min', String(minSec), env),
          upsertUserFilter(userId, sourceChatId, 'duration_max', String(maxSec), env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时长筛选已设置：${minSec}~${maxSec} 秒的视频喵～`, env);
      }

      // —— 时长：自定义上限（custom，兼容保留）——
      else if (filterSession.mode === 'filter_dur_input' || filterSession.mode === 'filter_dur_custom') {
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
          upsertUserFilter(userId, sourceChatId, 'duration_min', '', env),
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

      if (filterSession.mode === 'filter_date_after') {
        // 单日期：YYYY-MM-DD（≥ 某日期）
        const trimmed = input.trim();
        if (!FILTER_DATE_RE.test(trimmed)) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请按 `YYYY-MM-DD` 格式输入（如 2024-01-01），或发送 /cancel 取消喵～",
            parse_mode: 'Markdown'
          }, env);
        }
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'date_mode', 'after', env),
          upsertUserFilter(userId, sourceChatId, 'date_from', trimmed, env),
          upsertUserFilter(userId, sourceChatId, 'date_to', '', env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时间筛选已设置：≥ ${trimmed}喵～`, env);
      }

      if (filterSession.mode === 'filter_date_before') {
        // 单日期：YYYY-MM-DD（≤ 某日期）
        const trimmed = input.trim();
        if (!FILTER_DATE_RE.test(trimmed)) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请按 `YYYY-MM-DD` 格式输入（如 2024-12-31），或发送 /cancel 取消喵～",
            parse_mode: 'Markdown'
          }, env);
        }
        await Promise.all([
          upsertUserFilter(userId, sourceChatId, 'date_mode', 'before', env),
          upsertUserFilter(userId, sourceChatId, 'date_from', '', env),
          upsertUserFilter(userId, sourceChatId, 'date_to', trimmed, env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return sendFilterPanelNew(userId, chatId, topicId, sourceChatId, `✅ 时间筛选已设置：≤ ${trimmed}喵～`, env);
      }

      // 🌟 V5.11: 投票阈值设置（正整数）
      if (filterSession.mode === 'set_vote_threshold') {
        if (!/^[1-9]\d*$/.test(input.trim())) {
          return tgAPI('sendMessage', {
            chat_id: chatId, message_thread_id: topicId,
            text: "⚠️ 格式错误！请输入正整数（如 3、5），或发送 /cancel 取消喵～"
          }, env);
        }
        const thr = parseInt(input.trim(), 10);
        await Promise.all([
          upsertChatSetting(chatId, 'vote_threshold', String(thr), env),
          env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(filterSession.id).run()
        ]);
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: `✅ 投票移除阈值已设置为 ${thr} 票喵～` }, env);
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
      // 🌟 V5.11: ingest_enabled=false 时本群仅展示不收录（纯展示群）
      const ingestEnabled = await getSetting(chatId, 'ingest_enabled', env);
      if (ingestEnabled !== 'true') return;

      const existing = await env.D1.prepare(`SELECT id, duration, raw_message_json, sender_user_id FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(mediaInfo.fileUniqueId, chatId).first();
      if (existing) {
        // 🌟 V5.10+: 渐进式补全 duration + sender_user_id + 原始媒体元数据
        const mergedRawMessageJson = mergeStoredMediaPayload(existing.raw_message_json, mediaInfo.rawMessageJson);
        const shouldUpdateDuration = existing.duration === null && mediaInfo.duration !== null;
        const shouldUpdateSender = existing.sender_user_id === null && mediaInfo.senderUserId !== null;
        const shouldUpdateRaw = mergedRawMessageJson !== null && mergedRawMessageJson !== existing.raw_message_json;
        if (shouldUpdateDuration || shouldUpdateSender || shouldUpdateRaw) {
          ctx.waitUntil(
            env.D1.prepare(`UPDATE media_library SET duration = COALESCE(duration, ?), sender_user_id = COALESCE(sender_user_id, ?), raw_message_json = ? WHERE id = ?`)
              .bind(mediaInfo.duration ?? null, mediaInfo.senderUserId ?? null, shouldUpdateRaw ? mergedRawMessageJson : existing.raw_message_json, existing.id).run()
          );
        }
        const notify = await getSetting(chatId, 'dup_notify', env);
        if (notify === 'true') {
          await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, reply_to_message_id: message.message_id, text: "哎呀,籽青发现这个内容之前已经收录过啦喵~" }, env);
        }
        return;
      }
      await env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration, raw_message_json, sender_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .bind(message.message_id, chatId, topicId, query.category_name, mediaInfo.fileUniqueId, mediaInfo.fileId, mediaInfo.type, message.caption || '', mediaInfo.duration ?? null, mediaInfo.rawMessageJson, mediaInfo.senderUserId ?? null).run();
    }
  }
}

function extractMediaInfo(message) {
  const senderUserId = message.from?.id || null;
  let info = { fileUniqueId: null, fileId: null, type: null, duration: null, rawMessageJson: null, senderUserId: null };
  if (message.photo && message.photo.length > 0) {
    const p = message.photo[message.photo.length - 1];
    info = { fileUniqueId: p.file_unique_id, fileId: p.file_id, type: 'photo', duration: null, rawMessageJson: serializeStoredMediaPayload('telegram_webhook', message), senderUserId };
  } else if (message.video) {
    info = { fileUniqueId: message.video.file_unique_id, fileId: message.video.file_id, type: 'video',
             duration: Number.isInteger(message.video.duration) ? message.video.duration : null,
             rawMessageJson: serializeStoredMediaPayload('telegram_webhook', message), senderUserId };
  } else if (message.document) {
    info = { fileUniqueId: message.document.file_unique_id, fileId: message.document.file_id, type: 'document', duration: null,
             rawMessageJson: serializeStoredMediaPayload('telegram_webhook', message), senderUserId };
  } else if (message.animation) {
    info = { fileUniqueId: message.animation.file_unique_id, fileId: message.animation.file_id, type: 'animation',
             duration: Number.isInteger(message.animation.duration) ? message.animation.duration : null,
             rawMessageJson: serializeStoredMediaPayload('telegram_webhook', message), senderUserId };
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

  // 🌟 处理历史回退（V5.11: prev_${category}|${scopeChatId}|${offset}）
  //   scopeChatId: 群组=展示群, 私聊=远程源群（与 sendRandomMedia 历史足迹记录一致）
  else if (data.startsWith('prev_')) {
    const params = data.replace('prev_', '').split('|');
    const category = params[0];
    const scopeChatId = parseInt(params[1]) || chatId;
    const offset = parseInt(params[2]) || 0;
    await sendHistoricalMedia(userId, chatId, msgId, topicId, category, scopeChatId, offset, env, cbId);
  }

  else if (data.startsWith('random_') || data.startsWith('next_')) {
    const action = data.startsWith('random_') ? 'random_' : 'next_';
    const params = data.replace(action, '').split('|');
    const category = params[0];
    // 🌟 V5.12: callback 第二段为「作用域 chatId」
    //   私聊(chatId>0): 私聊ID本身（作用域，存储 user_filters/user_source_selection 的 key）
    //   群组(chatId<0): 展示群,需 resolveEffectiveSources 解析多源集合
    const scopeChatId = params.length > 1 ? parseInt(params[1]) : chatId;

    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "籽青正在为你抽取喵..." }, env);

    let sourceChatIds;
    if (chatId > 0) {
      // 🌟 V5.12: 私聊多源 — 按私聊ID解析有效源群集合（授权群∩用户选择∩成员资格）
      //   私聊时 scopeChatId=私聊ID=chatId，用 chatId 语义更清晰
      sourceChatIds = await resolveEffectiveSources(userId, chatId, env);
      if (sourceChatIds.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "呜呜,所选授权群你均已不可访问,请重新选择源群喵~ (筛选器→源群)" }, env);
      }
    } else {
      // 群组：按展示群解析有效源群集合（白名单∩选择∩成员资格）
      sourceChatIds = await resolveEffectiveSources(userId, chatId, env);
      if (sourceChatIds.length === 0) {
        return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "呜呜,所选源群你均已不可访问,请重新选择源群喵~ (筛选器→源群)" }, env);
      }
    }
    await sendRandomMedia(userId, chatId, msgId, topicId, category, sourceChatIds, action === 'next_', env, ctx, cbId);
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
  }
  // 🌟 V5.15: 查看当前媒体附近 5 条（同源群·同分类，按 message_id 邻域）
  //   从媒体键盘点开 → sendMessage；从附近列表「刷新」点开 → editMessageText
  else if (data.startsWith('near|')) {
    const mediaId = parseInt(data.split('|')[1], 10);
    if (mediaId && !isNaN(mediaId)) {
      const canEdit = !!(callback.message && callback.message.text);
      await showNearbyMedia(userId, chatId, topicId, mediaId, cbId, env, canEdit ? msgId : null);
    } else {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "参数无效喵~", show_alert: true }, env);
    }
  }
  // 🌟 V5.11: 投票移除
  else if (data.startsWith('vote_remove|')) {
    const mediaId = parseInt(data.split('|')[1]);
    if (mediaId && !isNaN(mediaId)) {
      await handleVoteRemove(userId, chatId, msgId, mediaId, cbId, env);
    }
  }
  // 🌟 V5.13: /promote 分类选择面板回调（从 batch_sessions 读暂存上下文）
  else if (data.startsWith('promote_cat|')) {
    const category = data.replace('promote_cat|', '');
    const sess = await env.D1.prepare(`SELECT collected_ids FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`).bind(chatId, userId).first();
    if (!sess) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "会话已过期喵~请重新 /promote", show_alert: true }, env);
      return;
    }
    const p = JSON.parse(sess.collected_ids || '{}');
    const aTopicRow = await env.D1.prepare(`SELECT topic_id FROM config_topics WHERE chat_id = ? AND category_name = ? AND category_name != 'output' LIMIT 1`).bind(p.aChatId, category).first();
    if (!aTopicRow) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: `A群未绑定分类【${category}】喵`, show_alert: true }, env);
      return;
    }
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "提升中喵..." }, env);
    // callback 无 reply_to_message,用暂存的 bInfo + replyMsgId 构造 stub 调 doPromote
    const replyStub = { message_id: p.replyMsgId, caption: p.replyCaption || '' };
    await doPromote(replyStub, p.bInfo, p.aChatId, aTopicRow.topic_id, category, p.bChatId, p.bTopicId, p.cmdMsgId, env);
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
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

  else if (data === 'leaderboard' || data.startsWith('leader_page_') || data.startsWith('leader_pick')) {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    // 🌟 V5.12: 私聊走"选群"流程，群组直接展示本群排行
    if (chatId > 0) {
      if (data === 'leaderboard') return showLeaderGroupPicker(userId, chatId, msgId, 0, env);
      if (data.startsWith('leader_pick_page_')) {
        const pickPage = parseInt(data.replace('leader_pick_page_', '')) || 0;
        return showLeaderGroupPicker(userId, chatId, msgId, pickPage, env);
      }
      if (data.startsWith('leader_pick_pg|')) {
        // 排行翻页：leader_pick_pg|{群ID}|{页码}
        const parts = data.split('|');
        const targetGroup = parseInt(parts[1]);
        const pg = parseInt(parts[2]) || 0;
        if (targetGroup && !isNaN(targetGroup)) return showLeaderboard(chatId, msgId, pg, env, targetGroup);
        return;
      }
      if (data.startsWith('leader_pick_|')) {
        // 选定某群查看排行
        const targetGroup = parseInt(data.split('|')[1]);
        if (!targetGroup || isNaN(targetGroup)) return;
        const inGroup = await isUserInGroup(targetGroup, userId, env);
        if (!inGroup) {
          return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "喵... 你已经不在该群啦,无法查看排行哦", reply_markup: getBackMarkup() }, env);
        }
        return showLeaderboard(chatId, msgId, 0, env, targetGroup);
      }
      return;
    }
    const page = data === 'leaderboard' ? 0 : parseInt(data.replace('leader_page_', ''));
    await showLeaderboard(chatId, msgId, page, env);
  }

  else if (data.startsWith('set_')) {
    // 🌟 V5.12: 私聊精简设置面板（仅 6 个抽取展现开关，按私聊ID独立存，不走 isAdmin）
    if (chatId > 0) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
      if (data === 'set_main') return showSettingsMainPrivate(chatId, msgId, env);
      if (data === 'set_toggle_mode') return toggleSettingPrivate('display_mode', env, chatId, msgId, ['A', 'B']);
      if (data === 'set_toggle_repeat') return toggleSettingPrivate('anti_repeat', env, chatId, msgId, ['true', 'false']);
      if (data === 'set_toggle_jump') return toggleSettingPrivate('auto_jump', env, chatId, msgId, ['true', 'false']);
      if (data === 'set_toggle_success') return toggleSettingPrivate('show_success', env, chatId, msgId, ['true', 'false']);
      if (data === 'set_toggle_nextmode') return toggleSettingPrivate('next_mode', env, chatId, msgId, ['replace', 'new']);
      if (data === 'set_toggle_strict') return toggleSettingPrivate('strict_skip', env, chatId, msgId, ['true', 'false']);
      // 私聊不响应：set_toggle_dup / set_stats / set_unbind_* / set_source_* / set_vote_* / set_toggle_ingest / set_danger_zone / set_clear_* / set_toggle_member_check / set_member_check_do (V5.13) / set_toggle_expose_src (V5.14) / set_clean_served / set_danger_orphan_do / set_danger_reset_* (V5.15)
      return;
    }
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

    // 🌟 V5.11: 跨群共享与投票相关设置路由
    // 本群收录开关
    else if (data === 'set_toggle_ingest') await toggleSetting('ingest_enabled', env, chatId, msgId, ['true', 'false']);
    // 🌟 V5.14: 暴露第三方转发来源深链开关（仅展示群面板显示；循环切换 true/false）
    else if (data === 'set_toggle_expose_src') await toggleSetting('expose_forward_source', env, chatId, msgId, ['true', 'false']);
    // 源群白名单列表
    else if (data === 'set_source_list') await showSourceList(chatId, msgId, env);
    // 源群解绑（从设置面板入口）
    else if (data.startsWith('set_source_unbind_')) {
      const srcId = parseInt(data.replace('set_source_unbind_', ''));
      if (srcId && !isNaN(srcId)) {
        await env.D1.prepare(`DELETE FROM group_sources WHERE display_chat_id = ? AND source_chat_id = ?`).bind(chatId, srcId).run();
        await env.D1.prepare(`DELETE FROM user_source_selection WHERE display_chat_id = ? AND source_chat_id = ?`).bind(chatId, srcId).run();
      }
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已解绑该源群喵！", show_alert: true }, env);
      await showSourceList(chatId, msgId, env);
    }
    // 投票开关
    else if (data === 'set_toggle_vote_enabled') await toggleSetting('vote_enabled', env, chatId, msgId, ['true', 'false']);
    // 投票阈值输入（ForceReply）
    else if (data === 'set_vote_threshold_prompt') {
      const conflictSession = await env.D1.prepare(`SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "请先结束批量操作会话喵", show_alert: true }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, 'set_vote_threshold', ?)`).bind(chatId, userId, JSON.stringify({})).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: "📉 **设置投票移除阈值**\n请回复本条消息输入阈值（正整数,如 `3` 表示累计 3 票即触发移除）\n\n发送 /cancel 取消",
        parse_mode: 'Markdown',
        reply_markup: { force_reply: true, selective: true }
      }, env);
    }
    // 投票作用域切换：切到 delete 需2步确认
    else if (data === 'set_toggle_vote_scope') {
      const cur = await getSetting(chatId, 'vote_scope', env);
      if (cur === 'hide') {
        // 切到 delete：危险，先警告确认
        await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
        await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, parse_mode: 'Markdown', text: "🚨 **高危警告**\n\n投票作用域切到「源库删除」后,达阈值会**永久从源库物理删除**该媒体,影响所有群,不可逆喵！\n\n确定要开启吗？", reply_markup: { inline_keyboard: [[{ text: "🩸 我确定要开启源库删除", callback_data: "set_vote_scope_do" }], [{ text: "⬅️ 算了,保持隐藏", callback_data: "set_main" }]] } }, env);
      } else {
        // delete → hide：安全直接切
        await upsertChatSetting(chatId, 'vote_scope', 'hide', env);
        await showSettingsMain(chatId, msgId, env);
      }
    }
    else if (data === 'set_vote_scope_do') {
      await upsertChatSetting(chatId, 'vote_scope', 'delete', env);
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已切换为源库删除模式喵！(高危)", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }

    // 🌟 V5.13: 源群成员校验开关（切到 false=分发模式 需1步确认；false→true 直接切）
    else if (data === 'set_toggle_member_check') {
      const cur = await getSetting(chatId, 'source_membership_check', env);
      if (cur === 'true') {
        // 切到 false：危险，先警告确认
        await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
        await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, parse_mode: 'Markdown', text: "🚨 **高危警告**\n\n关闭「源群成员校验」后,本群(展示群)用户**无需加入源群**即可抽取源群媒体喵！\n\n⚠️ 这意味着本群的陌生人无需加入源群也能看到源群内容,请确认本群是受控的分发群,且你信任本群所有成员。\n\n确定要关闭吗？", reply_markup: { inline_keyboard: [[{ text: "🔓 我确定关闭(分发模式)", callback_data: "set_member_check_do" }], [{ text: "⬅️ 算了,保持校验", callback_data: "set_main" }]] } }, env);
      } else {
        // false → true：安全直接切
        await upsertChatSetting(chatId, 'source_membership_check', 'true', env);
        await showSettingsMain(chatId, msgId, env);
      }
    }
    else if (data === 'set_member_check_do') {
      await upsertChatSetting(chatId, 'source_membership_check', 'false', env);
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "已关闭源群成员校验(分发模式)喵！(高危)", show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }

    else if (data === 'set_danger_zone') {
      const text = "⚠️ **危险操作区**\n\n这里的操作仅对当前群组生效,且不可逆喵！";
      const keyboard = [[{ text: "🧨 清空本群数据统计", callback_data: "set_clear_stats_1" }], [{ text: "🚨 彻底清空本群媒体库", callback_data: "set_clear_media_1" }], [{ text: "🧹 清理防重库", callback_data: "set_clean_served" }], [{ text: "⬅️ 返回安全区", callback_data: "set_main" }]];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
    }
    // V5.15: 防重库清理子菜单
    else if (data === 'set_clean_served') {
      const text = "🧹 **清理防重库**\n\n主人要清理哪种类型呢喵？\n\n• **清理孤儿记录**：删除 media_library 中已不存在的防重记录（安全无害，全局生效）\n• **重置本群防重库**：清空本群所有已服务的防重记录（下次抽图会重新开始）";
      const keyboard = [
        [{ text: "🧽 清理孤儿记录(安全)", callback_data: "set_danger_orphan_do" }],
        [{ text: "🔄 重置本群防重库(2步确认)", callback_data: "set_danger_reset_1" }],
        [{ text: "⬅️ 返回危险操作区", callback_data: "set_danger_zone" }]
      ];
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
    }
    // 清理孤儿记录 - 1步直接执行 (安全无害)
    else if (data === 'set_danger_orphan_do') {
      const result = await env.D1.prepare(`DELETE FROM served_history WHERE media_id NOT IN (SELECT id FROM media_library)`).run();
      const cnt = result.meta?.changes || 0;
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: `已清理 ${cnt} 条孤儿防重记录喵！`, show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
    }
    // 重置本群防重库 - 第1步确认
    else if (data === 'set_danger_reset_1') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🔄 **重置本群防重库**\n\n即将清空本群所有已服务的防重记录（只影响反重复判断，不删除任何媒体），确定要重置吗喵？", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "🔴 确认重置(第1次)", callback_data: "set_danger_reset_2" }], [{ text: "⬅️ 返回", callback_data: "set_clean_served" }]] } }, env);
    }
    // 重置本群防重库 - 第2步最终警告
    else if (data === 'set_danger_reset_2') {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🧹 **最终警告**\n\n将清空本群所有「已服务」的记录，这意味着你可能会再次抽到之前已经看过的图喵！\n\n确定要重置吗？", parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "☠️ 彻底重置！", callback_data: "set_danger_reset_do" }], [{ text: "⬅️ 算了", callback_data: "set_clean_served" }]] } }, env);
    }
    // 重置本群防重库 - 执行
    else if (data === 'set_danger_reset_do') {
      const result = await env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (SELECT id FROM media_library WHERE chat_id = ?)`).bind(chatId).run();
      const cnt = result.meta?.changes || 0;
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: `已重置本群 ${cnt} 条防重记录喵！`, show_alert: true }, env);
      await showSettingsMain(chatId, msgId, env);
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

  // 🌟 V5.14.1: 检测无效媒体（限管理员+展示群，2步确认，异步分批探活）
  else if (data === 'scan_dead_start' || data === 'scan_dead_confirm' || data.startsWith('scan_dead_run')) {
    if (chatId > 0) {
      return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "请在群组内使用此功能喵~", show_alert: true }, env);
    }
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "🚨 仅管理员可检测无效媒体喵！", show_alert: true }, env);
    }

    if (data === 'scan_dead_start') {
      const totalRow = await env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE chat_id = ?`).bind(chatId).first();
      const total = totalRow?.c || 0;
      if (total === 0) {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "本群媒体库为空,无需检测喵~", show_alert: true }, env);
      }
      const text = `🧹 **检测无效媒体**\n\n即将探活本群媒体库共 ${total} 条记录喵~\n\n📋 **流程说明**:\n• 用 file_id 直发探活,活的立刻删除探活消息(不打扰)\n• 死链完整清理所有关联表并逐条通知\n• ⚠️ 耗时较长,期间可正常使用其他功能\n\n确认开始检测吗？`;
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: "✅ 开始检测", callback_data: "scan_dead_confirm" }], [{ text: "⬅️ 返回主菜单", callback_data: "main_menu_new" }]] } }, env);
    }
    else if (data === 'scan_dead_confirm') {
      // 解析输出话题(探活消息发到这里,活的成功后立即删除)
      const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' AND chat_id = ? LIMIT 1`).bind(chatId).first();
      if (!output) {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "请先 /bind_output 设置输出话题喵！", show_alert: true }, env);
      }
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "🚀 检测已启动,稍后看通知喵~" }, env);
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🧹 无效媒体检测已启动喵~\n\n籽青正在后台分批探活,死链清理结果会发到本群输出话题。\n完成后会发完成通知喵！", reply_markup: { inline_keyboard: [[{ text: "🏠 返回主菜单", callback_data: "main_menu_new" }]] } }, env);
      // 异步分批扫描,不阻塞响应
      ctx.waitUntil(scanDeadMedia(chatId, output.chat_id, output.topic_id, env));
    }
  }

  // 🌟 V5.14.1: 死链清理确认回调
  //   deadpurge|<mediaId>|s  → 单条清理(s=single)
  //   deadpurge|<mediaId>|g  → 整群清理(g=group) — 按该媒体的 chat_id 整群清理
  //   deadpurge|<mediaId>|skip → 跳过,删询问消息
  else if (data.startsWith('deadpurge|')) {
    const parts = data.replace('deadpurge|', '').split('|');
    const mediaId = parseInt(parts[0]);
    const mode = parts[1]; // 's' | 'g' | 'skip'
    if (mode === 'skip') {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
      try { await tgAPI('deleteMessage', { chat_id: chatId, message_id: msgId }, env); } catch(e){}
    } else {
      if (!(await isAdmin(chatId, userId, env))) {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "🚨 仅管理员可执行死链清理喵！", show_alert: true }, env);
      }
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "🧹 正在清理..." }, env);
      if (mode === 'g') {
        // 整群清理:查该媒体的 chat_id 再整群清理
        const m = await env.D1.prepare(`SELECT chat_id FROM media_library WHERE id = ?`).bind(mediaId).first();
        if (m) {
          const cnt = await purgeChatFully(m.chat_id, env);
          await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `✅ 源群 <code>${m.chat_id}</code> 已整群清理 ${cnt} 条记录喵~`, parse_mode: 'HTML' }, env);
        } else {
          await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "该媒体已被清理,无需重复操作喵~" }, env);
        }
      } else {
        // 单条清理
        await purgeMediaById(mediaId, env);
        await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `✅ 媒体 #${mediaId} 已彻底清理喵~` }, env);
      }
    }
  }

  // 🌟 V5.14.1: 批量死链清理回调（主动扫描产出的确认）
  //   dead_scan_all|<sessionId>   → 确认清理全部
  //   dead_scan_cancel|<sessionId> → 取消,保留死链,删 session
  else if (data.startsWith('dead_scan_all|') || data.startsWith('dead_scan_cancel|')) {
    const sessionId = parseInt(data.split('|')[1]);
    const session = await env.D1.prepare(`SELECT * FROM batch_sessions WHERE id = ? AND mode = 'dead_scan'`).bind(sessionId).first();
    if (!session) {
      return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "会话已过期或不存在喵~", show_alert: true }, env);
    }
    if (data.startsWith('dead_scan_cancel|')) {
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(sessionId).run();
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "已取消,死链全部保留未清理喵~", reply_markup: { inline_keyboard: [[{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]] } }, env);
      return;
    }
    // 确认清理
    if (!(await isAdmin(chatId, userId, env))) {
      return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "🚨 仅管理员可执行死链清理喵！", show_alert: true }, env);
    }
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "🧹 正在批量清理..." }, env);
    const ids = JSON.parse(session.collected_ids || '[]');
    let cleaned = 0;
    for (const id of ids) {
      try { await purgeMediaById(id, env); cleaned++; } catch (e) { console.error(`清理死链 #${id} 失败:`, e); }
    }
    await env.D1.prepare(`DELETE FROM batch_sessions WHERE id = ?`).bind(sessionId).run();
    await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: `✅ 已清理 ${cleaned} 条死链喵~\n\n📊 原清单共 ${ids.length} 条,${cleaned} 条已处理`, reply_markup: { inline_keyboard: [[{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]] } }, env);
  }

  // 🌟 V5.9: 过滤器回调路由
  else if (data === 'filter_open') {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId }, env);
    // 🌟 V5.12: 私聊开放筛选器（第4参数 chatId 私聊=私聊ID即作用域，群组=展示群ID）
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

    // —— 自定义时间：≥ 某日期（ForceReply）——
    else if (action === 'filter_time_after') {
      const conflictSession = await env.D1.prepare(
        `SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`
      ).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "请先结束当前的批量操作会话，再设置筛选器喵～" }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, 'filter_date_after', ?)`).bind(chatId, userId, JSON.stringify({ sourceChatId: sc })).run();
      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: "📅 **设置收录时间：≥ 某日期**\n请回复本条消息输入起始日期喵～\n\n📌 格式：`YYYY-MM-DD`\n💡 示例：`2024-06-01`（表示 2024-06-01 及之后收录的媒体）\n\n发送 /cancel 取消",
        parse_mode: 'Markdown',
        reply_markup: { force_reply: true, selective: true }
      }, env);
    }

    // —— 自定义时间：≤ 某日期（ForceReply）——
    else if (action === 'filter_time_before') {
      const conflictSession = await env.D1.prepare(
        `SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`
      ).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "请先结束当前的批量操作会话，再设置筛选器喵～" }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, 'filter_date_before', ?)`).bind(chatId, userId, JSON.stringify({ sourceChatId: sc })).run();
      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: "📅 **设置收录时间：≤ 某日期**\n请回复本条消息输入截止日期喵～\n\n📌 格式：`YYYY-MM-DD`\n💡 示例：`2024-12-31`（表示 2024-12-31 及之前收录的媒体）\n\n发送 /cancel 取消",
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
      if (fCur.duration_mode === val && fCur.duration_min === '' && fCur.duration_max === '') {
        return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "当前已是该选项喵～", show_alert: false }, env);
      }
      await Promise.all([
        upsertUserFilter(userId, sc, 'duration_mode', val, env),
        upsertUserFilter(userId, sc, 'duration_min',  '', env),
        upsertUserFilter(userId, sc, 'duration_max',  '', env)
      ]);
      await showFilterDurPanel(userId, chatId, msgId, sc, env);
    }

    // —— 大于/小于/区间（ForceReply）——
    else if (['filter_dur_gt', 'filter_dur_lt', 'filter_dur_range'].includes(action)) {
      const conflictSession = await env.D1.prepare(
        `SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`
      ).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "请先结束当前的批量操作会话，再设置筛选器喵～" }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      const mode = action.replace('filter_dur_', '');
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, ?, ?)`).bind(chatId, userId, `filter_dur_${mode}`, JSON.stringify({ sourceChatId: sc })).run();

      let promptText = '';
      if (mode === 'gt') {
        promptText = "⏱ **设置时长下限（大于）**\n请回复本条消息输入最小秒数喵～\n\n📌 示例：`30` 表示仅抽取 >30 秒的视频\n\n发送 /cancel 取消";
      } else if (mode === 'lt') {
        promptText = "⏱ **设置时长上限（小于）**\n请回复本条消息输入最大秒数喵～\n\n📌 示例：`120` 表示仅抽取 <120 秒的视频\n\n发送 /cancel 取消";
      } else if (mode === 'range') {
        promptText = "⏱ **设置时长区间**\n请回复本条消息输入区间喵～\n\n📌 格式：`最小-最大`（如 `30-120` 表示 30~120 秒）\n\n发送 /cancel 取消";
      }

      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: promptText,
        parse_mode: 'Markdown',
        reply_markup: { force_reply: true, selective: true }
      }, env);
    }

    // —— 自定义时长（ForceReply，兼容保留）——
    else if (action === 'filter_dur_custom') {
      const conflictSession = await env.D1.prepare(
        `SELECT mode FROM batch_sessions WHERE chat_id = ? AND user_id = ? LIMIT 1`
      ).bind(chatId, userId).first();
      if (conflictSession && ['bd','bmv','cleanup','bmv_quick'].includes(conflictSession.mode.split(':')[0])) {
        return tgAPI('sendMessage', { chat_id: chatId, text: "请先结束当前的批量操作会话，再设置筛选器喵～" }, env);
      }
      await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = ?`).bind(chatId, userId).run();
      await env.D1.prepare(`INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, ?, 'filter_dur_custom', ?)`).bind(chatId, userId, JSON.stringify({ sourceChatId: sc })).run();
      await tgAPI('sendMessage', {
        chat_id: chatId, message_thread_id: topicId,
        text: "⏱ **设置自定义视频时长**\n请回复本条消息输入最大秒数喵～\n\n📌 示例：`30` 表示仅抽取 0~30 秒内的视频\n\n发送 /cancel 取消",
        parse_mode: 'Markdown',
        reply_markup: { force_reply: true, selective: true }
      }, env);
    }

    // 🌟 V5.10: 发送者筛选器回调路由
    // —— 发送者子面板 ——
    else if (action === 'filter_sender_panel') {
      const page = parseInt(data.substring(pipeIdx + 1).split('|')[1]) || 0;
      await showFilterSenderPanel(userId, chatId, msgId, sc, page, env);
    }

    // —— 设置指定发送者 ——
    else if (action === 'filter_sender_set') {
      const parts = data.substring(pipeIdx + 1).split('|');
      const senderId = parts[1]; // user_id 在 |sc|userId 格式的第二段
      if (senderId) {
        await upsertUserFilter(userId, sc, 'sender_user_id', senderId, env);
      }
      await showFilterSenderPanel(userId, chatId, msgId, sc, 0, env);
    }

    // —— 清除发送者筛选（不限）——
    else if (action === 'filter_sender_all') {
      await upsertUserFilter(userId, sc, 'sender_user_id', '', env);
      await showFilterPanel(userId, chatId, msgId, sc, env);
    }

    // —— 设置默认资源（NULL 发送者）——
    else if (action === 'filter_sender_null') {
      await upsertUserFilter(userId, sc, 'sender_user_id', 'null', env);
      await showFilterSenderPanel(userId, chatId, msgId, sc, 0, env);
    }

    // 🌟 V5.11: 源群选择回调路由
    // —— 源群子面板 ——
    else if (action === 'filter_source_panel') {
      const page = parseInt(data.substring(pipeIdx + 1).split('|')[1]) || 0;
      await showFilterSourcePanel(userId, chatId, msgId, sc, page, env);
    }

    // —— 切换某个源群的选中状态 ——
    else if (action === 'filter_source_toggle') {
      const parts = data.substring(pipeIdx + 1).split('|');
      const srcId = parseInt(parts[1]);
      if (srcId && !isNaN(srcId)) {
        const existing = await env.D1.prepare(
          `SELECT 1 FROM user_source_selection WHERE user_id = ? AND display_chat_id = ? AND source_chat_id = ?`
        ).bind(userId, sc, srcId).first();
        if (existing) {
          await env.D1.prepare(
            `DELETE FROM user_source_selection WHERE user_id = ? AND display_chat_id = ? AND source_chat_id = ?`
          ).bind(userId, sc, srcId).run();
        } else {
          await env.D1.prepare(
            `INSERT OR IGNORE INTO user_source_selection (user_id, display_chat_id, source_chat_id) VALUES (?, ?, ?)`
          ).bind(userId, sc, srcId).run();
        }
      }
      // 保持当前页
      const page = parseInt(parts[2]) || 0;
      await showFilterSourcePanel(userId, chatId, msgId, sc, page, env);
    }

    // —— 全部源群（清空选择 = 全部允许）——
    else if (action === 'filter_source_all') {
      await env.D1.prepare(
        `DELETE FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
      ).bind(userId, sc).run();
      await showFilterPanel(userId, chatId, msgId, sc, env);
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
  // 🌟 V5.12: 筛选器指示灯统一公式（私聊按私聊ID，群组按展示群ID）
  const hasFilter = isFilterActive(await getUserFiltersBatch(userId, chatId, env)) || await isSourceSelectionActive(userId, chatId, env);
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "你好呀！我是籽青喵 (≧∇≦) 请问今天想看点什么呢？", reply_markup: getMainMenuMarkup(hasFilter, chatId) }, env);
}

async function editMainMenu(chatId, msgId, env, userId) {
  if (chatId > 0) {
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length === 0) {
      await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "⛔ 喵... 你好像退群了呢,籽青已经把菜单收回去了哦！" }, env);
      return;
    }
  }
  // 🌟 V5.12: 筛选器指示灯统一公式（私聊按私聊ID，群组按展示群ID）
  const hasFilter = isFilterActive(await getUserFiltersBatch(userId, chatId, env)) || await isSourceSelectionActive(userId, chatId, env);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "这是籽青的主菜单,请选择喵：", reply_markup: getMainMenuMarkup(hasFilter, chatId) }, env);
}

function getMainMenuMarkup(hasFilter = false, chatId = 0) {
  const filterBtn = hasFilter ? "🔍 筛选器 🟢" : "🔍 筛选器 🔴";
  // 🌟 V5.12: 私聊设置文案去"限管理"误导，排行文案改"群排行"
  const setBtnText = chatId > 0 ? "⚙️ 我的抽取设置" : "⚙️ 籽青设置 (限管理)";
  const leaderBtnText = chatId > 0 ? "🏆 群排行" : "🏆 本群排行";
  // 🌟 V5.14.1: 仅展示群显示"检测无效媒体"按钮(管理员功能,点击后异步分批探活)
  const rows = [
    [{ text: "🎲 开始随机", callback_data: "start_random" }, { text: filterBtn, callback_data: "filter_open" }],
    [{ text: leaderBtnText, callback_data: "leaderboard" }, { text: "📁 收藏夹", callback_data: "favorites" }],
    [{ text: "📜 历史足迹", callback_data: "history" }, { text: setBtnText, callback_data: "set_main" }]
  ];
  if (chatId < 0) {
    rows.push([{ text: "🧹 检测无效媒体 (限管理)", callback_data: "scan_dead_start" }]);
  }
  return { inline_keyboard: rows };
}

async function showCategories(chatId, msgId, env, userId) {
  let keyboard = [];

  if (chatId < 0) {
    // 🌟 V5.11: 展示群分支 — 按用户当前选中的源群(空选=全部允许源)列出分类
    const allowed = await getDisplaySources(chatId, env, userId); // 含自引用 {chatId}
    const remoteCount = allowed.length - 1; // 去掉自引用后的远程源数

    // 读用户源选择（空选=全部允许源）
    const { results: selRows } = await env.D1.prepare(
      `SELECT source_chat_id FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
    ).bind(userId, chatId).all();
    const selectedIds = (selRows || []).map(r => r.source_chat_id);
    const effectiveSources = selectedIds.length > 0
      ? allowed.filter(r => selectedIds.includes(r.source_chat_id))
      : allowed.slice();

    // 收集各源群的分类（🌟 V5.13: 同名分类合并为单按钮，无源名前缀）
    const titleMap = new Map(allowed.map(r => [r.source_chat_id, r.source_chat_title]));
    const labelOf = (srcId) => titleMap.get(srcId) || (srcId === chatId ? '本群' : `群${srcId}`);
    const catQueries = effectiveSources.map(r =>
      env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE category_name != 'output' AND chat_id = ?`).bind(r.source_chat_id).all()
        .then(res => ({ srcId: r.source_chat_id, rows: (res.results || []) }))
    );
    const catResults = await Promise.all(catQueries);
    // 🌟 V5.13: 同名分类按 category_name 聚合去重，合并为单按钮（Map 保持插入顺序）
    const mergedCats = new Map();
    for (const cr of catResults) {
      for (const row of cr.rows) {
        if (!mergedCats.has(row.category_name)) mergedCats.set(row.category_name, true);
      }
    }
    for (const catName of mergedCats.keys()) {
      keyboard.push([{ text: `📂 ${catName}`, callback_data: `random_${catName}|${chatId}` }]);
    }

    // 源选择入口按钮（仅跨群场景：存在真实远程源时显示）
    if (remoteCount > 0) {
      const selLabel = selectedIds.length === 0
        ? '全部'
        : selectedIds.map(id => titleMap.get(id) || (id === chatId ? '本群' : `群${id}`)).join('+') || '全部';
      keyboard.unshift([{ text: `📡 源群选择 (当前: ${selLabel}) ➡️`, callback_data: `filter_source_panel|${chatId}|0` }]);
    }
  } else {
    // 🌟 V5.12: 私聊分支 — 仿群组同构多源逻辑
    //   源 = 用户所有授权群（空选=全部授权群）；callback 第二段=私聊ID（作用域，非源群ID）
    //   点击分类后 random_/next_ 私聊分支调 resolveEffectiveSources(私聊ID) 解析多源
    const allowed = await getDisplaySources(chatId, env, userId); // 私聊=授权群列表
    const remoteCount = allowed.length; // 私聊无私聊自引用，全是远程源

    // 读用户源选择（display_chat_id=私聊ID；空选=全部授权群）
    const { results: selRows } = await env.D1.prepare(
      `SELECT source_chat_id FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
    ).bind(userId, chatId).all();
    const selectedIds = (selRows || []).map(r => r.source_chat_id);
    const effectiveSources = selectedIds.length > 0
      ? allowed.filter(r => selectedIds.includes(r.source_chat_id))
      : allowed.slice();

    // 收集各授权群的分类（🌟 V5.13: 同名分类合并为单按钮，无源名前缀）
    const titleMap = new Map(allowed.map(r => [r.source_chat_id, r.source_chat_title]));
    const labelOf = (srcId) => titleMap.get(srcId) || `群${srcId}`;
    const catQueries = effectiveSources.map(r =>
      env.D1.prepare(`SELECT DISTINCT category_name FROM config_topics WHERE category_name != 'output' AND chat_id = ?`).bind(r.source_chat_id).all()
        .then(res => ({ srcId: r.source_chat_id, rows: (res.results || []) }))
    );
    const catResults = await Promise.all(catQueries);
    // 🌟 V5.13: 同名分类按 category_name 聚合去重，合并为单按钮（Map 保持插入顺序）
    const mergedCats = new Map();
    for (const cr of catResults) {
      for (const row of cr.rows) {
        if (!mergedCats.has(row.category_name)) mergedCats.set(row.category_name, true);
      }
    }
    for (const catName of mergedCats.keys()) {
      // callback 第二段=私聊ID（作用域），点击后 resolveEffectiveSources(私聊ID) 解析多源
      keyboard.push([{ text: `📂 ${catName}`, callback_data: `random_${catName}|${chatId}` }]);
    }

    // 源选择入口按钮（授权群>1 时显示，空选=全部）
    if (remoteCount > 1) {
      const selLabel = selectedIds.length === 0
        ? '全部'
        : selectedIds.map(id => titleMap.get(id) || `群${id}`).join('+') || '全部';
      keyboard.unshift([{ text: `📡 源群选择 (当前: ${selLabel}) ➡️`, callback_data: `filter_source_panel|${chatId}|0` }]);
    }
  }

  if (keyboard.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "呜呜,当前群组还没有绑定任何分类喵，管理员请使用 /bind 绑定哦！", reply_markup: getBackMarkup() }, env);

  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  const text = chatId < 0 ? "请选择分类喵（源群可在筛选器中切换）：" : "👇 以下是您所在群组的专属图库喵（源群可在筛选器中切换）：";
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
    : f.date_mode === 'after'
    ? `≥${f.date_from}`
    : f.date_mode === 'before'
    ? `≤${f.date_to}`
    : ({ all: '不限', today: '今天', d7: '近7天', d30: '近30天', year: '今年' }[f.date_mode] || '不限');
  const durLabel = f.duration_mode === 'gt'
    ? `>${f.duration_min}s`
    : f.duration_mode === 'lt'
    ? `<${f.duration_max}s`
    : f.duration_mode === 'range'
    ? `${f.duration_min}~${f.duration_max}s`
    : f.duration_mode === 'custom'
    ? `≤${f.duration_max}s`
    : ({ all: '不限', s30: '≤30s', s60: '≤60s', s120: '≤120s', s300: '≤5分钟' }[f.duration_mode] || '不限');

  // 🌟 V5.10: 发送者标签
  let senderLabel = '不限';
  if (f.sender_user_id === 'null') {
    senderLabel = '默认资源';
  } else if (f.sender_user_id !== '') {
    const row = await env.D1.prepare(`SELECT first_name FROM user_roster WHERE user_id = ?`).bind(parseInt(f.sender_user_id, 10)).first();
    senderLabel = row?.first_name || `用户${f.sender_user_id}`;
  }

  const sc = sourceChatId;
  // 🌟 V5.11/V5.12: 源群行（存在可选源时显示）
  //   群组：含自引用，remoteCount=allowed.length-1，>0（≥1远程源）即显示
  //   私聊：无私聊自引用，全是远程源，>1（≥2授权群）才显示（单群无需选择）
  const allowed = await getDisplaySources(sc, env, userId);
  const remoteCount = sc > 0 ? allowed.length : (allowed.length - 1);
  const showSourceRow = sc > 0 ? remoteCount > 1 : remoteCount > 0;
  let sourceRow = null;
  if (showSourceRow) {
    const titleMap = new Map(allowed.map(r => [r.source_chat_id, r.source_chat_title]));
    const { results: selRows } = await env.D1.prepare(
      `SELECT source_chat_id FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
    ).bind(userId, sc).all();
    const selectedIds = (selRows || []).map(r => r.source_chat_id);
    const sourceLabel = selectedIds.length === 0
      ? '全部'
      : selectedIds.map(id => titleMap.get(id) || (id === sc ? '本群' : `群${id}`)).join('+') || '全部';
    sourceRow = [{ text: `📡 源群：${sourceLabel} ➡️`, callback_data: `filter_source_panel|${sc}|0` }];
  }

  const text = `🔍 **随机抽取筛选器**\n`
    + `（仅影响当前群组的随机抽取功能）\n\n`
    + `🎨 媒体类型：${FILTER_MEDIA_LABEL[f.media_type] || '全部'}\n`
    + `📅 收录时间：${dateLabel}\n`
    + `⏱ 视频时长：${durLabel}\n`
    + `👤 发送者：${senderLabel}`;
  const rows = [
    [{ text: `🎨 类型：${FILTER_MEDIA_LABEL[f.media_type]} 🔄`, callback_data: `filter_media|${sc}` }],
    [
      { text: `📅 时间：${dateLabel} ➡️`, callback_data: `filter_time_panel|${sc}` },
      { text: `⏱ 时长：${durLabel} ➡️`,  callback_data: `filter_dur_panel|${sc}` }
    ],
    [{ text: `👤 发送者：${senderLabel} ➡️`, callback_data: `filter_sender_panel|${sc}|0` }]
  ];
  if (sourceRow) rows.splice(1, 0, sourceRow); // 源群行置于类型与时间之间
  rows.push([
    { text: "🗑️ 清除所有筛选", callback_data: `filter_reset|${sc}` },
    { text: "🏠 返回主菜单",   callback_data: "main_menu" }
  ]);
  const keyboard = { inline_keyboard: rows };
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: keyboard }, env);
}

// ForceReply 成功后发送新的过滤器面板（sendMessage 版本，用于保持沉浸体验）
async function sendFilterPanelNew(userId, chatId, topicId, sourceChatId, successText, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const dateLabel = f.date_mode === 'custom'
    ? `${f.date_from}~${f.date_to}`
    : f.date_mode === 'after'
    ? `≥${f.date_from}`
    : f.date_mode === 'before'
    ? `≤${f.date_to}`
    : ({ all: '不限', today: '今天', d7: '近7天', d30: '近30天', year: '今年' }[f.date_mode] || '不限');
  const durLabel = f.duration_mode === 'gt'
    ? `>${f.duration_min}s`
    : f.duration_mode === 'lt'
    ? `<${f.duration_max}s`
    : f.duration_mode === 'range'
    ? `${f.duration_min}~${f.duration_max}s`
    : f.duration_mode === 'custom'
    ? `≤${f.duration_max}s`
    : ({ all: '不限', s30: '≤30s', s60: '≤60s', s120: '≤120s', s300: '≤5分钟' }[f.duration_mode] || '不限');

  // 🌟 V5.10: 发送者标签
  let senderLabel = '不限';
  if (f.sender_user_id === 'null') {
    senderLabel = '默认资源';
  } else if (f.sender_user_id !== '') {
    const sRow = await env.D1.prepare(`SELECT first_name FROM user_roster WHERE user_id = ?`).bind(parseInt(f.sender_user_id, 10)).first();
    senderLabel = sRow?.first_name || `用户${f.sender_user_id}`;
  }

  const sc = sourceChatId;
  // 🌟 V5.11/V5.12: 源群行（存在可选源时显示；私聊无私聊自引用，>1授权群才显示）
  const allowed = await getDisplaySources(sc, env, userId);
  const remoteCount = sc > 0 ? allowed.length : (allowed.length - 1);
  const showSourceRow = sc > 0 ? remoteCount > 1 : remoteCount > 0;
  let sourceRow = null;
  if (showSourceRow) {
    const titleMap = new Map(allowed.map(r => [r.source_chat_id, r.source_chat_title]));
    const { results: selRows } = await env.D1.prepare(
      `SELECT source_chat_id FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
    ).bind(userId, sc).all();
    const selectedIds = (selRows || []).map(r => r.source_chat_id);
    const sourceLabel = selectedIds.length === 0
      ? '全部'
      : selectedIds.map(id => titleMap.get(id) || (id === sc ? '本群' : `群${id}`)).join('+') || '全部';
    sourceRow = [{ text: `📡 源群：${sourceLabel} ➡️`, callback_data: `filter_source_panel|${sc}|0` }];
  }

  const text = `${successText}\n\n`
    + `🔍 **当前筛选状态**\n`
    + `🎨 类型：${FILTER_MEDIA_LABEL[f.media_type] || '全部'}\n`
    + `📅 时间：${dateLabel}\n`
    + `⏱ 时长：${durLabel}\n`
    + `👤 发送者：${senderLabel}`;
  const rows = [
    [{ text: `🎨 类型：${FILTER_MEDIA_LABEL[f.media_type]} 🔄`, callback_data: `filter_media|${sc}` }],
    [
      { text: `📅 时间：${dateLabel} ➡️`, callback_data: `filter_time_panel|${sc}` },
      { text: `⏱ 时长：${durLabel} ➡️`,  callback_data: `filter_dur_panel|${sc}` }
    ],
    [{ text: `👤 发送者：${senderLabel} ➡️`, callback_data: `filter_sender_panel|${sc}|0` }]
  ];
  if (sourceRow) rows.splice(1, 0, sourceRow);
  rows.push([
    { text: "🗑️ 清除所有筛选", callback_data: `filter_reset|${sc}` },
    { text: "🏠 返回主菜单",   callback_data: "main_menu_new" }
  ]);
  const keyboard = { inline_keyboard: rows };
  await tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text, parse_mode: 'Markdown', reply_markup: keyboard }, env);
}

// 收录时间子面板
async function showFilterTimePanel(userId, chatId, msgId, sourceChatId, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const ck = (val) => f.date_mode === val ? '✅ ' : '';
  const ckC = f.date_mode === 'custom' ? '✅ ' : '';
  const ckA = f.date_mode === 'after' ? '✅ ' : '';
  const ckB = f.date_mode === 'before' ? '✅ ' : '';
  const sc = sourceChatId;
  const keyboard = {
    inline_keyboard: [
      [{ text: `${ck('all')}不限`,   callback_data: `filter_time_all|${sc}` },  { text: `${ck('today')}今天`,  callback_data: `filter_time_today|${sc}` }],
      [{ text: `${ck('d7')}近7天`,   callback_data: `filter_time_d7|${sc}` },   { text: `${ck('d30')}近30天`,  callback_data: `filter_time_d30|${sc}` }],
      [{ text: `${ck('year')}今年`,   callback_data: `filter_time_year|${sc}` }],
      [{ text: `${ckA}≥ 某日期`, callback_data: `filter_time_after|${sc}` }, { text: `${ckB}≤ 某日期`, callback_data: `filter_time_before|${sc}` }],
      [{ text: `${ckC}✏️ 自定义时间段`, callback_data: `filter_time_custom|${sc}` }],
      [{ text: "⬅️ 返回筛选器",     callback_data: `filter_panel|${sc}` }]
    ]
  };
  const label = f.date_mode === 'custom'
    ? `${f.date_from}~${f.date_to}`
    : f.date_mode === 'after'
    ? `≥${f.date_from}`
    : f.date_mode === 'before'
    ? `≤${f.date_to}`
    : ({ all:'不限', today:'今天', d7:'近7天', d30:'近30天', year:'今年' }[f.date_mode] || '不限');
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
  const sc = sourceChatId;
  const keyboard = {
    inline_keyboard: [
      [{ text: `${ck('all')}不限`,      callback_data: `filter_dur_all|${sc}` },  { text: `${ck('s30')}≤30秒`,   callback_data: `filter_dur_s30|${sc}` }],
      [{ text: `${ck('s60')}≤60秒`,     callback_data: `filter_dur_s60|${sc}` },  { text: `${ck('s120')}≤120秒`, callback_data: `filter_dur_s120|${sc}` }],
      [{ text: `${ck('s300')}≤5分钟`,   callback_data: `filter_dur_s300|${sc}` }, { text: `${ck('custom')}✏️ ≤自定义`, callback_data: `filter_dur_custom|${sc}` }],
      [{ text: `${ck('gt')}>大于`,      callback_data: `filter_dur_gt|${sc}` },   { text: `${ck('lt')}<小于`,    callback_data: `filter_dur_lt|${sc}` }],
      [{ text: `${ck('range')}📏 区间`, callback_data: `filter_dur_range|${sc}` }],
      [{ text: "⬅️ 返回筛选器",        callback_data: `filter_panel|${sc}` }]
    ]
  };
  let durLabel = '不限';
  if (f.duration_mode === 'gt' && f.duration_min) {
    durLabel = `>${f.duration_min}s`;
  } else if (f.duration_mode === 'lt' && f.duration_max) {
    durLabel = `<${f.duration_max}s`;
  } else if (f.duration_mode === 'range' && f.duration_min && f.duration_max) {
    durLabel = `${f.duration_min}~${f.duration_max}s`;
  } else if (f.duration_mode === 'custom' && f.duration_max) {
    durLabel = `≤${f.duration_max}s`;
  } else if (Object.prototype.hasOwnProperty.call(FILTER_DURATION_PRESET_MAP, f.duration_mode)) {
    durLabel = { s30:'≤30s', s60:'≤60s', s120:'≤120s', s300:'≤5分钟' }[f.duration_mode];
  }
  await tgAPI('editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: `⏱ **视频时长筛选**\n当前：${durLabel}\n\n💡 支持：预设值、自定义上限、大于、小于、区间`,
    parse_mode: 'Markdown', reply_markup: keyboard
  }, env);
}

// 🌟 V5.10: 发送者筛选子面板（带数量显示，分页）
// 🌟 V5.11: 跨有效源群聚合发送者计数（m.chat_id IN (...)）
async function showFilterSenderPanel(userId, chatId, msgId, sourceChatId, page, env) {
  const f = await getUserFiltersBatch(userId, sourceChatId, env);
  const sc = sourceChatId;
  const pageSize = 5;
  const offset = page * pageSize;

  // 🌟 V5.11: 解析有效源群集合（白名单∩选择∩成员资格），跨源聚合发送者
  const effSources = await resolveEffectiveSources(userId, sc, env);
  const chatIn = buildChatInClause(effSources.length > 0 ? effSources : [sc]);

  const senderCountSQL = `SELECT m.sender_user_id, r.first_name, COUNT(*) as cnt FROM media_library m LEFT JOIN user_roster r ON m.sender_user_id = r.user_id WHERE m.chat_id ${chatIn.sql} GROUP BY m.sender_user_id ORDER BY cnt DESC`;

  let countRes;
  try {
    countRes = await env.D1.prepare(senderCountSQL).bind(...chatIn.binds).all();
  } catch (e) {
    const msg = String(e?.message || '');
    // 列不存在 → 尝试内联迁移
    if (/no such column.*sender_user_id/i.test(msg)) {
      try {
        await env.D1.prepare(`ALTER TABLE media_library ADD COLUMN sender_user_id INTEGER DEFAULT NULL;`).run();
        await env.D1.prepare(`CREATE INDEX IF NOT EXISTS idx_media_chat_sender ON media_library (chat_id, sender_user_id);`).run();
        // 重试查询
        countRes = await env.D1.prepare(senderCountSQL).bind(...chatIn.binds).all();
      } catch (e2) {
        console.error('sender_user_id 列内联迁移失败:', e2?.message);
        return tgAPI('editMessageText', {
          chat_id: chatId, message_id: msgId,
          text: `⚠️ 发送者筛选初始化失败！\n请访问 Worker 域名触发数据库迁移喵～\n\n💡 \`${(e2?.message || e.message || '未知错误').substring(0, 80)}\``,
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '⬅️ 返回筛选器', callback_data: `filter_panel|${sc}` }]] }
        }, env);
      }
    } else {
      console.error('showFilterSenderPanel 查询失败:', msg);
      return tgAPI('editMessageText', {
        chat_id: chatId, message_id: msgId,
        text: `⚠️ 发送者数据读取失败喵～\n\n💡 \`${msg.substring(0, 100)}\``,
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '⬅️ 返回筛选器', callback_data: `filter_panel|${sc}` }]] }
      }, env);
    }
  }

  const rows = (countRes.results || []);

  // 分离 NULL（默认资源）和正常发送者
  const defaultRow = rows.find(r => r.sender_user_id === null);
  const senderRows = rows.filter(r => r.sender_user_id !== null);
  const totalSenders = senderRows.length + (defaultRow ? 1 : 0);

  // 构建分页键盘
  const keyboard = { inline_keyboard: [] };

  // 「不限」选项（始终在第一行）
  const isAll = f.sender_user_id === '';
  keyboard.inline_keyboard.push([{ text: `${isAll ? '✅ ' : ''}不限`, callback_data: `filter_sender_all|${sc}` }]);

  // 分页切片的发送者列表
  const pagedSenders = senderRows.slice(offset, offset + pageSize);

  for (const row of pagedSenders) {
    const name = row.first_name || `用户${row.sender_user_id}`;
    const isSelected = f.sender_user_id === String(row.sender_user_id);
    keyboard.inline_keyboard.push([{
      text: `${isSelected ? '✅ ' : ''}${name} (${row.cnt})`,
      callback_data: `filter_sender_set|${sc}|${row.sender_user_id}`
    }]);
  }

  // 「默认资源」行（仅在第一页显示，如有数据）
  if (defaultRow && page === 0) {
    const isNull = f.sender_user_id === 'null';
    keyboard.inline_keyboard.push([{
      text: `${isNull ? '✅ ' : ''}默认资源 (${defaultRow.cnt})`,
      callback_data: `filter_sender_null|${sc}`
    }]);
  }

  // 导航行
  const navRow = [];
  if (page > 0) {
    navRow.push({ text: '⬅️ 上一页', callback_data: `filter_sender_panel|${sc}|${page - 1}` });
  }
  if (offset + pageSize < senderRows.length) {
    navRow.push({ text: '下一页 ➡️', callback_data: `filter_sender_panel|${sc}|${page + 1}` });
  }
  if (navRow.length > 0) keyboard.inline_keyboard.push(navRow);

  // 返回按钮
  keyboard.inline_keyboard.push([{ text: '⬅️ 返回筛选器', callback_data: `filter_panel|${sc}` }]);

  const selLabel = f.sender_user_id === 'null'
    ? '默认资源'
    : f.sender_user_id !== ''
    ? (senderRows.find(r => String(r.sender_user_id) === f.sender_user_id)?.first_name || `用户${f.sender_user_id}`)
    : '不限';

  await tgAPI('editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: `👤 **发送者筛选**\n当前：${selLabel}\n（共 ${totalSenders} 位发送者）`,
    parse_mode: 'Markdown', reply_markup: keyboard
  }, env);
}

// 🌟 V5.11: 源群选择子面板（分页多选，✅标记已选；空选=全部允许源）
async function showFilterSourcePanel(userId, chatId, msgId, displayChatId, page, env) {
  const sc = displayChatId;
  const allowed = await getDisplaySources(sc, env, userId); // 私聊=授权群, 群组=白名单含自引用
  const pageSize = 6;
  const offset = page * pageSize;

  // 当前用户选择集
  const { results: selRows } = await env.D1.prepare(
    `SELECT source_chat_id FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
  ).bind(userId, sc).all();
  const selectedIds = new Set((selRows || []).map(r => r.source_chat_id));

  // 标题映射（自引用显示「本群」）
  const titleOf = (r) => r.source_chat_title || (r.source_chat_id === sc ? '本群' : `群${r.source_chat_id}`);

  const keyboard = { inline_keyboard: [] };

  // 「全部/不限」行（清空选择 = 全部允许源）
  const isAll = selectedIds.size === 0;
  keyboard.inline_keyboard.push([{ text: `${isAll ? '✅ ' : ''}全部源群（不限）`, callback_data: `filter_source_all|${sc}` }]);

  // 分页切片
  const paged = allowed.slice(offset, offset + pageSize);
  for (const r of paged) {
    const isSelected = selectedIds.has(r.source_chat_id);
    keyboard.inline_keyboard.push([{
      text: `${isSelected ? '✅ ' : ''}${titleOf(r)}`,
      callback_data: `filter_source_toggle|${sc}|${r.source_chat_id}|${page}`
    }]);
  }

  // 导航行
  const navRow = [];
  if (page > 0) navRow.push({ text: '⬅️ 上一页', callback_data: `filter_source_panel|${sc}|${page - 1}` });
  if (offset + pageSize < allowed.length) navRow.push({ text: '下一页 ➡️', callback_data: `filter_source_panel|${sc}|${page + 1}` });
  if (navRow.length > 0) keyboard.inline_keyboard.push(navRow);

  keyboard.inline_keyboard.push([{ text: '⬅️ 返回筛选器', callback_data: `filter_panel|${sc}` }]);

  const curLabel = isAll
    ? '全部'
    : allowed.filter(r => selectedIds.has(r.source_chat_id)).map(titleOf).join('+') || '全部';

  await tgAPI('editMessageText', {
    chat_id: chatId, message_id: msgId,
    text: `📡 **源群选择**\n当前：${curLabel}\n（共 ${allowed.length} 个可选源群，空选=全部）\n\n💡 点击切换，勾选=只看这些源，不勾任何=看全部`,
    parse_mode: 'Markdown', reply_markup: keyboard
  }, env);
}

// ====================================================================================
// 🌟 V5.11: 历史回退按 scopeChatId 作用域（群组=展示群, 私聊=远程源群，与 sendRandomMedia 历史足迹记录一致）
async function sendHistoricalMedia(userId, chatId, msgId, topicId, category, scopeChatId, offset, env, cbId) {
  let outChatId = chatId; let outTopicId = topicId;
  if (chatId < 0) {
    const output = await env.D1.prepare(`SELECT chat_id, topic_id FROM config_topics WHERE category_name = 'output' AND chat_id = ? LIMIT 1`).bind(chatId).first();
    if (output) { outChatId = output.chat_id; outTopicId = output.topic_id; }
  }

  const settings = await getSettingsBatch(scopeChatId, ['display_mode', 'next_mode', 'expose_forward_source'], env);
  const mode = settings.display_mode;
  const nextMode = settings.next_mode || 'replace';
  // 🌟 V5.14: 历史回退同样支持暴露第三方来源深链(与 sendRandomMedia 对称)
  const exposeSrc = settings.expose_forward_source === 'true';

  // 根据偏移量拉取用户历史（按 scopeChatId 记录的足迹）
  // 🌟 V5.12: 私聊 scopeChatId=私聊ID，按私聊ID聚合历史 → 跨源统一回退不再断裂
  const media = await env.D1.prepare(`
    SELECT m.* FROM user_history h
    JOIN media_library m ON h.media_id = m.id
    WHERE h.user_id = ? AND h.chat_id = ? AND m.category_name = ?
    ORDER BY h.viewed_at DESC LIMIT 1 OFFSET ?
  `).bind(userId, scopeChatId, category, offset).first();

  if (!media) return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵... 时空尽头啦，前面没有更多记录了哦！", show_alert: true }, env);

  await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "时光倒流喵~ ⏪" }, env);

  if (nextMode === 'replace') {
    try { await tgAPI('deleteMessage', { chat_id: outChatId, message_id: msgId }, env); } catch(e){}
  }

  // 拼接回退控制键盘（🌟 V5.11: prev_/next_ 编码携带 scopeChatId，保持私聊/群组一致）
  const actionKeyboard = [
    [ { text: "⏪ 继退", callback_data: `prev_${category}|${scopeChatId}|${offset + 1}` }, { text: "⏭️ 换新", callback_data: `next_${category}|${scopeChatId}` } ],
    [ { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` } ]
  ];
  // 🌟 V5.15: 始终显示「查看附近5个」(同源群·同分类时间线邻域)
  actionKeyboard.push([{ text: "📍 查看附近5个", callback_data: `near|${media.id}` }]);

  // 🌟 V5.14.1: 历史回退同样加降级直发(与 sendRandomMedia 对称)
  let sentOk = false;
  let primaryErrDesc = '';
  if (mode === 'A') {
    const res = await tgAPI('forwardMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
    const data = await res.json();
    if (data.ok) {
      sentOk = true;
      actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
      await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: data.result.message_id, text: "👆 (历史回忆) 可以点这里操作喵：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
    } else {
      primaryErrDesc = data.description || '';
    }
  } else {
    // 🌟 V5.14: 历史回退 B 模式同样支持「看看来源」按钮(与 sendRandomMedia 对称)
    if (exposeSrc && media.raw_message_json) {
      const fwdSrc = extractForwardSourceDeepLink(media.raw_message_json);
      if (fwdSrc && fwdSrc.url) {
        actionKeyboard.unshift([{ text: "🔗 看看来源", url: fwdSrc.url }]);
      }
    }
    actionKeyboard.unshift([{ text: "🔗 去原记录围观", url: makeDeepLink(media.chat_id, media.message_id) }]);
    actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
    const res = await tgAPI('copyMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
    const data = await res.json();
    if (data.ok) {
      sentOk = true;
    } else {
      primaryErrDesc = data.description || '';
    }
  }

  // 🌟 V5.14.1: 主发送失败 → 降级 file_id 直发
  if (!sentOk && media.file_id && media.media_type) {
    const fb = await sendMediaByFileId(outChatId, outTopicId, media, actionKeyboard, env);
    if (fb.ok) {
      sentOk = true;
      await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: fb.sentMessageId, text: "⚠️ 源群已失效,籽青已用文件直发兜底喵~" }, env);
    } else {
      primaryErrDesc = primaryErrDesc || fb.data?.description || '';
    }
  }

  // 🌟 V5.14.1: 真死链 → 不自动删,发询问消息等管理员确认
  if (!sentOk) {
    const errDesc = primaryErrDesc || 'unknown';
    console.error("历史回退探活彻底失败,判定死链喵:", errDesc);
    const sourceDead = isSourceGroupDead(errDesc);
    const detailText = sourceDead
      ? `🚨 <b>历史足迹发现死链喵~</b>\n\n📦 类型: 源群整体失效\n📁 源群 chat_id: <code>${media.chat_id}</code>\n📋 媒体 id: <code>${media.id}</code>\n💬 错误: <code>${errDesc}</code>\n\n⚠️ 该源群已无法访问,是否清理？`
      : `🚨 <b>历史足迹发现死链喵~</b>\n\n📦 类型: 单条媒体失效\n📋 媒体 id: <code>${media.id}</code>\n📁 源群: <code>${media.chat_id}</code>\n💬 错误: <code>${errDesc}</code>\n\n该媒体已无法展现,是否清理？`;
    await tgAPI('sendMessage', {
      chat_id: outChatId, message_thread_id: outTopicId, text: detailText, parse_mode: 'HTML',
      reply_markup: { inline_keyboard: [
        [{ text: "✅ 确认清理", callback_data: `deadpurge|${media.id}|${sourceDead ? 'g' : 's'}` }, { text: "⏭️ 跳过", callback_data: `deadpurge|${media.id}|skip` }]
      ] }
    }, env);
    return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "呜呜,这条历史记录已失效,已通知清理喵~" }, env);
  }
}

// ==== 核心抽取与展现逻辑 ====
// 🌟 V5.12: sourceChatIds 为数组（群组/私聊均由 resolveEffectiveSources 解析多源）
// 作用域 scopeChatId：群组=展示群 chatId（多源设置/筛选器/历史）, 私聊=私聊ID本身（按私聊ID独立存设置/筛选器/历史）
async function sendRandomMedia(userId, chatId, msgId, topicId, category, sourceChatIds, isNext, env, ctx, cbId) {
  const ids = (sourceChatIds && sourceChatIds.length) ? sourceChatIds : [chatId];

  // 成员资格校验：私聊对每个远程源群校验；群组则在 resolveEffectiveSources 已过滤，这里兜底
  // 🌟 V5.13: 私聊场景始终校验(个人授权语义,不接 source_membership_check 开关)；群组由 resolveEffectiveSources 按展示群开关决定
  if (chatId > 0) {
    const accessible = await verifySourceMembership(userId, ids, env);
    if (accessible.length === 0) {
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

  // P1: 批量读取所有设置 + 用户过滤器
  // 🌟 V5.12: 作用域 scopeChatId — 群组=展示群chatId(多源), 私聊=私聊ID本身(按私聊ID独立存设置/筛选器)
  //   ids=多源查询集合（群组=resolveEffectiveSources结果, 私聊=用户勾选的授权群子集）
  const scopeChatId = chatId;
  const [settings, filters] = await Promise.all([
    getSettingsBatch(scopeChatId, ['display_mode', 'anti_repeat', 'auto_jump', 'show_success', 'next_mode', 'strict_skip', 'vote_enabled', 'vote_threshold', 'vote_scope', 'expose_forward_source'], env),
    getUserFiltersBatch(userId, scopeChatId, env)
  ]);
  const filterActive = isFilterActive(filters);
  const filterStatus = filterActive ? await renderFilterStatus(filters, env) : null;
  const mode = settings.display_mode;
  const useAntiRepeat = settings.anti_repeat === 'true';
  const autoJump = settings.auto_jump === 'true';
  const showSuccess = settings.show_success === 'true';
  const nextMode = settings.next_mode || 'replace';
  const strictSkip = settings.strict_skip === 'true';
  // 🌟 V5.11: 投票相关设置
  const voteEnabled = settings.vote_enabled === 'true';
  const voteThreshold = parseInt(settings.vote_threshold, 10) || 5;
  // 🌟 V5.14: 暴露第三方转发来源深链开关(仅展示群管理员开启;B模式copyMessage时显示「看看来源」按钮)
  const exposeSrc = settings.expose_forward_source === 'true';
  const now = Date.now();

  // 按展示群隐藏的 displayChatId（仅群组场景生效；私聊不隐藏）
  const hideDisplayChatId = chatId < 0 ? chatId : null;

  // 多源 IN 子句（防重重置分支复用）
  const chatIn = buildChatInClause(ids);

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

    media = await selectRandomMedia(category, ids, useAntiRepeat, excludeMediaId, filters, env, hideDisplayChatId);

    if (!media && useAntiRepeat) {
      const { sql: fSql, binds: fBinds } = buildFilterWhereClause(filters, 'm');
      const hideClause = hideDisplayChatId
        ? `AND NOT EXISTS (SELECT 1 FROM media_hide h WHERE h.media_id = m.id AND h.display_chat_id = ?)`
        : '';
      const cntBinds = [category, ...chatIn.binds];
      if (hideDisplayChatId) cntBinds.push(hideDisplayChatId);
      cntBinds.push(...fBinds);
      const totalCheck = await env.D1.prepare(
        `SELECT count(*) as c FROM media_library m WHERE m.category_name = ? AND m.chat_id ${chatIn.sql} ${hideClause}${fSql}`
      ).bind(...cntBinds).first();
      if (totalCheck && totalCheck.c > 0) {
        const resetBinds = [category, ...chatIn.binds];
        if (hideDisplayChatId) resetBinds.push(hideDisplayChatId);
        resetBinds.push(...fBinds);
        await env.D1.prepare(
          `DELETE FROM served_history WHERE media_id IN (SELECT m.id FROM media_library m WHERE m.category_name = ? AND m.chat_id ${chatIn.sql} ${hideClause}${fSql})`
        ).bind(...resetBinds).run();
        const resetMsg = filterActive
          ? `🎉 哇哦,【${category}】在当前筛选条件下已全看光！防重库已重置喵~\n🔍 ${filterStatus}`
          : `🎉 哇哦,【${category}】的内容全看光了！籽青已重置防重库喵~`;
        await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, text: resetMsg }, env);
        media = await selectRandomMedia(category, ids, false, excludeMediaId, filters, env, hideDisplayChatId);
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

    // 🌟 双排控制按钮 (带上 ⏪ 上一个) — V5.11: prev_/next_ 编码携带 scopeChatId（群组=展示群,私聊=远程源群）
    const actionKeyboard = [
      [ { text: "⏪ 上一个", callback_data: `prev_${category}|${scopeChatId}|1` }, { text: "⏭️ 换一个喵", callback_data: `next_${category}|${scopeChatId}` } ],
      [ { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` } ]
    ];

    // 🌟 V5.11: 投票移除按钮（仅展示群开启 vote_enabled 时显示，私聊不显示）
    if (voteEnabled && chatId < 0) {
      const vc = media.vote_count ?? 0;
      actionKeyboard.splice(1, 0, [{ text: `👎 移除投票 (${vc}/${voteThreshold})`, callback_data: `vote_remove|${media.id}` }]);
    }

    // 🌟 V5.15: 始终显示「查看附近5个」(同源群·同分类时间线邻域，不依赖看看来源是否可用)
    actionKeyboard.push([{ text: "📍 查看附近5个", callback_data: `near|${media.id}` }]);

    const originalDeepLink = makeDeepLink(media.chat_id, media.message_id);

    // 🌟 V5.14.1: 探活降级链 — forward/copy 失败 → 降级 file_id 直发 → 仍失败才判定死链
    //   核心原则:源群死亡≠媒体死亡。file_id 托管在 TG CDN,源群没了文件可能仍可用,不能直接删
    let sentOk = false;
    let usedFallback = false; // 标记本次是否走了降级直发(用于通知文案)
    let primaryErrDesc = '';

    if (mode === 'A') {
      const res = await tgAPI('forwardMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id }, env);
      const data = await res.json();
      if (data.ok) {
        sentOk = true;
        newSentMessageId = data.result.message_id;
        actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
        await tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: newSentMessageId, text: "👆 可以点这里操作喵：", reply_markup: { inline_keyboard: actionKeyboard } }, env);
      } else {
        primaryErrDesc = data.description || '';
      }
    } else {
      // 🌟 V5.14: 暴露来源开关开启 + 媒体有第三方转发来源 → 在「去原记录围观」上方插入「看看来源」按钮
      //   绕过 B 源群成员限制,直接跳第三方 X 原消息(C群用户不在B也能跳)
      if (exposeSrc && media.raw_message_json) {
        const fwdSrc = extractForwardSourceDeepLink(media.raw_message_json);
        if (fwdSrc && fwdSrc.url) {
          actionKeyboard.unshift([{ text: "🔗 看看来源", url: fwdSrc.url }]);
        }
      }
      actionKeyboard.unshift([{ text: "🔗 去原记录围观", url: originalDeepLink }]);
      actionKeyboard.push([{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]);
      const res = await tgAPI('copyMessage', { chat_id: outChatId, message_thread_id: outTopicId, from_chat_id: media.chat_id, message_id: media.message_id, reply_markup: { inline_keyboard: actionKeyboard } }, env);
      const data = await res.json();
      if (data.ok) {
        sentOk = true;
        newSentMessageId = data.result.message_id;
      } else {
        primaryErrDesc = data.description || '';
      }
    }

    // 🌟 V5.14.1: 主发送失败 → 降级用 file_id 直发(绕过源群,直走 TG CDN)
    if (!sentOk && media.file_id && media.media_type) {
      console.error("主发送失败,降级 file_id 直发喵:", primaryErrDesc);
      const fallbackKeyboard = [
        [ { text: "⏪ 上一个", callback_data: `prev_${category}|${scopeChatId}|1` }, { text: "⏭️ 换一个喵", callback_data: `next_${category}|${scopeChatId}` } ],
        [ { text: "❤️ 收藏", callback_data: `fav_add_${media.id}` } ],
        [ { text: "📍 查看附近5个", callback_data: `near|${media.id}` } ],
        [ { text: "🏠 呼出主菜单", callback_data: "main_menu_new" } ]
      ];
      const fb = await sendMediaByFileId(outChatId, outTopicId, media, fallbackKeyboard, env);
      if (fb.ok) {
        sentOk = true;
        usedFallback = true;
        newSentMessageId = fb.sentMessageId;
      }
    }

    if (sentOk) {
      foundValid = true;
      // 🌟 V5.14.1: 降级成功时给用户一个轻提示(源群失效但文件仍可用)
      if (usedFallback) {
        ctx.waitUntil(
          tgAPI('sendMessage', { chat_id: outChatId, message_thread_id: outTopicId, reply_to_message_id: newSentMessageId, text: "⚠️ 源群已失效,籽青已用文件直发兜底喵~" }, env)
        );
      }
    } else {
      // 🌟 V5.14.1: 真死链 — 主发送+降级全失败,不自动删,发询问消息让管理员确认
      //   附详细信息(媒体id/源群/分类/错误描述/源群死亡判定),按钮「确认清理」/「跳过」
      const errDesc = primaryErrDesc || 'unknown';
      console.error("探活彻底失败,判定死链喵:", errDesc);
      const sourceDead = isSourceGroupDead(errDesc);
      const detailText = sourceDead
        ? `🚨 <b>发现死链喵~</b>\n\n📦 类型: 源群整体失效\n📁 源群 chat_id: <code>${media.chat_id}</code>\n📋 媒体 id: <code>${media.id}</code>\n🏷️ 分类: ${category}\n💬 错误: <code>${errDesc}</code>\n\n⚠️ 该源群已无法访问,但源群死亡≠媒体全部死亡(file_id 可能仍托管在 TG CDN)。请主人确认是否清理？`
        : `🚨 <b>发现死链喵~</b>\n\n📦 类型: 单条媒体失效\n📋 媒体 id: <code>${media.id}</code>\n📁 源群: <code>${media.chat_id}</code>\n🏷️ 分类: ${category}\n💬 错误: <code>${errDesc}</code>\n\n该媒体已无法展现,请主人确认是否清理？`;
      await tgAPI('sendMessage', {
        chat_id: outChatId, message_thread_id: outTopicId, text: detailText, parse_mode: 'HTML',
        reply_markup: { inline_keyboard: [
          [{ text: "✅ 确认清理", callback_data: `deadpurge|${media.id}|${sourceDead ? 'g' : 's'}` }, { text: "⏭️ 跳过", callback_data: `deadpurge|${media.id}|skip` }]
        ] }
      }, env);
    }
  }

  if (!foundValid) {
    return tgAPI('sendMessage', { chat_id: chatId, message_thread_id: topicId, text: "🧹 呼... 连续抽到好多失效媒体,已发通知到输出话题队列等待管理员确认清理,请主人稍后再试喵~" }, env);
  }

  ctx.waitUntil(Promise.all([
    useAntiRepeat ? env.D1.prepare(`INSERT OR IGNORE INTO served_history (media_id) VALUES (?)`).bind(media.id).run() : Promise.resolve(),
    env.D1.prepare(`INSERT INTO last_served (user_id, last_media_id, served_at) VALUES (?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_media_id=excluded.last_media_id, served_at=excluded.served_at`).bind(userId, media.id, now).run(),
    env.D1.prepare(`UPDATE media_library SET view_count = view_count + 1 WHERE id = ?`).bind(media.id).run(),
    // 🌟 V5.12: 历史足迹按 scopeChatId 记录（群组=展示群, 私聊=私聊ID聚合，与 sendHistoricalMedia JOIN 一致）
    //   私聊按私聊ID聚合 → 跨源统一回退（不再断裂）；私聊不写 group_history（无私聊群级别足迹入口）
    env.D1.prepare(`INSERT INTO user_history (user_id, chat_id, media_id) VALUES (?, ?, ?)`).bind(userId, scopeChatId, media.id).run(),
    chatId < 0 ? env.D1.prepare(`INSERT INTO group_history (chat_id, media_id) VALUES (?, ?)`).bind(scopeChatId, media.id).run() : Promise.resolve()
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
// 🌟 V5.12: targetGroup 参数 — 私聊选群后传入目标群ID；群组默认 null（用 chatId=展示群）
async function showLeaderboard(chatId, msgId, page, env, targetGroup = null) {
  // 私聊未选群兜底（正常不会走到，路由已拦截）
  if (chatId > 0 && targetGroup === null) {
    return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "请先选择要查看排行的群喵~", reply_markup: getBackMarkup() }, env);
  }
  const scopeGroup = targetGroup !== null ? targetGroup : chatId;
  const isPrivateView = chatId > 0; // 私聊查看某群排行
  const limit = 5, offset = page * limit;
  const [leaderData, totalRes] = await Promise.all([
    env.D1.prepare(`SELECT chat_id, message_id, category_name, view_count, caption FROM media_library WHERE view_count > 0 AND chat_id = ? ORDER BY view_count DESC LIMIT ? OFFSET ?`).bind(scopeGroup, limit, offset).all(),
    env.D1.prepare(`SELECT count(*) as c FROM media_library WHERE view_count > 0 AND chat_id = ?`).bind(scopeGroup).first()
  ]);

  const escapeHTML = (str) => String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // 🌟 V5.12: 私聊排行标题显示群名
  let groupTitle = '本群';
  if (isPrivateView) {
    const tRow = await env.D1.prepare(`SELECT chat_title FROM config_topics WHERE chat_id = ? LIMIT 1`).bind(scopeGroup).first();
    groupTitle = (tRow && tRow.chat_title) ? escapeHTML(tRow.chat_title) : `群${scopeGroup}`;
  }
  let text = isPrivateView ? `🏆 <b>${groupTitle} 浏览量排行榜喵</b>\n\n` : "🏆 <b>本群浏览量排行榜喵</b>\n\n";
  if (!leaderData.results || leaderData.results.length === 0) {
    text += "当前群组还没有产生播放数据呢~";
  } else {
    leaderData.results.forEach((row, idx) => {
      const safeCaption = escapeHTML(row.caption ? row.caption.substring(0, 15) : '记录');
      text += `${offset + idx + 1}. [${escapeHTML(row.category_name)}] <a href="${makeDeepLink(row.chat_id, row.message_id)}">${safeCaption}</a> - 浏览: ${row.view_count}\n`;
    });
  }

  const keyboard = []; const navRow = [];
  // 🌟 V5.12: 私聊排行翻页编码 leader_pick_pg|{群ID}|{页码}，群组不变 leader_page_{页码}
  const prevCb = isPrivateView ? `leader_pick_pg|${scopeGroup}|${page - 1}` : `leader_page_${page - 1}`;
  const nextCb = isPrivateView ? `leader_pick_pg|${scopeGroup}|${page + 1}` : `leader_page_${page + 1}`;
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: prevCb });
  if (offset + limit < totalRes.c) navRow.push({ text: "下一页 ➡️", callback_data: nextCb });
  if (navRow.length > 0) keyboard.push(navRow);
  // 私聊排行返回时回到选群面板，群组返回主菜单
  keyboard.push([{ text: isPrivateView ? "⬅️ 返回选群" : "🏠 返回主菜单", callback_data: isPrivateView ? `leader_pick_page_0` : "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', disable_web_page_preview: true, reply_markup: { inline_keyboard: keyboard } }, env);
}

// 🌟 V5.12: 私聊群排行选群面板（列出用户授权群，点击进入该群排行）
async function showLeaderGroupPicker(userId, chatId, msgId, page, env) {
  const allowedGroups = await getUserAllowedGroups(userId, env);
  if (allowedGroups.length === 0) {
    return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "你还没有加入任何授权群喵,无法查看排行哦", reply_markup: getBackMarkup() }, env);
  }
  // 取群名
  const placeholders = allowedGroups.map(() => '?').join(', ');
  const { results } = await env.D1.prepare(
    `SELECT chat_id, chat_title FROM config_topics WHERE chat_id IN (${placeholders}) GROUP BY chat_id`
  ).bind(...allowedGroups).all();
  const titleMap = new Map((results || []).map(r => [r.chat_id, r.chat_title]));

  const limit = 6, offset = page * limit;
  const paged = allowedGroups.slice(offset, offset + limit);
  const keyboard = paged.map(g => [{ text: `🏆 ${titleMap.get(g) || `群${g}`}`, callback_data: `leader_pick_|${g}` }]);
  const navRow = [];
  if (page > 0) navRow.push({ text: "⬅️ 上一页", callback_data: `leader_pick_page_${page - 1}` });
  if (offset + limit < allowedGroups.length) navRow.push({ text: "下一页 ➡️", callback_data: `leader_pick_page_${page + 1}` });
  if (navRow.length > 0) keyboard.push(navRow);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "🏆 请选择要查看排行的群喵：", reply_markup: { inline_keyboard: keyboard } }, env);
}

async function handleAddFavorite(userId, cbId, mediaId, env) {
  try { await env.D1.prepare(`INSERT INTO user_favorites (user_id, media_id) VALUES (?, ?)`).bind(userId, mediaId).run(); await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "收藏成功喵！籽青帮你记下来啦~ ❤️", show_alert: true }, env); } catch (e) { await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "喵？你已经收藏过这个啦~", show_alert: true }, env); }
}

// 🌟 V5.11: 投票移除处理（按展示群作用域，不破坏源群数据）
async function handleVoteRemove(userId, displayChatId, msgId, mediaId, cbId, env) {
  // 仅群组场景生效（私聊不应出现投票按钮）
  if (displayChatId > 0) return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "私聊不支持投票喵", show_alert: true }, env);

  const settings = await getSettingsBatch(displayChatId, ['vote_enabled', 'vote_threshold', 'vote_scope'], env);
  if (settings.vote_enabled !== 'true') {
    return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "本群未开启投票移除喵", show_alert: true }, env);
  }

  // 安全复检：用户须仍在展示群
  const inGroup = await isUserInGroup(displayChatId, userId, env);
  if (!inGroup) {
    return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "你已不在本群，无法投票喵", show_alert: true }, env);
  }

  const threshold = parseInt(settings.vote_threshold, 10) || 5;

  // 插入投票（防重复：PK 命中则 changes=0）
  const ins = await env.D1.prepare(
    `INSERT OR IGNORE INTO media_votes (media_id, user_id) VALUES (?, ?)`
  ).bind(mediaId, userId).run();
  if (!ins.meta || ins.meta.changes === 0) {
    return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "你已经投过票啦喵~", show_alert: true }, env);
  }

  // 读最新计票（触发器已自增）
  const row = await env.D1.prepare(`SELECT vote_count, chat_id FROM media_library WHERE id = ?`).bind(mediaId).first();
  if (!row) {
    return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "该媒体已不存在喵", show_alert: true }, env);
  }
  const vc = row.vote_count ?? 0;

  // 达阈值执行移除
  if (vc >= threshold) {
    if (settings.vote_scope === 'delete') {
      // 危险开关：源库物理删除（跨群影响）
      await batchDeleteMediaByIds([mediaId], env);
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: `🗳️ 投票达 ${threshold} 票，该媒体已从源库永久移除喵！`, show_alert: true }, env);
    } else {
      // 默认安全：仅本展示群隐藏（源群与其他展示群不受影响）
      await env.D1.prepare(
        `INSERT OR IGNORE INTO media_hide (display_chat_id, media_id) VALUES (?, ?)`
      ).bind(displayChatId, mediaId).run();
      await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: `🗳️ 投票达 ${threshold} 票，本群已隐藏该媒体喵（源群不受影响）`, show_alert: true }, env);
    }
  } else {
    await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: `已投票 ${vc}/${threshold} 喵~`, show_alert: false }, env);
  }
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
  const settings = await getSettingsBatch(chatId, ['display_mode', 'anti_repeat', 'auto_jump', 'dup_notify', 'show_success', 'next_mode', 'strict_skip', 'vote_enabled', 'vote_threshold', 'vote_scope', 'ingest_enabled', 'source_membership_check', 'expose_forward_source'], env);
  const mode = settings.display_mode;
  const repeat = settings.anti_repeat;
  const jump = settings.auto_jump;
  const dup = settings.dup_notify;
  const showSuccess = settings.show_success;
  const nextMode = settings.next_mode;
  const strictSkip = settings.strict_skip;
  // 🌟 V5.11: 跨群共享与投票相关设置
  const voteEnabled = settings.vote_enabled;
  const voteThreshold = settings.vote_threshold;
  const voteScope = settings.vote_scope;
  const ingestEnabled = settings.ingest_enabled;
  const memberCheck = settings.source_membership_check;  // 🌟 V5.13: 源群成员校验开关
  const exposeSrc = settings.expose_forward_source;      // 🌟 V5.14: 暴露第三方转发来源深链开关

  // 判断是否为跨群展示群（存在真实远程源）
  const allowed = await getDisplaySources(chatId, env, null); // 群组专属面板，无私聊调用
  const isDisplayGroup = allowed.length - 1 > 0;

  const text = "⚙️ **本群的独立控制面板喵**\n\n请主人调整下方的功能开关：";
  const keyboard = [
    [{ text: `🔀 展现形式: ${mode === 'A' ? 'A(原生转发)' : 'B(复制+链接)'}`, callback_data: "set_toggle_mode" }],
    [{ text: `🔁 防重库机制: ${repeat === 'true' ? '✅ 已开启' : '❌ 未开启'}`, callback_data: "set_toggle_repeat" }],
    [{ text: `⏱️ 快划跳过模式: ${strictSkip === 'true' ? '🔥 严格消耗(强制防重)' : '♻️ 稍后再看(正常防重)'}`, callback_data: "set_toggle_strict" }],
    [{ text: `🔕 重复收录提示: ${dup === 'true' ? '📢 消息提醒' : '🔇 静默拦截'}`, callback_data: "set_toggle_dup" }],
    [{ text: `🔄 '换一个'模式: ${nextMode === 'replace' ? '🖼️ 原地替换(删旧发新)' : '💬 发新消息(保留历史)'}`, callback_data: "set_toggle_nextmode" }],
    [{ text: `🔔 抽取成功提示: ${showSuccess === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_success" }],
    [{ text: `🚀 抽取后生成跳转: ${jump === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_jump" }]
  ];

  // 🌟 V5.11: 跨群共享与投票区（展示群显示源管理；所有群显示投票与收录开关）
  keyboard.push([{ text: `📦 本群收录: ${ingestEnabled === 'true' ? '✅ 双向(收录+展示)' : '👁️ 仅展示'}`, callback_data: "set_toggle_ingest" }]);
  if (isDisplayGroup) {
    keyboard.push([{ text: "📡 管理源群白名单", callback_data: "set_source_list" }]);
    // 🌟 V5.13: 源群成员校验开关（仅展示群显示；关闭=分发模式，陌生人可看源群内容，高危）
    keyboard.push([{ text: `🔐 源群成员校验: ${memberCheck === 'false' ? '❌ 关闭(分发模式,无需在源群)' : '✅ 开启(用户须在源群)'}`, callback_data: "set_toggle_member_check" }]);
    // 🌟 V5.14: 暴露第三方转发来源深链开关（仅展示群显示；开启后B模式展现显示「看看来源」按钮，绕过源群成员限制跳第三方原消息）
    keyboard.push([{ text: `🔗 暴露来源: ${exposeSrc === 'true' ? '✅ 开启(显示第三方来源链接)' : '❌ 关闭(默认,不暴露来源)'}`, callback_data: "set_toggle_expose_src" }]);
  }
  keyboard.push([{ text: `🗳️ 投票移除: ${voteEnabled === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_vote_enabled" }]);
  if (voteEnabled === 'true') {
    keyboard.push([
      { text: `📉 阈值: ${voteThreshold}`, callback_data: "set_vote_threshold_prompt" },
      { text: `🩹 作用: ${voteScope === 'delete' ? '🗑️ 源库删除' : '🙈 仅本群隐藏'}`, callback_data: "set_toggle_vote_scope" }
    ]);
  }

  keyboard.push([{ text: "🗑️ 管理本群解绑", callback_data: "set_unbind_list" }, { text: "📊 本群超级数据看板", callback_data: "set_stats" }]);
  keyboard.push([{ text: "⚠️ 危险操作区 (清空本群数据)", callback_data: "set_danger_zone" }]);
  keyboard.push([{ text: "🏠 返回主菜单", callback_data: "main_menu" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

async function toggleSetting(key, env, chatId, msgId, values) {
  const current = await getSetting(chatId, key, env);
  const valCurrent = current === null ? values[0] : current;
  const next = valCurrent === values[0] ? values[1] : values[0];

  await env.D1.prepare(`INSERT INTO chat_settings (chat_id, key, value) VALUES (?, ?, ?) ON CONFLICT(chat_id, key) DO UPDATE SET value=excluded.value`).bind(chatId, key, next).run();

  await showSettingsMain(chatId, msgId, env);
}

// 🌟 V5.12: 私聊精简设置面板（仅 6 个抽取展现开关，按私聊ID独立存，不影响任何群组）
async function showSettingsMainPrivate(chatId, msgId, env) {
  const settings = await getSettingsBatch(chatId, ['display_mode', 'anti_repeat', 'auto_jump', 'show_success', 'next_mode', 'strict_skip'], env);
  const text = "⚙️ **你的私聊抽取设置喵**\n\n仅影响你在私聊中的随机抽取展现（独立存储，与群组设置互不影响）";
  const keyboard = [
    [{ text: `🔀 展现形式: ${settings.display_mode === 'A' ? 'A(原生转发)' : 'B(复制+链接)'}`, callback_data: "set_toggle_mode" }],
    [{ text: `🔁 防重库机制: ${settings.anti_repeat === 'true' ? '✅ 已开启' : '❌ 未开启'}`, callback_data: "set_toggle_repeat" }],
    [{ text: `⏱️ 快划跳过模式: ${settings.strict_skip === 'true' ? '🔥 严格消耗(强制防重)' : '♻️ 稍后再看(正常防重)'}`, callback_data: "set_toggle_strict" }],
    [{ text: `🔄 '换一个'模式: ${settings.next_mode === 'replace' ? '🖼️ 原地替换(删旧发新)' : '💬 发新消息(保留历史)'}`, callback_data: "set_toggle_nextmode" }],
    [{ text: `🔔 抽取成功提示: ${settings.show_success === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_success" }],
    [{ text: `🚀 抽取后生成跳转: ${settings.auto_jump === 'true' ? '✅ 开启' : '❌ 关闭'}`, callback_data: "set_toggle_jump" }],
    [{ text: "🏠 返回主菜单", callback_data: "main_menu" }]
  ];
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
}

// 🌟 V5.12: 私聊设置切换（chat_id=私聊ID正，与群组负ID隔离；刷新私聊面板）
async function toggleSettingPrivate(key, env, chatId, msgId, values) {
  const current = await getSetting(chatId, key, env);
  const valCurrent = current === null ? values[0] : current;
  const next = valCurrent === values[0] ? values[1] : values[0];
  await env.D1.prepare(`INSERT INTO chat_settings (chat_id, key, value) VALUES (?, ?, ?) ON CONFLICT(chat_id, key) DO UPDATE SET value=excluded.value`).bind(chatId, key, next).run();
  await showSettingsMainPrivate(chatId, msgId, env);
}

async function showUnbindList(chatId, msgId, env) {
  const { results } = await env.D1.prepare(`SELECT id, chat_title, category_name FROM config_topics WHERE chat_id = ?`).bind(chatId).all();
  if (!results || results.length === 0) return tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "本群目前没有绑定任何记录喵~", reply_markup: { inline_keyboard: [[{text: "返回设置", callback_data: "set_main"}]] } }, env);
  const keyboard = results.map(r => [{ text: `🗑️ 解绑 [${r.category_name}]`, callback_data: `set_unbind_do_${r.id}` }]);
  keyboard.push([{ text: "⬅️ 返回设置", callback_data: "set_main" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text: "点击对应按钮解除本群的话题绑定喵：", reply_markup: { inline_keyboard: keyboard } }, env);
}

// 🌟 V5.11: 管理员源群白名单列表（带解绑按钮）
async function showSourceList(chatId, msgId, env) {
  const { results } = await env.D1.prepare(
    `SELECT source_chat_id, source_chat_title FROM group_sources WHERE display_chat_id = ? AND source_chat_id != ? ORDER BY added_at ASC`
  ).bind(chatId, chatId).all();
  const rows = results || [];
  const text = rows.length === 0
    ? "📡 本群尚未绑定任何源群喵~\n\n用 `/bind_source` 绑定源群（回复源群转发消息,或带 ID/@用户名）"
    : `📡 **本群已绑定的源群白名单**\n\n点击按钮解绑对应源群喵：`;
  const keyboard = rows.map(r => [{
    text: `🗑️ 解绑 ${r.source_chat_title || `(${r.source_chat_id})`}`,
    callback_data: `set_source_unbind_${r.source_chat_id}`
  }]);
  keyboard.push([{ text: "⬅️ 返回设置", callback_data: "set_main" }]);
  await tgAPI('editMessageText', { chat_id: chatId, message_id: msgId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: keyboard } }, env);
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

// 🌟 V5.11: 跨群共享工具函数 ============================================================

// 构建 chat_id IN (...) 子句（占位符数组 + 绑定值）
// 单值时退化为 IN (?) 等价 = ?，保证向后兼容
function buildChatInClause(ids) {
  const list = (ids && ids.length) ? ids : [0];
  const placeholders = list.map(() => '?').join(', ');
  return { sql: `IN (${placeholders})`, binds: list };
}

// 获取显示群允许的源群白名单（含自引用：恒 union {displayChatId}）
// 返回 [{ source_chat_id, source_chat_title }]
// 🌟 V5.12: 私聊(displayChatId>0)场景 — 源=用户所有授权群(无 group_sources 白名单概念)
//   私聊本身不是源群，故不 unshift 自引用；群组分支保持 V5.11 语义不变
async function getDisplaySources(displayChatId, env, userId = null) {
  // 私聊分支：源群 = 用户所在的所有授权群
  if (displayChatId > 0) {
    if (!userId) return [];
    const allowedGroups = await getUserAllowedGroups(userId, env);
    if (allowedGroups.length === 0) return [];
    const placeholders = allowedGroups.map(() => '?').join(', ');
    const { results } = await env.D1.prepare(
      `SELECT chat_id, chat_title FROM config_topics WHERE chat_id IN (${placeholders}) GROUP BY chat_id`
    ).bind(...allowedGroups).all();
    // 去重映射（GROUP BY 已去重，但保留首个 title）
    const titleMap = new Map();
    for (const r of (results || [])) titleMap.set(r.chat_id, r.chat_title);
    return allowedGroups.map(gid => ({ source_chat_id: gid, source_chat_title: titleMap.get(gid) || null }));
  }
  // 群组分支（V5.11 原逻辑不变）
  const { results } = await env.D1.prepare(
    `SELECT source_chat_id, source_chat_title FROM group_sources WHERE display_chat_id = ?`
  ).bind(displayChatId).all();
  const rows = (results || []).filter(r => r.source_chat_id !== displayChatId);
  // 自引用：显示群自身恒为允许源
  rows.unshift({ source_chat_id: displayChatId, source_chat_title: null });
  return rows;
}

// 并行校验用户对多个源群的成员资格（复用 isUserInGroup 60s 缓存），静默过滤无权源
async function verifySourceMembership(userId, sourceChatIds, env) {
  if (!sourceChatIds || sourceChatIds.length === 0) return [];
  const checks = await Promise.all(sourceChatIds.map(sid => isUserInGroup(sid, userId, env)));
  return sourceChatIds.filter((sid, i) => checks[i]);
}

// 解析有效源群集合：白名单 ∩ 用户选择 ∩ 成员资格
// 用户空选 = 全部允许源（零摩擦首体验）；返回 number[]
// 🌟 V5.12: 私聊(displayChatId>0)场景 — 白名单=getUserAllowedGroups(授权群)，存储与解析同构
async function resolveEffectiveSources(userId, displayChatId, env) {
  const allowed = await getDisplaySources(displayChatId, env, userId);
  const allowedIds = allowed.map(r => r.source_chat_id);

  const { results: selRows } = await env.D1.prepare(
    `SELECT source_chat_id FROM user_source_selection WHERE user_id = ? AND display_chat_id = ?`
  ).bind(userId, displayChatId).all();
  const selected = (selRows || []).map(r => r.source_chat_id);

  // 用户选择须在白名单内（防撤销源被残留选择命中）；空选 = 全部允许
  const candidate = selected.length > 0
    ? allowedIds.filter(id => selected.includes(id))
    : allowedIds.slice();

  // 🌟 V5.13: 展示群可配置关闭源群成员校验（分发模式）；私聊保持校验
  //   关闭后 B 群陌生人可看 A 源内容（bot 在源群已由 /bind_source 校验 copyMessage 可行性）
  if (displayChatId < 0) {
    const check = await getSetting(displayChatId, 'source_membership_check', env);
    if (check === 'false') return candidate;  // 跳过成员校验，直接返回候选
  }
  // 成员资格过滤
  return await verifySourceMembership(userId, candidate, env);
}

// 判断用户是否在显示群内激活了源选择（用于主菜单筛选器指示灯）
async function isSourceSelectionActive(userId, displayChatId, env) {
  const { results } = await env.D1.prepare(
    `SELECT 1 FROM user_source_selection WHERE user_id = ? AND display_chat_id = ? LIMIT 1`
  ).bind(userId, displayChatId).all();
  return !!(results && results.length > 0);
}

// 写入单个群组设置（仿 upsertUserFilter，用于 chat_settings UPSERT）
async function upsertChatSetting(chatId, key, value, env) {
  const v = value == null ? '' : String(value);
  await env.D1.prepare(
    `INSERT INTO chat_settings (chat_id, key, value) VALUES (?, ?, ?) ON CONFLICT(chat_id, key) DO UPDATE SET value = excluded.value`
  ).bind(chatId, key, v).run();
}

// 🌟 V5.11: 解析 /bind_source / /unbind_source 的源群目标
// 优先级：回复转发消息(forward_origin/forward_from_chat) > 参数@用户名 > 参数数字ID
// 返回 { id, title } 或 null（解析失败）
async function resolveSourceTarget(message, argText, env) {
  // 1) 回复转发消息：从转发来源提取
  const replied = message.reply_to_message;
  if (replied) {
    // 新版 API: forward_origin.chat（频道/群转发）；旧版: forward_from_chat
    const origin = replied.forward_origin;
    if (origin?.chat?.id) return { id: origin.chat.id, title: origin.chat.title || null };
    if (origin?.sender_chat?.id) return { id: origin.sender_chat.id, title: origin.sender_chat.title || null };
    if (replied.forward_from_chat?.id) return { id: replied.forward_from_chat.id, title: replied.forward_from_chat.title || null };
  }

  // 2) 参数解析
  const arg = (argText || '').trim();
  if (!arg) return null;

  if (arg.startsWith('@')) {
    // @用户名：getChat 解析
    try {
      const res = await tgAPI('getChat', { chat_id: arg }, env);
      const data = await res.json();
      if (data?.ok && data?.result?.id) {
        return { id: data.result.id, title: data.result.title || data.result.username || null };
      }
    } catch (e) {
      console.warn('getChat 解析 @用户名 失败:', e?.message);
    }
    return null;
  }

  // 数字 ID（支持 -100xxx 形式）
  const numId = parseInt(arg, 10);
  if (Number.isInteger(numId) && numId !== 0) {
    // 尝试 getChat 获取标题（失败也不阻断，标题置空）
    let title = null;
    try {
      const res = await tgAPI('getChat', { chat_id: numId }, env);
      const data = await res.json();
      if (data?.ok && data?.result?.title) title = data.result.title;
    } catch (e) { /* bot 不在该群时 getChat 会失败，标题留空，由后续校验拦截 */ }
    return { id: numId, title };
  }

  return null;
}

// 🌟 V5.11: 校验 bot 自己是否在源群（绑定时提前发现，避免抽取时 copyMessage 失败）
// 返回 true=在源群；false=不在或查询失败
async function isBotInGroup(groupId, env) {
  const botId = await getBotUserId(env);
  if (!botId) return false;
  try {
    const res = await tgAPI('getChatMember', { chat_id: groupId, user_id: botId }, env);
    const data = await res.json();
    if (!data?.ok) return false;
    const status = data.result?.status;
    return ['creator', 'administrator', 'member', 'restricted'].includes(status);
  } catch (e) {
    return false;
  }
}

// 🌟 V5.13: /promote 核心执行 — forwardMessage 从B群转发到A群对应话题（保留第三方转发来源）→ 查重 → 收录
// bInfo: 被回复媒体 extractMediaInfo 结果；bChatId/bTopicId: B群上下文；cmdMsgId: 命令消息ID（用于回复）
async function doPromote(reply, bInfo, aChatId, aTopicId, category, bChatId, bTopicId, cmdMsgId, env) {
  // forwardMessage 保留原始转发来源（第三方频道），copyMessage 不保留
  const res = await tgAPI('forwardMessage', {
    chat_id: aChatId, message_thread_id: aTopicId,
    from_chat_id: bChatId, message_id: reply.message_id
  }, env);
  const data = await res.json();
  if (!data.ok) {
    const d = data.description || '';
    if (d.includes('chat not found') || d.includes('kicked') || d.includes('not enough rights')) {
      return tgAPI('sendMessage', { chat_id: bChatId, message_thread_id: bTopicId, reply_to_message_id: cmdMsgId, text: "🚨 bot 不在目标A群或无发消息权限,转发失败喵~" }, env);
    }
    if (d.includes('message to forward not found') || d.includes('not found')) {
      return tgAPI('sendMessage', { chat_id: bChatId, message_thread_id: bTopicId, reply_to_message_id: cmdMsgId, text: "🚨 被回复的消息已被删除,无法转发喵~" }, env);
    }
    console.error('promote forwardMessage err:', d);
    return tgAPI('sendMessage', { chat_id: bChatId, message_thread_id: bTopicId, reply_to_message_id: cmdMsgId, text: "🚨 转发失败喵~" + d }, env);
  }
  const aMsgId = data.result.message_id;

  // 查重：file_unique_id 转发不变，全局唯一
  const dup = await env.D1.prepare(`SELECT id, message_id FROM media_library WHERE file_unique_id = ? AND chat_id = ? LIMIT 1`).bind(bInfo.fileUniqueId, aChatId).first();
  if (dup) {
    // 已收录 → 尝试删除刚转发的重复消息保持A群整洁，并提示
    try { await tgAPI('deleteMessage', { chat_id: aChatId, message_id: aMsgId }, env); } catch (e) {}
    return tgAPI('sendMessage', { chat_id: bChatId, message_thread_id: bTopicId, reply_to_message_id: cmdMsgId, parse_mode: 'Markdown', text: `ℹ️ 该媒体已在A群收录过喵~\n${makeDeepLink(aChatId, dup.message_id)}` }, env);
  }

  // patch raw_message_json：复用被回复消息 payload,替换 message.chat.id / message.message_id 为A群新值
  let patchedRaw = bInfo.rawMessageJson;
  if (patchedRaw) {
    const parsed = safeJSONParse(patchedRaw);
    if (parsed && parsed.message) {
      if (parsed.message.chat) parsed.message.chat.id = aChatId;
      parsed.message.message_id = aMsgId;
      patchedRaw = safeJSONStringify(parsed);
    }
  }

  // INSERT 收录到A群
  await env.D1.prepare(
    `INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration, raw_message_json, sender_user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    aMsgId, aChatId, aTopicId, category,
    bInfo.fileUniqueId, bInfo.fileId, bInfo.type,
    reply.caption || '', bInfo.duration ?? null,
    patchedRaw, bInfo.senderUserId ?? null
  ).run();

  return tgAPI('sendMessage', { chat_id: bChatId, message_thread_id: bTopicId, reply_to_message_id: cmdMsgId, parse_mode: 'Markdown', text: `✅ 已提升到A群【${category}】喵！\n${makeDeepLink(aChatId, aMsgId)}`, reply_markup: { inline_keyboard: [[{ text: "📂 查看A群归档", url: makeDeepLink(aChatId, aMsgId) }]] } }, env);
}

async function handleExternalImport(dataBatch, env) {
  if (!dataBatch || !Array.isArray(dataBatch)) return;
  const stmts = dataBatch.map(item => {
    const normalizedRawMessageJson = normalizeStoredMediaPayload(item.raw_message_json ?? item.raw_message ?? item.raw_message_data ?? item.telegram_message ?? item.extra, 'external_import');
    return env.D1.prepare(`INSERT INTO media_library (message_id, chat_id, topic_id, category_name, file_unique_id, file_id, media_type, caption, duration, raw_message_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(item.message_id, item.chat_id || 0, item.topic_id || null, item.category_name, item.file_unique_id, item.file_id, item.media_type, item.caption || '', Number.isInteger(item.duration) ? item.duration : null, normalizedRawMessageJson);
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

  const parseDurationValue = (value) => {
    const str = String(value ?? '').trim();
    return /^(0|[1-9]\d*)$/.test(str) ? str : '';
  };
  out.duration_min = parseDurationValue(out.duration_min);
  out.duration_max = parseDurationValue(out.duration_max);

  // 校验 custom 模式：需要两个日期且起始不晚于结束
  if (out.date_mode === 'custom' && (!out.date_from || !out.date_to || out.date_from > out.date_to)) {
    out.date_mode = 'all'; out.date_from = ''; out.date_to = '';
  }
  // 校验 after 模式：需要 date_from
  if (out.date_mode === 'after' && !out.date_from) {
    out.date_mode = 'all'; out.date_from = '';
  }
  // 校验 before 模式：需要 date_to
  if (out.date_mode === 'before' && !out.date_to) {
    out.date_mode = 'all'; out.date_to = '';
  }

  if (out.duration_mode === 'gt' && out.duration_min === '') {
    out.duration_mode = 'all';
  }
  if (out.duration_mode === 'lt' && out.duration_max === '') {
    out.duration_mode = 'all';
  }
  if (out.duration_mode === 'range' && (out.duration_min === '' || out.duration_max === '' || Number(out.duration_min) >= Number(out.duration_max))) {
    out.duration_mode = 'all';
    out.duration_min = '';
    out.duration_max = '';
  }
  if (out.duration_mode === 'custom' && out.duration_max === '') {
    out.duration_mode = 'all';
  }

  // 🌟 V5.10: sender_user_id 校验
  const senderStr = String(out.sender_user_id ?? '').trim();
  if (senderStr === '' || senderStr === 'null') {
    out.sender_user_id = senderStr; // 有效值：空 或 'null'
  } else if (/^[1-9]\d*$/.test(senderStr)) {
    out.sender_user_id = senderStr; // 有效正整数
  } else {
    out.sender_user_id = ''; // 非法值降级
  }

  return out;
}

// 判断过滤器是否被激活（任意维度非默认即激活）
function isFilterActive(filters) {
  const f = normalizeFilters(filters);
  return f.media_type !== 'all' || f.date_mode !== 'all' || f.duration_mode !== 'all' || f.sender_user_id !== '';
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
    case 'after':
      clauses.push(`AND date(${alias}.added_at) >= date(?)`);
      binds.push(f.date_from); break;
    case 'before':
      clauses.push(`AND date(${alias}.added_at) <= date(?)`);
      binds.push(f.date_to); break;
    case 'custom':
      clauses.push(`AND date(${alias}.added_at) >= date(?) AND date(${alias}.added_at) <= date(?)`);
      binds.push(f.date_from, f.date_to); break;
  }

  // 时长筛选逻辑（支持 gt/lt/range/custom/预设）
  if (f.duration_mode === 'gt' && f.duration_min) {
    const minVal = parseInt(f.duration_min, 10);
    if (Number.isInteger(minVal) && minVal >= 0) {
      clauses.push(`AND ${alias}.duration IS NOT NULL AND ${alias}.duration > ?`);
      binds.push(minVal);
    }
  } else if (f.duration_mode === 'lt' && f.duration_max) {
    const maxVal = parseInt(f.duration_max, 10);
    if (Number.isInteger(maxVal) && maxVal >= 0) {
      clauses.push(`AND ${alias}.duration IS NOT NULL AND ${alias}.duration < ?`);
      binds.push(maxVal);
    }
  } else if (f.duration_mode === 'range' && f.duration_min && f.duration_max) {
    const minVal = parseInt(f.duration_min, 10);
    const maxVal = parseInt(f.duration_max, 10);
    if (Number.isInteger(minVal) && Number.isInteger(maxVal) && minVal >= 0 && maxVal >= 0 && minVal < maxVal) {
      clauses.push(`AND ${alias}.duration IS NOT NULL AND ${alias}.duration BETWEEN ? AND ?`);
      binds.push(minVal, maxVal);
    }
  } else {
    // custom 或预设模式（≤ 上限）
    let durationMax = null;
    if (f.duration_mode === 'custom' && f.duration_max) {
      durationMax = parseInt(f.duration_max, 10);
    } else if (Object.prototype.hasOwnProperty.call(FILTER_DURATION_PRESET_MAP, f.duration_mode)) {
      durationMax = FILTER_DURATION_PRESET_MAP[f.duration_mode];
    }
    if (Number.isInteger(durationMax) && durationMax >= 0) {
      clauses.push(`AND ${alias}.duration IS NOT NULL AND ${alias}.duration <= ?`);
      binds.push(durationMax);
    }
  }

  // 🌟 V5.10: 发送者筛选
  if (f.sender_user_id === 'null') {
    clauses.push(`AND ${alias}.sender_user_id IS NULL`);
  } else if (f.sender_user_id !== '') {
    clauses.push(`AND ${alias}.sender_user_id = ?`);
    binds.push(parseInt(f.sender_user_id, 10));
  }

  return { sql: clauses.length ? ` ${clauses.join(' ')}` : '', binds, normalized: f };
}

// 生成人类可读过滤状态文本（异步：需查询发送者名称）
async function renderFilterStatus(filters, env) {
  const f = normalizeFilters(filters);
  const mediaLabel = { all:'全部', photo:'仅图片', video:'仅视频', animation:'仅动图' }[f.media_type] || '全部';
  const dateLabel = f.date_mode === 'custom'
    ? `${f.date_from}~${f.date_to}`
    : f.date_mode === 'after'
    ? `≥${f.date_from}`
    : f.date_mode === 'before'
    ? `≤${f.date_to}`
    : ({ all:'不限', today:'今天', d7:'近7天', d30:'近30天', year:'今年' }[f.date_mode] || '不限');

  let durLabel = '不限';
  if (f.duration_mode === 'gt' && f.duration_min) {
    durLabel = `>${f.duration_min}s`;
  } else if (f.duration_mode === 'lt' && f.duration_max) {
    durLabel = `<${f.duration_max}s`;
  } else if (f.duration_mode === 'range' && f.duration_min && f.duration_max) {
    durLabel = `${f.duration_min}~${f.duration_max}s`;
  } else if (f.duration_mode === 'custom' && f.duration_max) {
    durLabel = `≤${f.duration_max}s`;
  } else if (Object.prototype.hasOwnProperty.call(FILTER_DURATION_PRESET_MAP, f.duration_mode)) {
    durLabel = { s30:'≤30s', s60:'≤60s', s120:'≤120s', s300:'≤5分钟' }[f.duration_mode];
  }

  // 🌟 V5.10: 发送者状态
  let senderLabel = '不限';
  if (f.sender_user_id === 'null') {
    senderLabel = '默认资源';
  } else if (f.sender_user_id !== '') {
    const senderId = parseInt(f.sender_user_id, 10);
    const row = await env.D1.prepare(`SELECT first_name FROM user_roster WHERE user_id = ?`).bind(senderId).first();
    senderLabel = row?.first_name ? row.first_name : `用户${senderId}`;
  }

  return `类型:${mediaLabel} | 时间:${dateLabel} | 时长:${durLabel} | 发送者:${senderLabel}`;
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
// 🌟 V5.11: 追加 media_votes / media_hide 清理（用子查询批量删，避免 per-id 语句数膨胀）
async function batchDeleteMediaByIds(ids, env) {
  let deleted = 0;
  for (let i = 0; i < ids.length; i += 20) {
    const chunk = ids.slice(i, i + 20);
    const placeholders = chunk.map(() => '?').join(', ');
    const stmts = chunk.flatMap(id => [
      env.D1.prepare(`DELETE FROM media_library WHERE id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM user_favorites WHERE media_id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM user_history WHERE media_id = ?`).bind(id),
      env.D1.prepare(`DELETE FROM group_history WHERE media_id = ?`).bind(id)
    ]);
    // 🌟 V5.11: 投票与隐藏标记按整批清理（子查询，2 条语句而非 N×2）
    stmts.push(env.D1.prepare(`DELETE FROM media_votes WHERE media_id IN (${placeholders})`).bind(...chunk));
    stmts.push(env.D1.prepare(`DELETE FROM media_hide WHERE media_id IN (${placeholders})`).bind(...chunk));
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
// 🌟 V5.11: sourceChatIds 改为数组支持多源抽取；displayChatId 用于按展示群隐藏排除
async function selectRandomMedia(category, sourceChatIds, useAntiRepeat, excludeId, filters, env, displayChatId) {
  const ids = (sourceChatIds && sourceChatIds.length) ? sourceChatIds : [0];
  const chatIn = buildChatInClause(ids);
  const antiClause  = useAntiRepeat ? `AND NOT EXISTS (SELECT 1 FROM served_history sh WHERE sh.media_id = m.id)` : '';
  const excludeClause = excludeId ? `AND m.id != ?` : '';
  const hideClause = displayChatId
    ? `AND NOT EXISTS (SELECT 1 FROM media_hide h WHERE h.media_id = m.id AND h.display_chat_id = ?)`
    : '';
  const { sql: filterSql, binds: filterBinds } = buildFilterWhereClause(filters, 'm');

  const binds = [category, ...chatIn.binds];
  if (displayChatId) binds.push(displayChatId);
  if (excludeId) binds.push(excludeId);
  binds.push(...filterBinds);

  const { results } = await env.D1.prepare(
    `SELECT m.id FROM media_library m WHERE m.category_name = ? AND m.chat_id ${chatIn.sql} ${hideClause} ${antiClause} ${excludeClause}${filterSql}`
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

// 🌟 V5.15: 查询当前媒体在同源群·同分类时间线上的附近记录
//   以 message_id 为序，取当前前 2 + 自身 + 后 2（边界不足则向另一侧补齐，最多 5 条）
//   返回按 message_id ASC 排序的行；找不到中心媒体时返回 []
async function queryNearbyMedia(mediaId, limit = 5, env) {
  const center = await env.D1.prepare(
    `SELECT id, chat_id, message_id, category_name, media_type, caption, view_count, duration
     FROM media_library WHERE id = ? LIMIT 1`
  ).bind(mediaId).first();
  if (!center) return { center: null, rows: [] };

  const half = Math.max(0, Math.floor((limit - 1) / 2)); // 默认 2
  const side = Math.max(half, limit - 1 - half);         // 另一侧目标数

  const [beforeRes, afterRes] = await Promise.all([
    env.D1.prepare(
      `SELECT id, chat_id, message_id, category_name, media_type, caption, view_count, duration
       FROM media_library
       WHERE chat_id = ? AND category_name = ? AND message_id < ?
       ORDER BY message_id DESC LIMIT ?`
    ).bind(center.chat_id, center.category_name, center.message_id, side).all(),
    env.D1.prepare(
      `SELECT id, chat_id, message_id, category_name, media_type, caption, view_count, duration
       FROM media_library
       WHERE chat_id = ? AND category_name = ? AND message_id > ?
       ORDER BY message_id ASC LIMIT ?`
    ).bind(center.chat_id, center.category_name, center.message_id, side).all()
  ]);

  let before = (beforeRes.results || []).slice().reverse(); // 转为 ASC
  let after = afterRes.results || [];

  // 边界补齐：某一侧不足时向另一侧多取，尽量凑满 limit
  const need = limit - 1 - before.length - after.length;
  if (need > 0) {
    if (before.length < side) {
      const moreAfter = await env.D1.prepare(
        `SELECT id, chat_id, message_id, category_name, media_type, caption, view_count, duration
         FROM media_library
         WHERE chat_id = ? AND category_name = ? AND message_id > ?
         ORDER BY message_id ASC LIMIT ?`
      ).bind(center.chat_id, center.category_name, center.message_id, after.length + need).all();
      after = moreAfter.results || after;
    } else if (after.length < side) {
      const moreBefore = await env.D1.prepare(
        `SELECT id, chat_id, message_id, category_name, media_type, caption, view_count, duration
         FROM media_library
         WHERE chat_id = ? AND category_name = ? AND message_id < ?
         ORDER BY message_id DESC LIMIT ?`
      ).bind(center.chat_id, center.category_name, center.message_id, before.length + need).all();
      before = (moreBefore.results || []).slice().reverse();
    }
  }

  // 截断到 limit（自身占 1 位）
  const room = limit - 1;
  while (before.length + after.length > room) {
    if (before.length > after.length) before.shift();
    else after.pop();
  }

  const rows = [...before, center, ...after];
  return { center, rows };
}

// 🌟 V5.15: 展示附近媒体列表（含原消息深链）
//   editMsgId 有值时原地刷新（附近列表「刷新」）；否则发新消息（从媒体键盘点开）
async function showNearbyMedia(userId, chatId, topicId, mediaId, cbId, env, editMsgId = null) {
  const { center, rows } = await queryNearbyMedia(mediaId, 5, env);
  if (!center) {
    return tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "呜呜，找不到这条媒体记录喵~", show_alert: true }, env);
  }

  await tgAPI('answerCallbackQuery', { callback_query_id: cbId, text: "附近媒体来啦喵~" }, env);

  const escapeHTML = (str) => String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const typeLabel = { photo: '🖼️', video: '🎬', animation: '🎠', document: '📄' };
  const formatDuration = (sec) => {
    if (sec == null || sec === '') return '';
    const n = Number(sec);
    if (!Number.isFinite(n) || n < 0) return '';
    const m = Math.floor(n / 60);
    const s = Math.floor(n % 60);
    return m > 0 ? `${m}分${s}秒` : `${s}秒`;
  };

  let text = `📍 <b>附近媒体</b>（同源群·同分类，按消息时间线）\n`;
  text += `🏷️ 分类: ${escapeHTML(center.category_name)}\n`;
  text += `📁 源群: <code>${center.chat_id}</code>\n\n`;

  if (rows.length === 0) {
    text += "附近没有其他收录记录喵~";
  } else {
    rows.forEach((row, idx) => {
      const isCenter = row.id === center.id;
      const icon = typeLabel[row.media_type] || '📎';
      const cap = escapeHTML((row.caption || '').substring(0, 18) || '无配文');
      const dur = formatDuration(row.duration);
      const durPart = dur ? ` · ⏱${dur}` : '';
      const mark = isCenter ? ' 👈 <b>当前</b>' : '';
      const link = makeDeepLink(row.chat_id, row.message_id);
      text += `${idx + 1}. ${icon} <a href="${link}">${cap}</a>${durPart} · 👀${row.view_count || 0}${mark}\n`;
    });
  }

  const markup = {
    inline_keyboard: [
      [{ text: "🔄 刷新附近", callback_data: `near|${mediaId}` }],
      [{ text: "🏠 呼出主菜单", callback_data: "main_menu_new" }]
    ]
  };

  if (editMsgId) {
    await tgAPI('editMessageText', {
      chat_id: chatId,
      message_id: editMsgId,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup: markup
    }, env);
  } else {
    await tgAPI('sendMessage', {
      chat_id: chatId,
      message_thread_id: topicId,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true,
      reply_markup: markup
    }, env);
  }
}

// 🌟 V5.14.1: 媒体死链探活降级与完整清理
// 设计动机：源群死亡/被踢/封禁 ≠ 媒体文件死亡。file_id 托管在 TG CDN,源群没了文件可能仍可用。
//   旧逻辑：copyMessage/forwardMessage 失败 → 直接 DELETE 整群/单条 → 误杀大量可用媒体
//   新逻辑：失败 → 降级用 file_id 直发 → 真发不出去才删,且必发通知

// 根据媒体类型用 file_id 直接发送(绕过源群,直接走 TG CDN)
// 返回 { ok, data, sentMessageId }
async function sendMediaByFileId(outChatId, outTopicId, media, keyboard, env) {
  const payload = {
    chat_id: outChatId,
    message_thread_id: outTopicId,
    reply_markup: keyboard ? { inline_keyboard: keyboard } : undefined
  };
  let method = null;
  const fid = media.file_id;
  const t = media.media_type;
  if (t === 'photo') { method = 'sendPhoto'; payload.photo = fid; }
  else if (t === 'video') { method = 'sendVideo'; payload.video = fid; }
  else if (t === 'animation') { method = 'sendAnimation'; payload.animation = fid; }
  else if (t === 'document') { method = 'sendDocument'; payload.document = fid; }
  else { return { ok: false, data: { description: 'unsupported media_type' }, sentMessageId: null }; }
  if (media.caption) payload.caption = media.caption;
  // 清理 undefined 键,避免 TG API 抱怨
  Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);
  const res = await tgAPI(method, payload, env);
  const data = await res.json();
  return { ok: !!data.ok, data, sentMessageId: data.ok ? data.result?.message_id : null };
}

// 判断错误描述是否属于"源群已死"(整群级失效)
function isSourceGroupDead(errDesc) {
  return errDesc.includes('chat not found') || errDesc.includes('bot was kicked') || errDesc.includes('channel not found');
}

// 完整清理单条媒体在所有关联表的痕迹(补全旧逻辑漏删的表)
async function purgeMediaById(mediaId, env) {
  await env.D1.batch([
    env.D1.prepare(`DELETE FROM media_library WHERE id = ?`).bind(mediaId),
    env.D1.prepare(`DELETE FROM served_history WHERE media_id = ?`).bind(mediaId),
    env.D1.prepare(`DELETE FROM user_favorites WHERE media_id = ?`).bind(mediaId),
    env.D1.prepare(`DELETE FROM user_history WHERE media_id = ?`).bind(mediaId),
    env.D1.prepare(`DELETE FROM group_history WHERE media_id = ?`).bind(mediaId),
    env.D1.prepare(`DELETE FROM media_votes WHERE media_id = ?`).bind(mediaId),
    env.D1.prepare(`DELETE FROM media_hide WHERE media_id = ?`).bind(mediaId)
  ]);
}

// 完整清理整个 chat_id 在所有关联表的痕迹(源群死亡时整群清理,两步法避免子查询依赖已删数据)
async function purgeChatFully(chatId, env) {
  // 1. 先查出该群所有 media id
  const { results } = await env.D1.prepare(`SELECT id FROM media_library WHERE chat_id = ?`).bind(chatId).all();
  const ids = (results || []).map(r => r.id);
  // 2. 删 media_library + config_topics + group_sources
  await env.D1.batch([
    env.D1.prepare(`DELETE FROM media_library WHERE chat_id = ?`).bind(chatId),
    env.D1.prepare(`DELETE FROM config_topics WHERE chat_id = ?`).bind(chatId),
    env.D1.prepare(`DELETE FROM group_sources WHERE display_chat_id = ?`).bind(chatId),
    env.D1.prepare(`DELETE FROM group_sources WHERE source_chat_id = ?`).bind(chatId),
    env.D1.prepare(`DELETE FROM user_source_selection WHERE display_chat_id = ?`).bind(chatId)
  ]);
  // 3. 按 id 批量清关联表
  if (ids.length > 0) {
    for (let i = 0; i < ids.length; i += 20) {
      const chunk = ids.slice(i, i + 20);
      const placeholders = chunk.map(() => '?').join(', ');
      await env.D1.batch([
        env.D1.prepare(`DELETE FROM served_history WHERE media_id IN (${placeholders})`).bind(...chunk),
        env.D1.prepare(`DELETE FROM user_favorites WHERE media_id IN (${placeholders})`).bind(...chunk),
        env.D1.prepare(`DELETE FROM user_history WHERE media_id IN (${placeholders})`).bind(...chunk),
        env.D1.prepare(`DELETE FROM group_history WHERE media_id IN (${placeholders})`).bind(...chunk),
        env.D1.prepare(`DELETE FROM media_votes WHERE media_id IN (${placeholders})`).bind(...chunk),
        env.D1.prepare(`DELETE FROM media_hide WHERE media_id IN (${placeholders})`).bind(...chunk)
      ]);
    }
  }
  return ids.length;
}

// 🌟 V5.14.1: 主动扫描无效媒体（分批探活,收集死链清单,询问管理员确认后再清理）
//   探活策略:用 sendMediaByFileId 发到输出话题 → 成功则立即 deleteMessage(活的,不打扰)
//   死链:收集到清单,不自动删,扫描完成后发清单消息+「全部清理」/「取消」按钮
async function scanDeadMedia(chatId, outChatId, outTopicId, env) {
  const PAGE = 50;
  let cursor = 0;
  let scanned = 0;
  const deadList = []; // 收集死链,不立即删
  const startTime = Date.now();

  while (true) {
    const { results } = await env.D1.prepare(
      `SELECT id, chat_id, message_id, file_id, media_type, category_name, caption FROM media_library WHERE chat_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`
    ).bind(chatId, PAGE, cursor).all();
    if (!results || results.length === 0) break;

    for (const media of results) {
      scanned++;
      let isAlive = false;
      if (media.file_id && media.media_type) {
        const fb = await sendMediaByFileId(outChatId, outTopicId, media, null, env);
        if (fb.ok) {
          isAlive = true;
          // 活的 → 立即删除探活消息,不打扰用户
          if (fb.sentMessageId) {
            try { await tgAPI('deleteMessage', { chat_id: outChatId, message_id: fb.sentMessageId }, env); } catch (e) {}
          }
        }
      }
      if (!isAlive) {
        deadList.push({ id: media.id, chat_id: media.chat_id, category: media.category_name || '未知', type: media.media_type || '?' });
      }
    }
    cursor += PAGE;
    // 安全上限:单次扫描不超过 2000 条,避免 Worker CPU 时间超限
    if (scanned >= 2000) break;
  }

  const elapsed = Math.round((Date.now() - startTime) / 1000);

  if (deadList.length === 0) {
    await tgAPI('sendMessage', {
      chat_id: outChatId, message_thread_id: outTopicId,
      text: `✅ 无效媒体检测完成喵~\n\n📊 扫描 ${scanned} 条\n🎉 全部有效,无死链\n⏱️ 耗时 ${elapsed} 秒`
    }, env);
    return;
  }

  // 有死链 — 按源群分组统计,存入 session,发清单让管理员确认
  const grpMap = new Map();
  for (const d of deadList) {
    if (!grpMap.has(d.chat_id)) grpMap.set(d.chat_id, []);
    grpMap.get(d.chat_id).push(d);
  }

  // 存 session(collected_ids 存死链 id 数组)
  // 先清旧 dead_scan 会话避免 UNIQUE(chat_id, user_id) 冲突(user_id=0 常量)
  await env.D1.prepare(`DELETE FROM batch_sessions WHERE chat_id = ? AND user_id = 0 AND mode = 'dead_scan'`).bind(chatId).run();
  const sessionRes = await env.D1.prepare(
    `INSERT INTO batch_sessions (chat_id, user_id, mode, collected_ids) VALUES (?, 0, 'dead_scan', ?)`
  ).bind(chatId, JSON.stringify(deadList.map(d => d.id))).run();
  const sessionId = sessionRes.meta?.last_row_id;

  // 构建清单文本(限制前 20 条,超出提示总数)
  const SHOW = 20;
  let listText = `⚠️ <b>检测完成,发现 ${deadList.length} 条死链</b>\n\n📊 扫描 ${scanned} 条 | ⏱️ ${elapsed}秒\n📦 涉及 ${grpMap.size} 个源群\n\n<b>📋 死链清单</b>\n`;
  const showItems = deadList.slice(0, SHOW);
  for (const d of showItems) {
    listText += `• #<code>${d.id}</code> [${d.type}] <code>${d.category}</code> 源群:<code>${d.chat_id}</code>\n`;
  }
  if (deadList.length > SHOW) {
    listText += `... 还有 <b>${deadList.length - SHOW}</b> 条 (完整清单已存后台)\n`;
  }
  listText += `\n各源群死链数统计:\n`;
  for (const [grpId, items] of grpMap) {
    listText += `• 源群 <code>${grpId}</code>: ${items.length} 条\n`;
  }
  listText += `\n⚠️ 确认清理将永久删除这些媒体的所有关联数据(收藏/历史/投票等),且不可恢复喵！`;

  await tgAPI('sendMessage', {
    chat_id: outChatId, message_thread_id: outTopicId, text: listText, parse_mode: 'HTML',
    reply_markup: { inline_keyboard: [
      [{ text: `✅ 确认清理全部 (${deadList.length}条)`, callback_data: `dead_scan_all|${sessionId}` }],
      [{ text: "❌ 取消,保留死链", callback_data: `dead_scan_cancel|${sessionId}` }]
    ] }
  }, env);
}

// 🌟 V5.14: 从 raw_message_json 解析第三方转发来源深链
// 用于 C 分发群(B的展示群)用户不在 B 源群时,绕过 B 直接跳第三方 X 原消息
// forward_origin 深链可行性(基于 Telegram Bot API 官方文档):
//   type=channel  : chat{id,username} + message_id  → 完整深链 ✅
//   type=chat     : sender_chat{id,username},无 message_id → 仅跳群 ⚠️
//   type=user     : sender_user,无 message_id → 不显示 ❌
//   type=hidden_user : sender_user_name,无 ID → 不显示 ❌
//   旧版: forward_from_chat + forward_from_message_id 等价 channel
// 返回 { url, kind: 'channel'|'chat' } 或 null(无可定位来源)
function extractForwardSourceDeepLink(rawMessageJson) {
  const parsed = safeJSONParse(rawMessageJson);
  if (!parsed || !parsed.message) return null;
  const msg = parsed.message;

  const origin = msg.forward_origin;          // 新版 API 7.x+
  const legacyChat = msg.forward_from_chat;   // 旧版 API

  let chatId = null, username = null, messageId = null, kind = null;

  if (origin) {
    if (origin.type === 'channel' && origin.chat) {
      chatId = origin.chat.id; username = origin.chat.username; messageId = origin.message_id; kind = 'channel';
    } else if (origin.type === 'chat' && origin.sender_chat) {
      chatId = origin.sender_chat.id; username = origin.sender_chat.username; kind = 'chat'; // 无 message_id
    }
    // type=user / hidden_user → 保持 null
  } else if (legacyChat && legacyChat.id) {
    chatId = legacyChat.id; username = legacyChat.username; messageId = msg.forward_from_message_id; kind = 'channel';
  }

  if (!chatId && !username) return null;

  // 公开 username 优先(任何人可跳)
  if (username) {
    return { url: messageId ? `https://t.me/${username}/${messageId}` : `https://t.me/${username}`, kind };
  }
  // 私聊群(无 username): t.me/c/{id去-100}/{msg_id},需在该群才能打开
  const stripped = String(chatId).replace('-100', '');
  return { url: messageId ? `https://t.me/c/${stripped}/${messageId}` : `https://t.me/c/${stripped}`, kind };
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