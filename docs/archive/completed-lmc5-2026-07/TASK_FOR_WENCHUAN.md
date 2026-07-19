# server.js 改动任务——给闻川

> 文件：`mcp-memory-server-live/server.js`（当前 1096 行）
> 仓库：lanmoyinyue/mcp-memory-server（私密）
> 已同步三端：本地 D:\MOON520KE\mcp-memory-server-live / VPS /root/mcp-memory-server / GitHub
>
> **红线**：
> - `diary / deep / anchor / 私藏 / 心动 / cc-diary` 这些 category 的记忆，自动化机制（supersede/decay/merge）一律不碰
> - 改完先在本地测，确认没问题再 push，push 前 VPS 的克会话要下线（重部署会踢崩）
> - 不要改 check_facts / merge_memories / patrol 的现有逻辑，只加新东西

---

## 一、数据库迁移（加在现有 migration 块后面，约第 63 行附近）

```js
// Migrate: raw_events evidence journal (LMC-5 E-axis)
db.exec(`
  CREATE TABLE IF NOT EXISTS raw_events (
    id          TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL DEFAULT '',
    channel     TEXT NOT NULL DEFAULT 'cc',
    role        TEXT NOT NULL,
    content     TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    linked_memory_ids TEXT NOT NULL DEFAULT '[]'
  );
  CREATE INDEX IF NOT EXISTS idx_re_channel   ON raw_events(channel);
  CREATE INDEX IF NOT EXISTS idx_re_timestamp ON raw_events(timestamp);
  CREATE INDEX IF NOT EXISTS idx_re_session   ON raw_events(session_id);
`);

// Migrate: protected field — automation cannot touch these memories
try { db.exec('ALTER TABLE memories ADD COLUMN protected INTEGER NOT NULL DEFAULT 0'); } catch {}
// Migrate: evidence links — curated memory → raw_event ids
try { db.exec('ALTER TABLE memories ADD COLUMN evidence_raw_ids TEXT NOT NULL DEFAULT \'[]\''); } catch {}
```

---

## 二、新增常量：自动保护的 category 列表（加在 EXPIRING 常量旁边）

```js
// Categories that automation (decay/supersede/merge) must never touch
const PROTECTED_CATEGORIES = new Set(['diary', 'deep', 'anchor', '私藏', '心动', 'cc-diary']);
```

---

## 三、修改 `write_memory` 工具

**参数新增**（在现有 zod schema 里加）：
```js
protected:         z.boolean().optional().describe('锁住这条记忆，自动化不能碰'),
evidence_raw_ids:  z.array(z.string()).optional().describe('关联的 raw_event id 列表'),
```

**写入逻辑新增**（在 INSERT 前）：
```js
// 按 category 自动设 protected
const isProtected = args.protected ?? PROTECTED_CATEGORIES.has(args.category) ? 1 : 0;
const evidenceIds = JSON.stringify(args.evidence_raw_ids ?? []);
```

**INSERT 语句要包含新字段**：`protected`, `evidence_raw_ids`

**同时修改 fmt() helper**，让它也返回这两个字段：
```js
protected:          !!r.protected,
evidence_raw_ids:   JSON.parse(r.evidence_raw_ids || '[]'),
```

---

## 四、修改 `decay_activation` 工具

在执行 UPDATE 前加一行过滤，跳过 protected 记忆：

```js
// 原来大概是：UPDATE memories SET activation_score = ... WHERE ...
// 改成：
WHERE protected = 0 AND activation_score > 0 AND ...
```

---

## 五、修改 `merge_memories` 工具

合并前检查：如果任意一条是 protected，拒绝合并，返回错误：

```js
const hasProtected = memoriesToMerge.some(m => m.protected);
if (hasProtected) return { error: 'protected 记忆不参与 merge' };
```

---

## 六、新增 MCP 工具：`log_raw_event`

```js
server.tool('log_raw_event',
  '记录一条原始对话事件到 raw_events 证据日志',
  {
    session_id: z.string().optional().describe('会话 id，同一次对话用同一个'),
    channel:    z.enum(['cc', 'daily', 'intimate']).describe('对话渠道'),
    role:       z.enum(['user', 'assistant']).describe('说话角色'),
    content:    z.string().describe('原文内容'),
    linked_memory_ids: z.array(z.string()).optional().describe('关联的 curated memory id'),
  },
  async (args) => {
    const id = uuidv4();
    const now = new Date().toISOString();
    db.prepare(`
      INSERT INTO raw_events (id, session_id, channel, role, content, timestamp, linked_memory_ids)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      args.session_id || '',
      args.channel,
      args.role,
      args.content,
      now,
      JSON.stringify(args.linked_memory_ids || [])
    );
    return { id, timestamp: now };
  }
);
```

---

## 七、新增 MCP 工具：`search_raw_events`

```js
server.tool('search_raw_events',
  '在 raw_events 里关键词搜索，返回原始对话片段',
  {
    query:    z.string().describe('搜索词'),
    channel:  z.enum(['cc', 'daily', 'intimate', 'all']).default('all'),
    limit:    z.number().int().min(1).max(50).default(20),
  },
  async (args) => {
    const like = `%${args.query}%`;
    const rows = args.channel === 'all'
      ? db.prepare(`SELECT * FROM raw_events WHERE content LIKE ? ORDER BY timestamp DESC LIMIT ?`).all(like, args.limit)
      : db.prepare(`SELECT * FROM raw_events WHERE content LIKE ? AND channel = ? ORDER BY timestamp DESC LIMIT ?`).all(like, args.channel, args.limit);
    return { results: rows.map(r => ({ ...r, linked_memory_ids: JSON.parse(r.linked_memory_ids) })) };
  }
);
```

---

## 八、新增 MCP 工具：`get_evidence`

```js
server.tool('get_evidence',
  '根据 curated memory id 找到它关联的所有原始 raw_events',
  {
    memory_id: z.string().describe('curated memory 的 id'),
  },
  async (args) => {
    const mem = db.prepare('SELECT evidence_raw_ids FROM memories WHERE id = ?').get(args.memory_id);
    if (!mem) return { error: 'memory not found' };
    const ids = JSON.parse(mem.evidence_raw_ids || '[]');
    if (!ids.length) return { evidence: [] };
    const placeholders = ids.map(() => '?').join(',');
    const rows = db.prepare(`SELECT * FROM raw_events WHERE id IN (${placeholders})`).all(...ids);
    return { evidence: rows };
  }
);
```

---

## 九、修改 GitHub 备份逻辑

现有备份代码大约在 950-1000 行附近，它把 memories 表导出成 JSON 推到 GitHub。

在备份函数里，额外导出 raw_events 表，**但排除 intimate channel**（隐私保护）：

```js
// 备份 raw_events（跳过 intimate 渠道）
const rawEventsBackup = db.prepare(
  "SELECT * FROM raw_events WHERE channel != 'intimate' ORDER BY timestamp DESC"
).all();
// 写入同一备份文件或单独文件 raw_events_backup.json
```

---

## 十、Claude Code Hooks（本地 .claude/settings.json）

这是 CC 端（克这边）配的，不在 server.js 里，闻川不用动，我（克）自己配。

已有的 hooks 基础上新加两条：
1. `UserPromptSubmit` — 把月亮的消息 log 进 `raw_events`，channel 按对话类型判断
2. `SessionEnd` — 把本次 session 的最后状态 log 一条

---

## 改完之后

1. 本地跑 `node server.js` 验证无报错
2. `git add server.js && git commit -m "feat: raw_events evidence layer + protected field"`
3. `git push`
4. Zeabur 重部署前通知月亮，等她确认 VPS 克已下线
