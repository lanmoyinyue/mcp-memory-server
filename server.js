import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import express from 'express';
import { DatabaseSync as Database } from 'node:sqlite';
import { v4 as uuidv4 } from 'uuid';
import { z } from 'zod';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── Database ──────────────────────────────────────────────────────────────────

const dataDir = process.env.DATA_DIR || path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

const db = new Database(path.join(dataDir, 'memories.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS memories (
    id          TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    category    TEXT NOT NULL,
    tags        TEXT NOT NULL DEFAULT '[]',
    source      TEXT NOT NULL DEFAULT '',
    mood        TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    expires_at  TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_cat     ON memories(category);
  CREATE INDEX IF NOT EXISTS idx_created ON memories(created_at);
`);

// ── Helpers ───────────────────────────────────────────────────────────────────

const parseTags = (t) => { try { return JSON.parse(t); } catch { return []; } };

const fmt = (r) => ({
  id: r.id, content: r.content, category: r.category,
  tags: parseTags(r.tags), source: r.source, mood: r.mood || null,
  created_at: r.created_at, updated_at: r.updated_at, expires_at: r.expires_at || null,
});

const cleanExpired = () =>
  db.prepare('DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at < ?')
    .run(new Date().toISOString());

// ── MCP factory — one McpServer instance per SSE connection ───────────────────

function createMcpServer() {
  const mcp = new McpServer({ name: 'memory-server', version: '1.0.0' });

  // 1. write_memory
  mcp.tool(
    'write_memory',
    'Save a new memory. category: deep=永久, daily=3天过期, diary=日记, writing=写作进度',
    {
      content:  z.string().min(1).describe('Memory content'),
      category: z.enum(['deep', 'daily', 'diary', 'writing']).describe('Memory layer'),
      tags:     z.array(z.string()).optional().describe('Tags'),
      source:   z.string().optional().describe('Source context'),
      mood:     z.string().optional().describe('Mood for diary (happy/sad/calm/excited/anxious)'),
    },
    async ({ content, category, tags, source, mood }) => {
      const id = uuidv4(), now = new Date().toISOString();
      let expires_at = null;
      if (category === 'daily') {
        const exp = new Date(); exp.setDate(exp.getDate() + 3); expires_at = exp.toISOString();
      }
      db.prepare(
        'INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?)'
      ).run(id, content, category, JSON.stringify(tags ?? []), source ?? '', mood ?? null, now, now, expires_at);
      return { content: [{ type: 'text', text: `Memory saved [${category}] ID: ${id}` }] };
    }
  );

  // 2. read_memories
  mcp.tool(
    'read_memories',
    'Read memories with optional filters by category, tags, or keyword',
    {
      category:        z.enum(['deep', 'daily', 'diary', 'writing']).optional(),
      tags:            z.array(z.string()).optional().describe('Match any of these tags'),
      keyword:         z.string().optional().describe('Keyword in content'),
      limit:           z.number().int().min(1).max(100).optional().describe('Max results (default 20)'),
      include_expired: z.boolean().optional().describe('Include expired daily memories'),
    },
    async ({ category, tags, keyword, limit = 20, include_expired = false }) => {
      if (!include_expired) cleanExpired();
      let sql = 'SELECT * FROM memories WHERE 1=1';
      const p = [];
      if (!include_expired) { sql += ' AND (expires_at IS NULL OR expires_at > ?)'; p.push(new Date().toISOString()); }
      if (category) { sql += ' AND category = ?'; p.push(category); }
      if (keyword)  { sql += ' AND content LIKE ?'; p.push(`%${keyword}%`); }
      sql += ' ORDER BY created_at DESC LIMIT ?';
      p.push(limit);
      let rows = db.prepare(sql).all(...p).map(fmt);
      if (tags?.length) rows = rows.filter(r => tags.some(t => r.tags.includes(t)));
      return { content: [{ type: 'text', text: rows.length ? JSON.stringify(rows, null, 2) : 'No memories found.' }] };
    }
  );

  // 3. search_memories
  mcp.tool(
    'search_memories',
    'Full-text search across all memories',
    {
      query:    z.string().min(1).describe('Search term'),
      category: z.enum(['deep', 'daily', 'diary', 'writing']).optional(),
    },
    async ({ query, category }) => {
      cleanExpired();
      let sql = 'SELECT * FROM memories WHERE content LIKE ? AND (expires_at IS NULL OR expires_at > ?)';
      const p = [`%${query}%`, new Date().toISOString()];
      if (category) { sql += ' AND category = ?'; p.push(category); }
      sql += ' ORDER BY created_at DESC LIMIT 20';
      const rows = db.prepare(sql).all(...p).map(fmt);
      return {
        content: [{
          type: 'text',
          text: rows.length ? `Found ${rows.length} result(s):\n${JSON.stringify(rows, null, 2)}` : `No results for: "${query}"`,
        }],
      };
    }
  );

  // 4. delete_memory
  mcp.tool(
    'delete_memory',
    'Delete a memory by ID',
    { id: z.string().describe('Memory ID') },
    async ({ id }) => {
      const r = db.prepare('DELETE FROM memories WHERE id = ?').run(id);
      return { content: [{ type: 'text', text: r.changes ? `Deleted ${id}.` : `Not found: ${id}` }] };
    }
  );

  // 5. update_memory
  mcp.tool(
    'update_memory',
    'Update content, tags, source, or mood of an existing memory',
    {
      id:      z.string().describe('Memory ID'),
      content: z.string().optional(),
      tags:    z.array(z.string()).optional(),
      source:  z.string().optional(),
      mood:    z.string().optional(),
    },
    async ({ id, content, tags, source, mood }) => {
      const row = db.prepare('SELECT * FROM memories WHERE id = ?').get(id);
      if (!row) return { content: [{ type: 'text', text: `Not found: ${id}` }] };
      db.prepare('UPDATE memories SET content=?,tags=?,source=?,mood=?,updated_at=? WHERE id=?').run(
        content ?? row.content,
        tags !== undefined ? JSON.stringify(tags) : row.tags,
        source ?? row.source,
        mood !== undefined ? mood : row.mood,
        new Date().toISOString(),
        id
      );
      return { content: [{ type: 'text', text: `Updated ${id}.` }] };
    }
  );

  // 6. get_stats
  mcp.tool(
    'get_stats',
    'Return statistics about the memory store',
    {},
    async () => {
      cleanExpired();
      const total = db.prepare('SELECT COUNT(*) as c FROM memories').get().c;
      const byCat = db.prepare('SELECT category, COUNT(*) as c FROM memories GROUP BY category').all();
      const recent = db.prepare('SELECT * FROM memories ORDER BY created_at DESC LIMIT 5').all().map(fmt);
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            total_memories: total,
            by_category: Object.fromEntries(byCat.map(r => [r.category, r.c])),
            recent_memories: recent,
          }, null, 2),
        }],
      };
    }
  );

  return mcp;
}

// ── Express ───────────────────────────────────────────────────────────────────

const app = express();

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const AUTH_TOKEN = process.env.AUTH_TOKEN;
const auth = (req, res, next) => {
  if (!AUTH_TOKEN) return next();
  const token = req.headers.authorization?.replace('Bearer ', '').trim();
  if (token !== AUTH_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
  next();
};

// ── MCP SSE — fresh McpServer per connection ──────────────────────────────────

const sessions = new Map(); // sessionId → SSEServerTransport

app.get('/sse', auth, async (req, res) => {
  // Disable nginx/caddy proxy buffering — required for SSE on Zeabur
  res.setHeader('X-Accel-Buffering', 'no');

  const transport = new SSEServerTransport('/messages', res);
  sessions.set(transport.sessionId, transport);
  res.on('close', () => sessions.delete(transport.sessionId));

  try {
    await createMcpServer().connect(transport);
  } catch (err) {
    console.error('[SSE] connect error:', err);
    sessions.delete(transport.sessionId);
    if (!res.headersSent) res.status(500).end();
  }
});

app.post('/messages', auth, async (req, res) => {
  const transport = sessions.get(req.query.sessionId);
  if (!transport) return res.status(404).json({ error: 'Session not found' });
  try {
    await transport.handlePostMessage(req, res);
  } catch (err) {
    console.error('[messages] error:', err);
    if (!res.headersSent) res.status(500).json({ error: 'Internal error' });
  }
});

// ── REST API ──────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/memories', (req, res) => {
  cleanExpired();
  const { category, keyword, limit = 50 } = req.query;
  let sql = 'SELECT * FROM memories WHERE (expires_at IS NULL OR expires_at > ?)';
  const p = [new Date().toISOString()];
  if (category && category !== 'all') { sql += ' AND category = ?'; p.push(category); }
  if (keyword) { sql += ' AND content LIKE ?'; p.push(`%${keyword}%`); }
  sql += ' ORDER BY created_at DESC LIMIT ?';
  p.push(parseInt(limit));
  res.json(db.prepare(sql).all(...p).map(fmt));
});

app.post('/api/memories', (req, res) => {
  const { content, category, tags = [], source = '', mood = null } = req.body;
  if (!content || !category) return res.status(400).json({ error: 'content and category required' });
  const id = uuidv4(), now = new Date().toISOString();
  let expires_at = null;
  if (category === 'daily') {
    const exp = new Date(); exp.setDate(exp.getDate() + 3); expires_at = exp.toISOString();
  }
  db.prepare('INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?)')
    .run(id, content, category, JSON.stringify(tags), source, mood, now, now, expires_at);
  res.json({ id, message: 'Memory saved' });
});

app.put('/api/memories/:id', (req, res) => {
  const row = db.prepare('SELECT * FROM memories WHERE id = ?').get(req.params.id);
  if (!row) return res.status(404).json({ error: 'Not found' });
  const { content, tags, source, mood } = req.body;
  db.prepare('UPDATE memories SET content=?,tags=?,source=?,mood=?,updated_at=? WHERE id=?').run(
    content ?? row.content,
    tags !== undefined ? JSON.stringify(tags) : row.tags,
    source ?? row.source,
    mood !== undefined ? mood : row.mood,
    new Date().toISOString(),
    req.params.id
  );
  res.json({ message: 'Updated' });
});

app.delete('/api/memories/:id', (req, res) => {
  const r = db.prepare('DELETE FROM memories WHERE id = ?').run(req.params.id);
  res.json({ deleted: r.changes > 0 });
});

app.get('/api/stats', (_req, res) => {
  cleanExpired();
  const total = db.prepare('SELECT COUNT(*) as c FROM memories').get().c;
  const byCat = db.prepare('SELECT category, COUNT(*) as c FROM memories GROUP BY category').all();
  res.json({ total, by_category: Object.fromEntries(byCat.map(r => [r.category, r.c])) });
});

app.get('/api/diary-calendar', (req, res) => {
  const { year, month } = req.query;
  const prefix = `${year}-${String(month).padStart(2, '0')}`;
  const rows = db.prepare(`
    SELECT id, mood, substr(content,1,120) as preview, substr(created_at,1,10) as date
    FROM memories WHERE category='diary' AND created_at LIKE ?
    ORDER BY created_at DESC
  `).all(`${prefix}%`);
  res.json(rows);
});

// ── Global error guards ───────────────────────────────────────────────────────

process.on('uncaughtException',  (err) => console.error('[uncaughtException]', err));
process.on('unhandledRejection', (err) => console.error('[unhandledRejection]', err));

// ── Start ─────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Memory server on port ${PORT}`);
  console.log(`MCP SSE:   http://localhost:${PORT}/sse`);
  console.log(`Frontend:  http://localhost:${PORT}`);
  if (AUTH_TOKEN) console.log('Auth: Bearer token enabled');
});
