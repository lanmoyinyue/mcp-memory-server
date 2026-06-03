import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
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

// Migrate: add embedding column for existing databases
try { db.exec('ALTER TABLE memories ADD COLUMN embedding TEXT'); } catch {}

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

// ── Embedding & vector similarity ─────────────────────────────────────────────
// Set VOYAGE_API_KEY env var to enable. Free at voyageai.com (200M tokens/month)

async function getEmbedding(text) {
  const apiKey = process.env.VOYAGE_API_KEY;
  if (!apiKey) return null;
  try {
    const res = await fetch('https://api.voyageai.com/v1/embeddings', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: [text], model: 'voyage-3-lite' }),
    });
    if (!res.ok) { console.error('[embed] API error', res.status, await res.text()); return null; }
    const data = await res.json();
    return data.data[0].embedding; // float[]
  } catch (err) { console.error('[embed] error:', err.message); return null; }
}

function cosineSim(a, b) {
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { dot += a[i]*b[i]; na += a[i]*a[i]; nb += b[i]*b[i]; }
  const d = Math.sqrt(na) * Math.sqrt(nb);
  return d === 0 ? 0 : dot / d;
}

// Returns top-k semantically similar memories, excluding the given id
function findRelated(embedding, excludeId, topK = 3) {
  if (!embedding) return [];
  const rows = db.prepare(
    'SELECT id,content,category,tags,source,mood,created_at,embedding FROM memories WHERE id != ? AND embedding IS NOT NULL AND (expires_at IS NULL OR expires_at > ?)'
  ).all(excludeId, new Date().toISOString());
  return rows
    .map(r => { try { return { ...r, score: cosineSim(embedding, JSON.parse(r.embedding)) }; } catch { return null; } })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score)
    .slice(0, topK)
    .map(r => ({
      id: r.id, content: r.content, category: r.category,
      tags: parseTags(r.tags), source: r.source, created_at: r.created_at,
      similarity: +r.score.toFixed(3),
    }));
}

// ── MCP factory — one McpServer instance per SSE connection ───────────────────

function createMcpServer() {
  const mcp = new McpServer({ name: 'memory-server', version: '1.0.0' });

  // 1. write_memory
  mcp.tool(
    'write_memory',
    'Save a new memory. category: deep=永久, daily=3天过期, diary=日记, writing=写作进度, anchor=情绪锚(永久), 也可自定义分类名',
    {
      content:  z.string().min(1).describe('Memory content'),
      category: z.string().describe('Memory layer: deep/daily/diary/writing/anchor or any custom category'),
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
      // deep/anchor and custom categories never expire
      // Compute embedding and find related BEFORE inserting (so new memory is excluded)
      const embedding = await getEmbedding(content);
      const related = findRelated(embedding, id);

      db.prepare(
        'INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at,embedding) VALUES (?,?,?,?,?,?,?,?,?,?)'
      ).run(id, content, category, JSON.stringify(tags ?? []), source ?? '', mood ?? null, now, now, expires_at,
        embedding ? JSON.stringify(embedding) : null);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            saved: { id, category },
            related_memories: related,
            note: embedding ? `Found ${related.length} related memories.` : 'Set VOYAGE_API_KEY to enable semantic associations.',
          }, null, 2),
        }],
      };
    }
  );

  // 2. read_memories
  mcp.tool(
    'read_memories',
    'Read memories with optional filters by category, tags, or keyword',
    {
      category:        z.string().optional().describe('Filter by category (deep/daily/diary/writing/anchor or custom)'),
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
      category: z.string().optional().describe('Filter by category'),
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

  // 7. find_related — semantic vector search
  mcp.tool(
    'find_related',
    'Find semantically similar memories by meaning using vector embeddings (requires VOYAGE_API_KEY)',
    {
      query:    z.string().min(1).describe('Concept or text to search by meaning'),
      top_k:    z.number().int().min(1).max(10).optional().describe('Number of results (default 5)'),
      category: z.string().optional().describe('Limit to category'),
    },
    async ({ query, top_k = 5, category }) => {
      const embedding = await getEmbedding(query);
      if (!embedding) {
        return { content: [{ type: 'text', text: 'Vector search unavailable: set VOYAGE_API_KEY env var. Get a free key at voyageai.com.' }] };
      }
      let sql = 'SELECT * FROM memories WHERE embedding IS NOT NULL AND (expires_at IS NULL OR expires_at > ?)';
      const p = [new Date().toISOString()];
      if (category) { sql += ' AND category = ?'; p.push(category); }
      const rows = db.prepare(sql).all(...p);
      const results = rows
        .map(r => { try { return { ...r, score: cosineSim(embedding, JSON.parse(r.embedding)) }; } catch { return null; } })
        .filter(Boolean)
        .sort((a, b) => b.score - a.score)
        .slice(0, top_k)
        .map(r => ({ ...fmt(r), similarity: +r.score.toFixed(3) }));
      return {
        content: [{
          type: 'text',
          text: results.length ? JSON.stringify(results, null, 2) : `No semantic matches found for: "${query}"`,
        }],
      };
    }
  );

  // 8. hybrid_search — keyword + semantic, fused with RRF (best default search)
  mcp.tool(
    'hybrid_search',
    'Hybrid search: runs keyword (full-text) and semantic (vector) search together, fuses and re-ranks them with Reciprocal Rank Fusion. Best default search. Falls back to keyword-only if VOYAGE_API_KEY is unset.',
    {
      query:    z.string().min(1).describe('Search term or concept'),
      category: z.string().optional().describe('Filter by category'),
      limit:    z.number().int().min(1).max(20).optional().describe('Number of results (default 10)'),
    },
    async ({ query, category, limit = 10 }) => {
      cleanExpired();
      const now = new Date().toISOString();

      // ── keyword half ──
      let kSql = 'SELECT * FROM memories WHERE content LIKE ? AND (expires_at IS NULL OR expires_at > ?)';
      const kp = [`%${query}%`, now];
      if (category) { kSql += ' AND category = ?'; kp.push(category); }
      kSql += ' ORDER BY created_at DESC LIMIT 50';
      const keywordRows = db.prepare(kSql).all(...kp);

      // ── semantic half ──
      let semanticRows = [];
      const embedding = await getEmbedding(query);
      if (embedding) {
        let sSql = 'SELECT * FROM memories WHERE embedding IS NOT NULL AND (expires_at IS NULL OR expires_at > ?)';
        const sp = [now];
        if (category) { sSql += ' AND category = ?'; sp.push(category); }
        semanticRows = db.prepare(sSql).all(...sp)
          .map(r => { try { return { ...r, score: cosineSim(embedding, JSON.parse(r.embedding)) }; } catch { return null; } })
          .filter(Boolean)
          .sort((a, b) => b.score - a.score)
          .slice(0, 50);
      }

      // ── Reciprocal Rank Fusion ──
      const K = 60;
      const fuse = new Map(); // id -> { row, rrf }
      keywordRows.forEach((r, i) => {
        const e = fuse.get(r.id) || { row: r, rrf: 0 };
        e.rrf += 1 / (K + i + 1);
        fuse.set(r.id, e);
      });
      semanticRows.forEach((r, i) => {
        const e = fuse.get(r.id) || { row: r, rrf: 0 };
        e.rrf += 1 / (K + i + 1);
        fuse.set(r.id, e);
      });

      const fused = [...fuse.values()]
        .sort((a, b) => b.rrf - a.rrf)
        .slice(0, limit)
        .map(e => ({ ...fmt(e.row), rrf_score: +e.rrf.toFixed(4) }));

      return {
        content: [{
          type: 'text',
          text: fused.length
            ? `Found ${fused.length} result(s) (keyword+semantic fused):\n${JSON.stringify(fused, null, 2)}`
            : `No results for: "${query}"`,
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
  const headerToken = req.headers.authorization?.replace('Bearer ', '').trim();
  const queryToken = req.query.token;
  if (headerToken !== AUTH_TOKEN && queryToken !== AUTH_TOKEN) return res.status(401).json({ error: 'Unauthorized' });
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

// ── MCP StreamableHTTP — for Claude.ai web ───────────────────────────────────

app.post('/mcp', auth, async (req, res) => {
  res.setHeader('X-Accel-Buffering', 'no');
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  try {
    await createMcpServer().connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error('[MCP] StreamableHTTP error:', err);
    if (!res.headersSent) res.status(500).end();
  }
});

// GET /mcp is not used in stateless mode
app.get('/mcp', (_req, res) => res.status(405).json({ error: 'Use POST /mcp or GET /sse' }));
app.delete('/mcp', (_req, res) => res.status(405).json({ error: 'Stateless mode, no sessions to delete' }));

// ── REST API ──────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.get('/api/memories', auth, (req, res) => {
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

app.post('/api/memories', auth, async (req, res) => {
  const { content, category, tags = [], source = '', mood = null } = req.body;
  if (!content || !category) return res.status(400).json({ error: 'content and category required' });
  const id = uuidv4(), now = new Date().toISOString();
  let expires_at = null;
  if (category === 'daily') {
    const exp = new Date(); exp.setDate(exp.getDate() + 3); expires_at = exp.toISOString();
  }
  const embedding = await getEmbedding(content);
  db.prepare('INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at,embedding) VALUES (?,?,?,?,?,?,?,?,?,?)')
    .run(id, content, category, JSON.stringify(tags), source, mood, now, now, expires_at,
      embedding ? JSON.stringify(embedding) : null);
  res.json({ id, message: 'Memory saved' });
});

app.put('/api/memories/:id', auth, (req, res) => {
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

app.delete('/api/memories/:id', auth, (req, res) => {
  const r = db.prepare('DELETE FROM memories WHERE id = ?').run(req.params.id);
  res.json({ deleted: r.changes > 0 });
});

app.get('/api/stats', auth, (_req, res) => {
  cleanExpired();
  const total = db.prepare('SELECT COUNT(*) as c FROM memories').get().c;
  const byCat = db.prepare('SELECT category, COUNT(*) as c FROM memories GROUP BY category').all();
  res.json({ total, by_category: Object.fromEntries(byCat.map(r => [r.category, r.c])) });
});

app.get('/api/diary-calendar', auth, (req, res) => {
  const { year, month } = req.query;
  const prefix = `${year}-${String(month).padStart(2, '0')}`;
  const rows = db.prepare(`
    SELECT id, mood, substr(content,1,120) as preview, substr(created_at,1,10) as date
    FROM memories WHERE category='diary' AND created_at LIKE ?
    ORDER BY created_at DESC
  `).all(`${prefix}%`);
  res.json(rows);
});

// ── Read-only search endpoints (for reflex hook) ──────────────────────────────
// Mirror the MCP find_related / hybrid_search tools but expose them as REST so
// external hooks (CC UserPromptSubmit hook) can POST queries without speaking
// MCP protocol. **Read-only by design** — no write/delete/update here.

app.post('/api/search/find_related', auth, async (req, res) => {
  try {
    const { query, top_k = 5, category } = req.body || {};
    if (!query || typeof query !== 'string') {
      return res.status(400).json({ error: 'query (non-empty string) required' });
    }
    const k = Math.max(1, Math.min(20, parseInt(top_k) || 5));

    const embedding = await getEmbedding(query);
    if (!embedding) {
      return res.json({ results: [], count: 0, note: 'Embedding unavailable (set VOYAGE_API_KEY)' });
    }

    let sql = 'SELECT * FROM memories WHERE embedding IS NOT NULL AND (expires_at IS NULL OR expires_at > ?)';
    const p = [new Date().toISOString()];
    if (category) { sql += ' AND category = ?'; p.push(category); }
    const rows = db.prepare(sql).all(...p);

    const results = rows
      .map(r => { try { return { ...r, score: cosineSim(embedding, JSON.parse(r.embedding)) }; } catch { return null; } })
      .filter(Boolean)
      .sort((a, b) => b.score - a.score)
      .slice(0, k)
      .map(r => ({ ...fmt(r), similarity: +r.score.toFixed(3) }));

    res.json({ results, count: results.length });
  } catch (err) {
    console.error('[api/search/find_related] error:', err);
    res.status(500).json({ error: 'Internal error' });
  }
});

app.post('/api/search/hybrid', auth, async (req, res) => {
  try {
    const { query, limit = 10, category } = req.body || {};
    if (!query || typeof query !== 'string') {
      return res.status(400).json({ error: 'query (non-empty string) required' });
    }
    const lim = Math.max(1, Math.min(20, parseInt(limit) || 10));

    cleanExpired();
    const now = new Date().toISOString();

    // keyword half
    let kSql = 'SELECT * FROM memories WHERE content LIKE ? AND (expires_at IS NULL OR expires_at > ?)';
    const kp = [`%${query}%`, now];
    if (category) { kSql += ' AND category = ?'; kp.push(category); }
    kSql += ' ORDER BY created_at DESC LIMIT 50';
    const keywordRows = db.prepare(kSql).all(...kp);

    // semantic half
    let semanticRows = [];
    const embedding = await getEmbedding(query);
    if (embedding) {
      let sSql = 'SELECT * FROM memories WHERE embedding IS NOT NULL AND (expires_at IS NULL OR expires_at > ?)';
      const sp = [now];
      if (category) { sSql += ' AND category = ?'; sp.push(category); }
      semanticRows = db.prepare(sSql).all(...sp)
        .map(r => { try { return { ...r, score: cosineSim(embedding, JSON.parse(r.embedding)) }; } catch { return null; } })
        .filter(Boolean)
        .sort((a, b) => b.score - a.score)
        .slice(0, 50);
    }

    // RRF fusion
    const K = 60;
    const fuse = new Map();
    keywordRows.forEach((r, i) => {
      const e = fuse.get(r.id) || { row: r, rrf: 0 };
      e.rrf += 1 / (K + i + 1);
      fuse.set(r.id, e);
    });
    semanticRows.forEach((r, i) => {
      const e = fuse.get(r.id) || { row: r, rrf: 0 };
      e.rrf += 1 / (K + i + 1);
      fuse.set(r.id, e);
    });

    const fused = [...fuse.values()]
      .sort((a, b) => b.rrf - a.rrf)
      .slice(0, lim)
      .map(e => ({ ...fmt(e.row), rrf_score: +e.rrf.toFixed(4) }));

    res.json({ results: fused, count: fused.length, semantic_enabled: !!embedding });
  } catch (err) {
    console.error('[api/search/hybrid] error:', err);
    res.status(500).json({ error: 'Internal error' });
  }
});

// ── GitHub Backup ─────────────────────────────────────────────────────────────

const BACKUP_REPO  = process.env.BACKUP_REPO  || 'lanmoyinyue/mcp-memory-server';
const BACKUP_PATH  = 'backups/memories.jsonl';
const BACKUP_TOKEN = process.env.BACKUP_GITHUB_TOKEN;

async function ghRequest(method, path, body) {
  const res = await fetch(`https://api.github.com/repos/${BACKUP_REPO}/contents/${path}`, {
    method,
    headers: {
      'Authorization': `Bearer ${BACKUP_TOKEN}`,
      'Content-Type': 'application/json',
      'User-Agent': 'mcp-memory-server',
      'Accept': 'application/vnd.github+json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return res;
}

async function runBackup() {
  if (!BACKUP_TOKEN) return;
  try {
    const rows = db.prepare('SELECT * FROM memories').all().map(fmt);
    const content = rows.map(r => JSON.stringify(r)).join('\n');
    const contentB64 = Buffer.from(content).toString('base64');

    // 查现有文件的 sha（更新时必须带上）
    let sha = null;
    const existing = await ghRequest('GET', BACKUP_PATH);
    if (existing.ok) sha = (await existing.json()).sha;

    await ghRequest('PUT', BACKUP_PATH, {
      message: `backup: ${new Date().toISOString().slice(0, 10)} (${rows.length} memories) [zeabur skip]`,
      content: contentB64,
      ...(sha ? { sha } : {}),
    });
    console.log(`[backup] ${rows.length} memories backed up to GitHub`);
  } catch (e) {
    console.error('[backup] failed:', e.message);
  }
}

async function autoRestore() {
  if (!BACKUP_TOKEN) return;
  try {
    const r = await ghRequest('GET', BACKUP_PATH);
    if (!r.ok) return;
    const data = await r.json();
    const content = Buffer.from(data.content.replace(/\n/g, ''), 'base64').toString('utf-8');
    const lines = content.split('\n').filter(l => l.trim());
    let restored = 0;
    for (const line of lines) {
      const m = JSON.parse(line);
      const exists = db.prepare('SELECT id FROM memories WHERE id = ?').get(m.id);
      if (!exists) {
        db.prepare('INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?)')
          .run(m.id, m.content, m.category, JSON.stringify(m.tags || []), m.source || '', m.mood, m.created_at, m.updated_at, m.expires_at || null);
        restored++;
      }
    }
    console.log(`[backup] auto-restored ${restored} memories from GitHub`);
  } catch (e) {
    console.error('[backup] auto-restore failed:', e.message);
  }
}

// 每24小时自动备份，启动30秒后先跑一次
setTimeout(() => { runBackup(); setInterval(runBackup, 24 * 60 * 60 * 1000); }, 30_000);
// 启动时若 DB 为空则自动从 GitHub 恢复
autoRestore();

// 手动触发端点
app.post('/api/backup/run', auth, async (_req, res) => {
  try { await runBackup(); res.json({ ok: true }); }
  catch (e) { res.status(500).json({ error: e.message }); }
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
  if (BACKUP_TOKEN) console.log('Backup: GitHub auto-backup enabled');
});
