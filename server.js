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
import crypto from 'crypto';

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

db.exec(`
  CREATE TABLE IF NOT EXISTS memory_edges (
    source_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    weight    REAL NOT NULL DEFAULT 1.0,
    created_at TEXT NOT NULL,
    PRIMARY KEY (source_id, target_id)
  );
  CREATE INDEX IF NOT EXISTS idx_edges_source ON memory_edges(source_id);
  CREATE INDEX IF NOT EXISTS idx_edges_target ON memory_edges(target_id);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS raw_events (
    id                TEXT PRIMARY KEY,
    session_id        TEXT NOT NULL DEFAULT '',
    source            TEXT NOT NULL DEFAULT '',
    channel           TEXT NOT NULL DEFAULT 'cc',
    role              TEXT NOT NULL,
    speaker           TEXT NOT NULL DEFAULT '',
    content           TEXT NOT NULL,
    timestamp         TEXT NOT NULL,
    linked_memory_ids TEXT NOT NULL DEFAULT '[]',
    metadata          TEXT NOT NULL DEFAULT '{}'
  );
  CREATE INDEX IF NOT EXISTS idx_re_channel   ON raw_events(channel);
  CREATE INDEX IF NOT EXISTS idx_re_timestamp ON raw_events(timestamp);
  CREATE INDEX IF NOT EXISTS idx_re_session   ON raw_events(session_id);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS memory_candidates (
    id                       TEXT PRIMARY KEY,
    raw_event_ids            TEXT NOT NULL DEFAULT '[]',
    dedupe_key               TEXT NOT NULL,
    source                   TEXT NOT NULL DEFAULT '',
    channel                  TEXT NOT NULL DEFAULT '',
    speaker                  TEXT NOT NULL DEFAULT '',
    summary                  TEXT NOT NULL,
    suggested_category       TEXT NOT NULL DEFAULT 'daily',
    reason                   TEXT NOT NULL DEFAULT '',
    confidence               REAL NOT NULL DEFAULT 0,
    status                   TEXT NOT NULL DEFAULT 'pending',
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL,
    reviewed_at              TEXT,
    review_note              TEXT,
    expires_at               TEXT
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_dedupe ON memory_candidates(dedupe_key);
  CREATE INDEX IF NOT EXISTS idx_candidate_status ON memory_candidates(status, created_at);
  CREATE INDEX IF NOT EXISTS idx_candidate_expires ON memory_candidates(expires_at);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS somatic_events (
    id           TEXT PRIMARY KEY,
    actor        TEXT NOT NULL DEFAULT 'moon',
    target       TEXT NOT NULL DEFAULT 'ke',
    source       TEXT NOT NULL,
    channel      TEXT,
    modality     TEXT NOT NULL,
    action       TEXT,
    zone         TEXT,
    labels       TEXT NOT NULL DEFAULT '[]',
    intensity    REAL NOT NULL DEFAULT 1.0,
    valence      TEXT,
    text_excerpt TEXT,
    created_at   TEXT NOT NULL,
    expires_at   TEXT
  );
  CREATE INDEX IF NOT EXISTS idx_somatic_events_target ON somatic_events(target, created_at);
  CREATE INDEX IF NOT EXISTS idx_somatic_events_expires ON somatic_events(expires_at);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS somatic_state (
    id             TEXT PRIMARY KEY,
    target         TEXT NOT NULL DEFAULT 'ke',
    modality       TEXT NOT NULL,
    zone           TEXT,
    label          TEXT NOT NULL,
    strength       REAL NOT NULL,
    half_life_sec  INTEGER NOT NULL,
    last_event_id  TEXT,
    updated_at     TEXT NOT NULL,
    expires_at     TEXT NOT NULL
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_somatic_state_key ON somatic_state(target, modality, zone, label);
  CREATE INDEX IF NOT EXISTS idx_somatic_state_expires ON somatic_state(expires_at);
`);

db.exec(`
  CREATE TABLE IF NOT EXISTS somatic_hooks (
    id          TEXT PRIMARY KEY,
    target      TEXT NOT NULL DEFAULT 'ke',
    modality    TEXT NOT NULL,
    cue         TEXT NOT NULL,
    fact_key    TEXT,
    memory_id   TEXT,
    note        TEXT,
    weight      REAL NOT NULL DEFAULT 1.0,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_somatic_hooks_key ON somatic_hooks(target, modality, cue);
  CREATE INDEX IF NOT EXISTS idx_somatic_hooks_fact ON somatic_hooks(fact_key);
`);

// Migrate: add embedding column for existing databases
try { db.exec('ALTER TABLE memories ADD COLUMN embedding TEXT'); } catch {}
// Migrate: add pinned column (pinned memories sort first in read_memories)
try { db.exec('ALTER TABLE memories ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0'); } catch {}
// Migrate: add activation_score for memory temperature
try { db.exec('ALTER TABLE memories ADD COLUMN activation_score REAL NOT NULL DEFAULT 0'); } catch {}
// Migrate: add content_hash for dedup
try { db.exec('ALTER TABLE memories ADD COLUMN content_hash TEXT'); } catch {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_content_hash ON memories(content_hash)'); } catch {}
// Migrate: Z-axis fact evolution (LMC-5 Module 1)
try { db.exec('ALTER TABLE memories ADD COLUMN fact_key TEXT'); } catch {}
try { db.exec('ALTER TABLE memories ADD COLUMN superseded_by TEXT'); } catch {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_fact_key ON memories(fact_key)'); } catch {}
// Migrate: protected memories and curated-memory evidence links
try { db.exec('ALTER TABLE memories ADD COLUMN protected INTEGER NOT NULL DEFAULT 0'); } catch {}
try { db.exec("ALTER TABLE memories ADD COLUMN evidence_raw_ids TEXT NOT NULL DEFAULT '[]'"); } catch {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_memories_protected ON memories(protected)'); } catch {}
// Migrate: raw event provenance fields for external conversation entrypoints
try { db.exec("ALTER TABLE raw_events ADD COLUMN source TEXT NOT NULL DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE raw_events ADD COLUMN speaker TEXT NOT NULL DEFAULT ''"); } catch {}
try { db.exec("ALTER TABLE raw_events ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'"); } catch {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_re_source ON raw_events(source)'); } catch {}
// Migrate: hippocampus proposal layer for raw_events -> reviewed candidates.
try { db.exec("ALTER TABLE memory_candidates ADD COLUMN expires_at TEXT"); } catch {}
try { db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_dedupe ON memory_candidates(dedupe_key)'); } catch {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_candidate_status ON memory_candidates(status, created_at)'); } catch {}
try { db.exec('CREATE INDEX IF NOT EXISTS idx_candidate_expires ON memory_candidates(expires_at)'); } catch {}

const contentHash = (text, category) => crypto.createHash('sha256').update(`${category}:${text.trim()}`).digest('hex');

// Categories that automation (supersede/decay/merge/heat) must never mutate.
const PROTECTED_CATEGORIES = new Set(['diary', 'deep', 'anchor', '私藏', '心动', 'cc-diary']);
const isProtectedCategory = (category) => PROTECTED_CATEGORIES.has(String(category || '').trim());
const resolveProtectedFlag = (category, requested, existing = 0) => {
  if (isProtectedCategory(category)) return 1;
  if (requested !== undefined) return requested ? 1 : 0;
  return existing ? 1 : 0;
};
const rowIsProtected = (row) => !!row?.protected || isProtectedCategory(row?.category);

// One-time backfill: content_hash for existing memories
{
  const unhashed = db.prepare("SELECT id, content, category FROM memories WHERE content_hash IS NULL").all();
  if (unhashed.length) {
    const stmt = db.prepare('UPDATE memories SET content_hash = ? WHERE id = ?');
    for (const m of unhashed) stmt.run(contentHash(m.content, m.category), m.id);
    console.log(`[migrate] backfilled content_hash for ${unhashed.length} memories`);
  }
}

// One-time backfill: protect existing first-person and treasure-box memories.
{
  const cats = [...PROTECTED_CATEGORIES];
  const placeholders = cats.map(() => '?').join(',');
  const r = db.prepare(`UPDATE memories SET protected = 1 WHERE category IN (${placeholders}) AND protected = 0`).run(...cats);
  if (r.changes) console.log(`[migrate] protected ${r.changes} existing first-person memories`);
}

// One-time backfill: anchors & corridor now expire like daily (3 days).
// Both are only ever read as "latest 1" at wake-up and their content lives
// in diary. Stamp legacy rows that have no expiry; pinned ones are exempt.
// Archives: backups/anchors-archive-2026-06-10.jsonl, corridor-archive-2026-06-10.jsonl.
{
  const legacy = db.prepare(
    "SELECT id, created_at FROM memories WHERE category IN ('anchor','cc-anchor','corridor') AND expires_at IS NULL AND pinned = 0"
  ).all();
  const stamp = db.prepare('UPDATE memories SET expires_at = ? WHERE id = ?');
  for (const m of legacy) {
    const exp = new Date(m.created_at); exp.setDate(exp.getDate() + 3);
    stamp.run(exp.toISOString(), m.id);
  }
  if (legacy.length) console.log(`[migrate] stamped 3-day expiry on ${legacy.length} legacy anchors`);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const parseArrayField = (t) => {
  try {
    const parsed = JSON.parse(t || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};
const parseTags = parseArrayField;
const parseObjectField = (t) => {
  try {
    const parsed = typeof t === 'string' ? JSON.parse(t || '{}') : (t || {});
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
};

const fmt = (r) => ({
  id: r.id, content: r.content, category: r.category,
  tags: parseTags(r.tags), source: r.source, mood: r.mood || null, pinned: !!r.pinned,
  activation_score: +(r.activation_score || 0),
  protected: rowIsProtected(r),
  evidence_raw_ids: parseArrayField(r.evidence_raw_ids),
  fact_key: r.fact_key || null, superseded_by: r.superseded_by || null,
  created_at: r.created_at, updated_at: r.updated_at, expires_at: r.expires_at || null,
});

const fmtRawEvent = (r) => ({
  id: r.id,
  session_id: r.session_id || '',
  source: r.source || '',
  channel: r.channel,
  role: r.role,
  speaker: r.speaker || '',
  content: r.content,
  timestamp: r.timestamp,
  linked_memory_ids: parseArrayField(r.linked_memory_ids),
  metadata: parseObjectField(r.metadata),
});

function extractExactTerms(query) {
  const text = String(query || '').trim();
  if (!text) return [];
  const terms = [];
  const re = /["'`“”‘’「」『』]([^"'`“”‘’「」『』]{1,120})["'`“”‘’「」『』]/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const term = m[1].trim();
    if (term) terms.push(term);
  }
  if (!terms.length) terms.push(text);
  return [...new Set(terms)].slice(0, 8);
}

function exactTermMatches(content, term) {
  const haystack = String(content || '');
  const needle = String(term || '').trim();
  if (!needle) return false;
  if (/^[A-Za-z0-9_-]+$/.test(needle)) {
    const escaped = needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`(^|[^A-Za-z0-9_-])${escaped}($|[^A-Za-z0-9_-])`, 'i').test(haystack);
  }
  return haystack.includes(needle);
}

function rawEventSearch({ query, mode = 'fuzzy', channel = 'all', source = '', speaker = '', limit = 20 }) {
  const params = [];
  const where = [];
  if (mode === 'exact') {
    const terms = extractExactTerms(query);
    if (!terms.length) return { rows: [], terms };
    where.push(`(${terms.map(() => 'INSTR(content, ?) > 0').join(' OR ')})`);
    params.push(...terms);
  } else {
    where.push('content LIKE ?');
    params.push(`%${query}%`);
  }
  if (channel !== 'all') { where.push('channel = ?'); params.push(channel); }
  if (source) { where.push('source = ?'); params.push(source); }
  if (speaker) { where.push('speaker = ?'); params.push(speaker); }
  let sql = `SELECT * FROM raw_events WHERE ${where.join(' AND ')} ORDER BY timestamp DESC LIMIT ?`;
  params.push(Math.max(limit * (mode === 'exact' ? 3 : 1), limit));
  let rows = db.prepare(sql).all(...params);
  const terms = mode === 'exact' ? extractExactTerms(query) : [];
  if (mode === 'exact') {
    rows = rows.filter(row => terms.some(term => exactTermMatches(row.content, term))).slice(0, limit);
  }
  return { rows, terms };
}

function inspectMemoryEdges(limit = 50) {
  const now = new Date().toISOString();
  const issues = [];
  const push = (type, severity, source_id, target_id, description) => {
    if (issues.length >= limit) return;
    issues.push({ type, severity, source_id, target_id, description });
  };

  const orphanRows = db.prepare(`
    SELECT e.source_id, e.target_id
    FROM memory_edges e
    LEFT JOIN memories s ON s.id = e.source_id
    LEFT JOIN memories t ON t.id = e.target_id
    WHERE s.id IS NULL OR t.id IS NULL
    ORDER BY e.created_at DESC
    LIMIT ?
  `).all(limit);
  for (const row of orphanRows) {
    push('orphan_edge', 'critical', row.source_id, row.target_id, '边指向不存在的记忆');
  }

  const selfLoopRows = db.prepare(`
    SELECT source_id, target_id
    FROM memory_edges
    WHERE source_id = target_id
    ORDER BY created_at DESC
    LIMIT ?
  `).all(limit);
  for (const row of selfLoopRows) {
    push('self_loop', 'critical', row.source_id, row.target_id, '边的起点和终点是同一条记忆');
  }

  const nonLiveRows = db.prepare(`
    SELECT e.source_id, e.target_id,
           s.superseded_by AS source_superseded_by, t.superseded_by AS target_superseded_by,
           s.expires_at AS source_expires_at, t.expires_at AS target_expires_at
    FROM memory_edges e
    JOIN memories s ON s.id = e.source_id
    JOIN memories t ON t.id = e.target_id
    WHERE s.superseded_by IS NOT NULL
       OR t.superseded_by IS NOT NULL
       OR (s.expires_at IS NOT NULL AND s.expires_at <= ?)
       OR (t.expires_at IS NOT NULL AND t.expires_at <= ?)
    ORDER BY e.created_at DESC
    LIMIT ?
  `).all(now, now, limit);
  for (const row of nonLiveRows) {
    const reasons = [];
    if (row.source_superseded_by) reasons.push('起点已被事实演化替代');
    if (row.target_superseded_by) reasons.push('终点已被事实演化替代');
    if (row.source_expires_at && row.source_expires_at <= now) reasons.push('起点已过期');
    if (row.target_expires_at && row.target_expires_at <= now) reasons.push('终点已过期');
    push('non_live_endpoint', 'warning', row.source_id, row.target_id, reasons.join('；') || '边连接到非当前记忆');
  }

  const total_edges = db.prepare('SELECT COUNT(*) as c FROM memory_edges').get().c;
  return {
    total_edges,
    issue_count: issues.length,
    checked_at: now,
    issues,
    note: '只读巡逻报告：不删除、不归档、不修改 memory_edges。',
  };
}

// Candidate category labels shown to Moon. Machine values stay stable; edit here
// when adding a new candidate category.
const CANDIDATE_CATEGORY_LABELS = {
  daily: '日常候选',
  work: '工作候选',
  todo_candidate: '待办候选',
  private_candidate: '私密候选',
  boundary_candidate: '边界候选',
  preference_candidate: '偏好候选',
};
const candidateCategoryLabel = (category) => CANDIDATE_CATEGORY_LABELS[category] || category || '';
const VALID_CANDIDATE_STATUS = new Set(['pending', 'accepted', 'rejected', 'merged', 'stale']);
const candidateDedupeKey = (ids) => crypto.createHash('sha256').update([...ids].sort().join('\0')).digest('hex');
const normalizeCandidateText = (text) => String(text || '').replace(/\s+/g, ' ').trim().toLowerCase();
const candidateTimeBucket = (timestamp, bucketMs = 60_000) => {
  const ms = Date.parse(timestamp || '');
  if (!Number.isFinite(ms)) return '';
  return String(Math.floor(ms / bucketMs));
};
const candidateFingerprint = (row) => crypto.createHash('sha256').update([
  row.source || '',
  row.channel || '',
  row.speaker || row.role || '',
  candidateTimeBucket(row.timestamp),
  normalizeCandidateText(row.content),
].join('\0')).digest('hex');
const candidateExpiresAt = (now, days = 7) => {
  const exp = new Date(now);
  exp.setDate(exp.getDate() + days);
  return exp.toISOString();
};
const markStaleCandidates = (now = new Date().toISOString()) => db.prepare(
  "UPDATE memory_candidates SET status='stale', updated_at=? WHERE status='pending' AND expires_at IS NOT NULL AND expires_at < ?"
).run(now, now);
function candidateHasExistingRawEvents(rawIds) {
  const stmt = db.prepare('SELECT id FROM memory_candidates WHERE raw_event_ids LIKE ? LIMIT 1');
  return rawIds.some(id => stmt.get(`%"${String(id).replace(/[%_]/g, '')}"%`));
}
const fmtMemoryCandidate = (r) => ({
  id: r.id,
  raw_event_ids: parseArrayField(r.raw_event_ids),
  dedupe_key: r.dedupe_key,
  source: r.source || '',
  channel: r.channel || '',
  speaker: r.speaker || '',
  summary: r.summary,
  suggested_category: r.suggested_category,
  suggested_category_label: candidateCategoryLabel(r.suggested_category),
  reason: r.reason || '',
  confidence: Number(r.confidence || 0),
  status: r.status,
  created_at: r.created_at,
  updated_at: r.updated_at,
  reviewed_at: r.reviewed_at || null,
  review_note: r.review_note || null,
  expires_at: r.expires_at || null,
});

function rawEventDisplaySource(row) {
  if (row.source === 'telegram') return row.channel === 'private' ? 'TG 私聊' : 'TG';
  if (row.source === 'kechat-light') return '小家';
  return row.source || row.channel || '未知入口';
}

function truncateText(text, max = 180) {
  const clean = String(text || '').replace(/\s+/g, ' ').trim();
  return clean.length > max ? `${clean.slice(0, max - 1)}…` : clean;
}

function classifyRawEvent(row) {
  const text = String(row.content || '');
  const lower = text.toLowerCase();
  const isPrivate = row.channel === 'private' || row.channel === 'intimate';
  if (isPrivate) {
    return { category: 'private_candidate', reason: '私密或亲密入口的原话，只进入私密候选。', confidence: 0.72 };
  }
  if (/记住|记一下|存一下|这个要记|下次别忘|别忘/.test(text)) {
    return { category: 'preference_candidate', reason: '月亮明确要求记录或提醒。', confidence: 0.9 };
  }
  if (/边界|权限|不能|不要|不许|只能|必须|严格|越权/.test(text)) {
    return { category: 'boundary_candidate', reason: '包含明确边界、权限或约束。', confidence: 0.82 };
  }
  if (/待办|下一步|接下来|TODO|todo|要做|之后做/.test(text)) {
    return { category: 'todo_candidate', reason: '包含后续行动或待办线索。', confidence: 0.78 };
  }
  if (/项目|方案|审核|部署|上线|重启|同步|架构|记忆库|raw_events|候选|mcp|git|github|vps|zeabur|api|bug|修|测试|推送|commit/i.test(lower)) {
    return { category: 'work', reason: '包含项目、部署、架构、测试或排错信息。', confidence: 0.76 };
  }
  if (/喜欢|偏好|我会|我不|我只|我想|我需要/.test(text)) {
    return { category: 'preference_candidate', reason: '包含月亮的偏好或需求表达。', confidence: 0.68 };
  }
  if (text.length >= 18) {
    return { category: 'daily', reason: '有一定信息量的日常原话。', confidence: 0.55 };
  }
  return null;
}

function buildCandidateSummary(row, category) {
  const source = rawEventDisplaySource(row);
  const speaker = row.speaker === 'moon' || row.role === 'user' ? '月亮' : (row.speaker || row.role || '对方');
  const text = truncateText(row.content, 220);
  if (category === 'work') return `${speaker}在${source}说：${text}`;
  if (category === 'private_candidate') {
    const ts = new Date(row.timestamp || Date.now()).toLocaleString('zh-CN', { hour12: false });
    return `${speaker}在${source}发了一条私密消息（${ts}）`;
  }
  if (category === 'boundary_candidate') return `${speaker}在${source}表达了边界：${text}`;
  if (category === 'preference_candidate') return `${speaker}在${source}表达了偏好或需求：${text}`;
  if (category === 'todo_candidate') return `${speaker}在${source}提到待办：${text}`;
  return `${speaker}在${source}说：${text}`;
}

const cleanExpired = () => {
  const r = db.prepare('DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at < ?')
    .run(new Date().toISOString());
  if (r.changes) {
    db.prepare(`DELETE FROM memory_edges WHERE source_id NOT IN (SELECT id FROM memories) OR target_id NOT IN (SELECT id FROM memories)`).run();
  }
};

// Categories that auto-expire after 3 days. Everything else is permanent.
const EXPIRING = new Set(['daily', 'anchor', 'cc-anchor', 'corridor']);
function expiryFor(category) {
  if (!EXPIRING.has(category)) return null;
  const exp = new Date(); exp.setDate(exp.getDate() + 3);
  return exp.toISOString();
}

const SOMATIC_MODALITIES = new Set(['touch', 'smell', 'taste', 'sound']);
const SOMATIC_DEFAULT_HALF_LIFE = {
  touch: 600,
  smell: 1200,
  taste: 900,
  sound: 450,
};
const SOMATIC_MIN_STRENGTH = 0.15;
const SOMATIC_MAX_STRENGTH = 1.5;
const SOMATIC_TARGET = 'ke';

function ensureSomaticTarget(target = SOMATIC_TARGET) {
  const normalized = String(target || SOMATIC_TARGET).trim();
  if (normalized !== SOMATIC_TARGET) {
    throw new Error('somatic v1 only supports target=ke');
  }
  return normalized;
}

function ensureSomaticModality(modality) {
  const normalized = String(modality || '').trim().toLowerCase();
  if (!SOMATIC_MODALITIES.has(normalized)) {
    throw new Error('modality must be one of touch/smell/taste/sound');
  }
  return normalized;
}

function somaticHalfLife(modality, halfLifeSec) {
  const value = Number(halfLifeSec || 0);
  if (Number.isFinite(value) && value >= 60 && value <= 24 * 60 * 60) return Math.round(value);
  return SOMATIC_DEFAULT_HALF_LIFE[modality] || SOMATIC_DEFAULT_HALF_LIFE.touch;
}

function somaticExpiresAt(now, halfLifeSec, ttlSec) {
  const ttl = Number(ttlSec || 0);
  const sec = Number.isFinite(ttl) && ttl >= 60
    ? Math.min(ttl, 7 * 24 * 60 * 60)
    : Math.min(halfLifeSec * 5, 7 * 24 * 60 * 60);
  return new Date(new Date(now).getTime() + sec * 1000).toISOString();
}

function somaticStateId(target, modality, zone, label) {
  return crypto.createHash('sha256')
    .update([target, modality, zone || '', label].join('\u0000'))
    .digest('hex');
}

function somaticDecayedStrength(row, now = new Date().toISOString()) {
  const elapsedSec = Math.max(0, (Date.parse(now) - Date.parse(row.updated_at)) / 1000);
  const halfLife = Math.max(1, Number(row.half_life_sec || SOMATIC_DEFAULT_HALF_LIFE.touch));
  const strength = Number(row.strength || 0) * Math.pow(0.5, elapsedSec / halfLife);
  return Math.max(0, strength);
}

function fmtSomaticEvent(r) {
  return {
    id: r.id,
    actor: r.actor,
    target: r.target,
    source: r.source,
    channel: r.channel || null,
    modality: r.modality,
    action: r.action || null,
    zone: r.zone || null,
    labels: parseArrayField(r.labels),
    intensity: Number(r.intensity || 0),
    valence: r.valence || null,
    text_excerpt: r.text_excerpt || null,
    created_at: r.created_at,
    expires_at: r.expires_at || null,
  };
}

function fmtSomaticHook(r) {
  return {
    id: r.id,
    target: r.target,
    modality: r.modality,
    cue: r.cue,
    fact_key: r.fact_key || null,
    memory_id: r.memory_id || null,
    note: r.note || null,
    weight: Number(r.weight || 0),
    created_at: r.created_at,
    updated_at: r.updated_at,
  };
}

function cleanSomaticExpired(now = new Date().toISOString(), minStrength = SOMATIC_MIN_STRENGTH) {
  db.prepare('DELETE FROM somatic_events WHERE expires_at IS NOT NULL AND expires_at < ?').run(now);
  const rows = db.prepare('SELECT * FROM somatic_state WHERE expires_at < ?').all(now);
  const del = db.prepare('DELETE FROM somatic_state WHERE id = ?');
  for (const row of rows) del.run(row.id);
  const active = db.prepare('SELECT * FROM somatic_state WHERE expires_at >= ?').all(now);
  for (const row of active) {
    if (somaticDecayedStrength(row, now) < minStrength) del.run(row.id);
  }
}

function activeSomaticStates(target = SOMATIC_TARGET, minStrength = SOMATIC_MIN_STRENGTH) {
  const now = new Date().toISOString();
  cleanSomaticExpired(now, minStrength);
  const rows = db.prepare('SELECT * FROM somatic_state WHERE target = ? AND expires_at >= ? ORDER BY modality, updated_at DESC').all(target, now);
  const updateRows = [];
  const del = db.prepare('DELETE FROM somatic_state WHERE id = ?');
  for (const row of rows) {
    const strength = somaticDecayedStrength(row, now);
    if (strength < minStrength) {
      del.run(row.id);
      continue;
    }
    updateRows.push({ ...row, strength: +strength.toFixed(4) });
  }
  return updateRows;
}

function matchingSomaticHooks(state) {
  const hooks = db.prepare('SELECT * FROM somatic_hooks WHERE target = ? AND modality = ? ORDER BY weight DESC, updated_at DESC')
    .all(state.target, state.modality)
    .map(fmtSomaticHook);
  const haystack = `${state.zone || ''} ${state.label}`.toLowerCase();
  return hooks.filter(h => haystack.includes(String(h.cue).toLowerCase())).slice(0, 3);
}

function somaticPromptText(items) {
  if (!items.length) return '';
  const names = { touch: '触觉', smell: '嗅觉', taste: '味觉', sound: '听觉' };
  const lines = ['[身体状态]'];
  for (const item of items) {
    const zone = item.zone ? `${item.zone}：` : '';
    let line = `${names[item.modality] || item.modality}：${zone}${item.label}，强度 ${Number(item.strength).toFixed(2)}，正在散。`;
    if (item.hooks?.length) {
      const hook = item.hooks[0];
      line += ` ${hook.cue} 勾起了旧联想${hook.fact_key ? `（fact_key: ${hook.fact_key}）` : ''}。`;
    }
    lines.push(line);
  }
  return lines.join('\n');
}

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
    'Save a new memory. category: deep=永久, diary=日记(永久), writing=写作进度, daily/anchor/cc-anchor=3天过期, 也可自定义分类名(永久). pinned=true 的记忆在 read_memories 里永远排最前且不过期',
    {
      content:  z.string().min(1).describe('Memory content'),
      category: z.string().describe('Memory layer: deep/daily/diary/writing/anchor or any custom category'),
      tags:     z.array(z.string()).optional().describe('Tags'),
      source:   z.string().optional().describe('Source context'),
      mood:     z.string().optional().describe('Mood for diary (happy/sad/calm/excited/anxious)'),
      pinned:   z.boolean().optional().describe('Pin this memory: always sorts first in read_memories, never expires'),
      fact_key: z.string().optional().describe('Z-axis fact key (e.g. "闻川部署位置"). Same fact_key = same evolving fact; old version auto-superseded'),
      protected: z.boolean().optional().describe('Lock this memory so automation cannot mutate it'),
      evidence_raw_ids: z.array(z.string()).optional().describe('raw_event ids used as evidence for this curated memory'),
    },
    async ({ content, category, tags, source, mood, pinned, fact_key, protected: protectedArg, evidence_raw_ids }) => {
      const hash = contentHash(content, category);
      const dupe = db.prepare('SELECT * FROM memories WHERE content_hash = ? AND (expires_at IS NULL OR expires_at > ?)').get(hash, new Date().toISOString());
      if (dupe) {
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              duplicate: true,
              existing: fmt(dupe),
              note: `已有相同内容的记忆（${dupe.category}），跳过写入。`,
            }, null, 2),
          }],
        };
      }

      const id = uuidv4(), now = new Date().toISOString();
      // daily/anchor/cc-anchor expire in 3 days; pinned never expires
      const expires_at = pinned ? null : expiryFor(category);
      const protectedFlag = resolveProtectedFlag(category, protectedArg, 0);
      const evidenceIds = JSON.stringify(evidence_raw_ids ?? []);
      // Compute embedding and find related BEFORE inserting (so new memory is excluded)
      const embedding = await getEmbedding(content);
      const related = findRelated(embedding, id);

      // Z-axis protected guard: never auto-supersede protected old memories.
      let oldFactRows = [];
      if (fact_key) {
        oldFactRows = db.prepare(
          'SELECT * FROM memories WHERE fact_key = ? AND id != ? AND superseded_by IS NULL'
        ).all(fact_key, id);
        const blocked = oldFactRows.filter(rowIsProtected);
        if (blocked.length) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                error: 'protected_fact_conflict',
                fact_key,
                protected_ids: blocked.map(m => m.id),
                note: 'Refusing to auto-supersede protected memories. Update protected facts manually after review.',
              }, null, 2),
            }],
          };
        }
      }

      db.prepare(
        'INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at,embedding,pinned,content_hash,fact_key,protected,evidence_raw_ids) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
      ).run(id, content, category, JSON.stringify(tags ?? []), source ?? '', mood ?? null, now, now, expires_at,
        embedding ? JSON.stringify(embedding) : null, pinned ? 1 : 0, hash, fact_key ?? null, protectedFlag, evidenceIds);

      // Z-axis: auto-supersede old versions of the same fact
      let superseded = [];
      if (fact_key) {
        const superStmt = db.prepare('UPDATE memories SET superseded_by = ?, updated_at = ? WHERE id = ?');
        for (const o of oldFactRows) {
          superStmt.run(id, now, o.id);
          superseded.push(o.id);
        }
      }

      // "写就是读"：把相关记忆存为边
      if (related.length) {
        const edgeStmt = db.prepare('INSERT OR IGNORE INTO memory_edges (source_id, target_id, weight, created_at) VALUES (?, ?, ?, ?)');
        for (const r of related) {
          edgeStmt.run(id, r.id, r.similarity ?? 0.5, now);
          edgeStmt.run(r.id, id, r.similarity ?? 0.5, now);
        }
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            saved: { id, category, fact_key: fact_key || null },
            superseded_ids: superseded,
            related_memories: related,
            edges_created: related.length * 2,
            note: embedding ? `Found ${related.length} related memories, created ${related.length * 2} edges.` : 'Set VOYAGE_API_KEY to enable semantic associations.',
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
      category:           z.string().optional().describe('Filter by category (deep/daily/diary/writing/anchor or custom)'),
      tags:               z.array(z.string()).optional().describe('Match any of these tags'),
      keyword:            z.string().optional().describe('Keyword in content'),
      limit:              z.number().int().min(1).max(100).optional().describe('Max results (default 20)'),
      include_expired:    z.boolean().optional().describe('Include expired daily memories'),
      include_superseded: z.boolean().optional().describe('Include superseded fact versions (default false)'),
    },
    async ({ category, tags, keyword, limit = 20, include_expired = false, include_superseded = false }) => {
      if (!include_expired) cleanExpired();
      let sql = 'SELECT * FROM memories WHERE 1=1';
      const p = [];
      if (!include_expired) { sql += ' AND (expires_at IS NULL OR expires_at > ?)'; p.push(new Date().toISOString()); }
      if (!include_superseded) { sql += ' AND superseded_by IS NULL'; }
      if (category) { sql += ' AND category = ?'; p.push(category); }
      if (keyword)  { sql += ' AND content LIKE ?'; p.push(`%${keyword}%`); }
      sql += ' ORDER BY pinned DESC, created_at DESC LIMIT ?';
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
      if (r.changes) {
        db.prepare('DELETE FROM memory_edges WHERE source_id = ? OR target_id = ?').run(id, id);
      }
      return { content: [{ type: 'text', text: r.changes ? `Deleted ${id}.` : `Not found: ${id}` }] };
    }
  );

  // 5. update_memory
  mcp.tool(
    'update_memory',
    'Update content, category, tags, source, mood, or pinned of an existing memory. Passing category moves a memory between layers without delete+re-create. pinned=true pins it to the top of read_memories and exempts it from expiry; pinned=false unpins.',
    {
      id:       z.string().describe('Memory ID'),
      content:  z.string().optional(),
      category: z.string().optional().describe('Move memory to a different category (deep/daily/diary/writing/anchor or custom)'),
      tags:     z.array(z.string()).optional(),
      source:   z.string().optional(),
      mood:     z.string().optional(),
      pinned:   z.boolean().optional().describe('Pin/unpin: pinned memories sort first in read_memories and never expire'),
      fact_key: z.string().optional().describe('Set or change the Z-axis fact key for this memory'),
      protected: z.boolean().optional().describe('Lock/unlock automation protection; protected categories stay locked'),
      evidence_raw_ids: z.array(z.string()).optional().describe('Replace linked raw_event evidence ids'),
    },
    async ({ id, content, category, tags, source, mood, pinned, fact_key, protected: protectedArg, evidence_raw_ids }) => {
      const row = db.prepare('SELECT * FROM memories WHERE id = ?').get(id);
      if (!row) return { content: [{ type: 'text', text: `Not found: ${id}` }] };
      const newCategory = category ?? row.category;
      const newPinned = pinned !== undefined ? (pinned ? 1 : 0) : row.pinned;
      const newProtected = resolveProtectedFlag(newCategory, protectedArg, row.protected);
      const newEvidenceIds = evidence_raw_ids !== undefined ? JSON.stringify(evidence_raw_ids) : row.evidence_raw_ids;
      let expires_at = row.expires_at;
      const categoryChanged = category !== undefined && category !== row.category;
      const pinnedChanged = pinned !== undefined && (pinned ? 1 : 0) !== row.pinned;
      if (categoryChanged || pinnedChanged) {
        expires_at = newPinned ? null : expiryFor(newCategory);
      }
      const newFactKey = fact_key !== undefined ? fact_key : row.fact_key;
      db.prepare('UPDATE memories SET content=?,category=?,tags=?,source=?,mood=?,pinned=?,updated_at=?,expires_at=?,content_hash=?,fact_key=?,protected=?,evidence_raw_ids=? WHERE id=?').run(
        content ?? row.content,
        newCategory,
        tags !== undefined ? JSON.stringify(tags) : row.tags,
        source ?? row.source,
        mood !== undefined ? mood : row.mood,
        newPinned,
        new Date().toISOString(),
        expires_at,
        contentHash(content ?? row.content, newCategory),
        newFactKey ?? null,
        newProtected,
        newEvidenceIds,
        id
      );
      const notes = [];
      if (categoryChanged) notes.push(`moved ${row.category} → ${newCategory}`);
      if (pinnedChanged) notes.push(newPinned ? 'pinned' : 'unpinned');
      if (fact_key !== undefined) notes.push(`fact_key=${fact_key || '(cleared)'}`);
      if (protectedArg !== undefined || newProtected !== row.protected) notes.push(newProtected ? 'protected' : 'unprotected');
      if (evidence_raw_ids !== undefined) notes.push(`evidence_raw_ids=${evidence_raw_ids.length}`);
      return { content: [{ type: 'text', text: notes.length ? `Updated ${id} (${notes.join(', ')}).` : `Updated ${id}.` }] };
    }
  );

  // 6. get_stats
  mcp.tool(
    'get_stats',
    'Return statistics about the memory store',
    {
      include_edge_health: z.boolean().optional().describe('Include read-only memory_edges patrol report'),
      edge_issue_limit: z.number().int().min(1).max(200).optional().describe('Max edge issues to return when include_edge_health=true'),
    },
    async ({ include_edge_health = false, edge_issue_limit = 50 }) => {
      cleanExpired();
      const total = db.prepare('SELECT COUNT(*) as c FROM memories').get().c;
      const byCat = db.prepare('SELECT category, COUNT(*) as c FROM memories GROUP BY category').all();
      const recent = db.prepare('SELECT * FROM memories ORDER BY created_at DESC LIMIT 5').all().map(fmt);
      const payload = {
        total_memories: total,
        by_category: Object.fromEntries(byCat.map(r => [r.category, r.c])),
        recent_memories: recent,
      };
      if (include_edge_health) payload.edge_health = inspectMemoryEdges(edge_issue_limit);
      return { content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }] };
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

  // 8. get_neighbors — find connected memories via edges (for association slot)
  mcp.tool(
    'get_neighbors',
    'Find memories connected to a given memory via edges. Returns neighbors sorted by edge weight. Use for association/联想.',
    {
      memory_id: z.string().min(1).describe('Memory ID to find neighbors for'),
      exclude:   z.array(z.string()).optional().describe('Memory IDs to exclude from results'),
      limit:     z.number().int().min(1).max(10).optional().describe('Max results (default 3)'),
    },
    async ({ memory_id, exclude = [], limit = 3 }) => {
      const excludeSet = new Set(exclude);
      const edges = db.prepare(
        'SELECT target_id, weight FROM memory_edges WHERE source_id = ? ORDER BY weight DESC'
      ).all(memory_id);
      const now = new Date().toISOString();
      const results = [];
      for (const edge of edges) {
        if (excludeSet.has(edge.target_id)) continue;
        const mem = db.prepare('SELECT * FROM memories WHERE id = ? AND (expires_at IS NULL OR expires_at > ?)').get(edge.target_id, now);
        if (mem) {
          results.push({ ...fmt(mem), edge_weight: +edge.weight.toFixed(3), bridge_from: memory_id });
          if (results.length >= limit) break;
        }
      }
      return {
        content: [{
          type: 'text',
          text: results.length
            ? JSON.stringify(results, null, 2)
            : `No neighbors found for memory ${memory_id}`,
        }],
      };
    }
  );

  // 9. hybrid_search — keyword + semantic, fused with RRF (best default search)
  mcp.tool(
    'hybrid_search',
    'Hybrid search: runs keyword (full-text) and semantic (vector) search together, fuses and re-ranks them with Reciprocal Rank Fusion. Best default search. Falls back to keyword-only if VOYAGE_API_KEY is unset.',
    {
      query:    z.string().min(1).describe('Search term or concept'),
      category: z.string().optional().describe('Filter by category'),
      limit:    z.number().int().min(1).max(20).optional().describe('Number of results (default 10)'),
      fallback_to_raw: z.boolean().optional().describe('Also run exact raw_events lookup for literal short terms / quoted text. Explicit only; default false.'),
      raw_limit: z.number().int().min(1).max(10).optional().describe('Max raw_events fallback results when fallback_to_raw=true'),
    },
    async ({ query, category, limit = 10, fallback_to_raw = false, raw_limit = 3 }) => {
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

      // 命中升温
      if (fused.length) {
        const heat = db.prepare('UPDATE memories SET activation_score = MIN(activation_score + 0.2, 8.0) WHERE id = ? AND protected = 0');
        for (const r of fused) heat.run(r.id);
      }

      let rawFallback = null;
      if (fallback_to_raw) {
        const { rows, terms } = rawEventSearch({
          query,
          mode: 'exact',
          channel: 'all',
          limit: raw_limit,
        });
        rawFallback = {
          mode: 'exact',
          terms,
          count: rows.length,
          results: rows.map(fmtRawEvent),
          note: '显式 fallback_to_raw=true 才会查 raw_events；不写正式记忆。',
        };
      }

      return {
        content: [{
          type: 'text',
          text: fallback_to_raw
            ? JSON.stringify({
                query,
                count: fused.length,
                results: fused,
                raw_fallback: rawFallback,
              }, null, 2)
            : (fused.length
                ? `Found ${fused.length} result(s) (keyword+semantic fused):\n${JSON.stringify(fused, null, 2)}`
                : `No results for: "${query}"`),
        }],
      };
    }
  );

  // 10. decay_activation — periodic cooldown for memory temperature
  mcp.tool(
    'decay_activation',
    'Run a global cooldown on all memory activation scores. Multiply by decay_factor (default 0.85). Memories below 0.01 are zeroed. Call this daily as a "dream pass".',
    {
      decay_factor: z.number().min(0.5).max(0.99).optional().describe('Decay multiplier (default 0.85)'),
    },
    async ({ decay_factor = 0.85 }) => {
      const before = db.prepare('SELECT COUNT(*) as c FROM memories WHERE protected = 0 AND activation_score > 0.01').get().c;
      db.prepare('UPDATE memories SET activation_score = activation_score * ? WHERE protected = 0 AND activation_score > 0.01').run(decay_factor);
      db.prepare('UPDATE memories SET activation_score = 0 WHERE protected = 0 AND activation_score <= 0.01 AND activation_score > 0').run();
      const after = db.prepare('SELECT COUNT(*) as c FROM memories WHERE protected = 0 AND activation_score > 0.01').get().c;
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            decay_factor,
            warm_before: before,
            warm_after: after,
            cooled: before - after,
          }, null, 2),
        }],
      };
    }
  );

  // 11. check_facts — Z-axis conflict detection
  mcp.tool(
    'check_facts',
    'Z-axis: list all fact_key groups. Shows current version and any conflicts (same fact_key with multiple non-superseded entries). Use to audit evolving facts.',
    {
      fact_key: z.string().optional().describe('Check a specific fact_key, or omit to scan all'),
    },
    async ({ fact_key }) => {
      cleanExpired();
      const now = new Date().toISOString();

      if (fact_key) {
        const rows = db.prepare(
          'SELECT * FROM memories WHERE fact_key = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY created_at DESC'
        ).all(fact_key, now).map(fmt);
        const current = rows.filter(r => !r.superseded_by);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              fact_key,
              total_versions: rows.length,
              current_count: current.length,
              conflict: current.length > 1,
              current: current,
              superseded: rows.filter(r => r.superseded_by),
            }, null, 2),
          }],
        };
      }

      // Scan all fact_keys
      const groups = db.prepare(
        "SELECT fact_key, COUNT(*) as total, SUM(CASE WHEN superseded_by IS NULL THEN 1 ELSE 0 END) as current_count FROM memories WHERE fact_key IS NOT NULL AND (expires_at IS NULL OR expires_at > ?) GROUP BY fact_key ORDER BY fact_key"
      ).all(now);

      const conflicts = groups.filter(g => g.current_count > 1);

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            total_fact_keys: groups.length,
            conflicts: conflicts.length,
            fact_keys: groups.map(g => ({
              fact_key: g.fact_key,
              versions: g.total,
              current: g.current_count,
              conflict: g.current_count > 1,
            })),
          }, null, 2),
        }],
      };
    }
  );

  // 12. merge_memories — lightweight hippocampus (LMC-5 Module 3)
  mcp.tool(
    'merge_memories',
    'Merge multiple memory fragments into one consolidated entry. Source memories are tagged "已合并" but NOT deleted. The merged result is saved as a new memory. Use when the same event is scattered across diary/daily/anchor fragments.',
    {
      ids:      z.array(z.string()).min(2).max(10).describe('IDs of memories to merge (2-10)'),
      content:  z.string().min(1).describe('The merged narrative text (you write the synthesis)'),
      category: z.string().optional().describe('Category for merged memory (default: same as first source)'),
      tags:     z.array(z.string()).optional().describe('Tags for merged memory'),
      source:   z.string().optional().describe('Source note'),
    },
    async ({ ids, content, category, tags, source }) => {
      const now = new Date().toISOString();
      const sources = [];
      for (const id of ids) {
        const row = db.prepare('SELECT * FROM memories WHERE id = ?').get(id);
        if (!row) return { content: [{ type: 'text', text: `Source memory not found: ${id}` }] };
        sources.push(row);
      }
      const protectedSources = sources.filter(rowIsProtected);
      if (protectedSources.length) {
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              error: 'protected_merge_refused',
              protected_ids: protectedSources.map(m => m.id),
              note: 'protected memories do not participate in merge_memories.',
            }, null, 2),
          }],
        };
      }

      const mergedCategory = category || sources[0].category;
      const hash = contentHash(content, mergedCategory);
      const dupe = db.prepare('SELECT * FROM memories WHERE content_hash = ? AND (expires_at IS NULL OR expires_at > ?)').get(hash, now);
      if (dupe) {
        return { content: [{ type: 'text', text: JSON.stringify({ duplicate: true, existing: fmt(dupe) }, null, 2) }] };
      }

      const mergedId = uuidv4();
      const expires_at = expiryFor(mergedCategory);
      const embedding = await getEmbedding(content);
      const protectedFlag = resolveProtectedFlag(mergedCategory, undefined, 0);

      const mergedTags = tags || [];
      if (!mergedTags.includes('merged')) mergedTags.push('merged');

      const sourceRef = ids.map(id => id.slice(0, 8)).join('+');
      const mergedSource = source || `merged from ${sourceRef}`;

      db.prepare(
        'INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at,embedding,pinned,content_hash,protected,evidence_raw_ids) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
      ).run(mergedId, content, mergedCategory, JSON.stringify(mergedTags), mergedSource, null, now, now, expires_at,
        embedding ? JSON.stringify(embedding) : null, 0, hash, protectedFlag, '[]');

      // Tag source memories as merged (don't delete them)
      const tagStmt = db.prepare('SELECT tags FROM memories WHERE id = ?');
      const updateStmt = db.prepare('UPDATE memories SET tags = ?, updated_at = ? WHERE id = ?');
      for (const id of ids) {
        const row = tagStmt.get(id);
        const existingTags = parseTags(row.tags);
        if (!existingTags.includes('已合并')) existingTags.push('已合并');
        updateStmt.run(JSON.stringify(existingTags), now, id);
      }

      // Create edges from merged → sources
      const edgeStmt = db.prepare('INSERT OR IGNORE INTO memory_edges (source_id, target_id, weight, created_at) VALUES (?, ?, ?, ?)');
      for (const id of ids) {
        edgeStmt.run(mergedId, id, 1.0, now);
        edgeStmt.run(id, mergedId, 1.0, now);
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            merged: { id: mergedId, category: mergedCategory },
            source_count: ids.length,
            sources_tagged: ids,
            note: `合并完成。${ids.length}条原始记忆已标记"已合并"，未删除。`,
          }, null, 2),
        }],
      };
    }
  );

  // 13. log_raw_event — append-only raw evidence journal
  mcp.tool(
    'log_raw_event',
    'Record one original dialogue event into raw_events evidence journal. Use linked_memory_ids to connect raw text to curated memories.',
    {
      session_id: z.string().optional().describe('Session id; keep the same id for one conversation'),
      source: z.string().optional().describe('Entrypoint or system source, e.g. kechat-light or telegram'),
      channel: z.enum(['cc', 'daily', 'intimate', 'private', 'group', 'normal']).default('normal').describe('Conversation channel'),
      role: z.enum(['user', 'assistant', 'system']).describe('Speaker role'),
      speaker: z.string().optional().describe('Human or agent speaker name/id, e.g. moon or ke'),
      content: z.string().min(1).describe('Original text content'),
      linked_memory_ids: z.array(z.string()).optional().describe('Curated memory ids linked to this raw event'),
      metadata: z.record(z.unknown()).optional().describe('Structured provenance metadata'),
    },
    async ({ session_id = '', source = '', channel = 'normal', role, speaker = '', content, linked_memory_ids = [], metadata = {} }) => {
      const id = uuidv4();
      const now = new Date().toISOString();
      db.prepare(
        'INSERT INTO raw_events (id,session_id,source,channel,role,speaker,content,timestamp,linked_memory_ids,metadata) VALUES (?,?,?,?,?,?,?,?,?,?)'
      ).run(id, session_id, source, channel, role, speaker, content, now, JSON.stringify(linked_memory_ids), JSON.stringify(metadata || {}));
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({ id, timestamp: now, source, channel, role, speaker }, null, 2),
        }],
      };
    }
  );

  // 14. search_raw_events — keyword search original evidence
  mcp.tool(
    'search_raw_events',
    'Keyword-search raw_events and return original dialogue snippets.',
    {
      query: z.string().min(1).describe('Search term'),
      channel: z.enum(['cc', 'daily', 'intimate', 'private', 'group', 'normal', 'all']).default('all').describe('Filter by channel, or all'),
      source: z.string().optional().describe('Optional exact source filter'),
      speaker: z.string().optional().describe('Optional exact speaker filter'),
      mode: z.enum(['fuzzy', 'exact']).default('fuzzy').describe('fuzzy = current LIKE search; exact = literal quoted phrase / whole ASCII word / exact CJK substring'),
      limit: z.number().int().min(1).max(50).default(20),
    },
    async ({ query, channel = 'all', source = '', speaker = '', mode = 'fuzzy', limit = 20 }) => {
      const { rows, terms } = rawEventSearch({ query, channel, source, speaker, mode, limit });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            mode,
            terms: mode === 'exact' ? terms : undefined,
            results: rows.map(fmtRawEvent),
            count: rows.length,
          }, null, 2),
        }],
      };
    }
  );

  // 15. get_evidence — curated memory → linked raw events
  mcp.tool(
    'get_evidence',
    'Return raw_events linked from one curated memory via evidence_raw_ids.',
    {
      memory_id: z.string().describe('Curated memory id'),
    },
    async ({ memory_id }) => {
      const mem = db.prepare('SELECT evidence_raw_ids FROM memories WHERE id = ?').get(memory_id);
      if (!mem) return { content: [{ type: 'text', text: JSON.stringify({ error: 'memory not found' }, null, 2) }] };
      const ids = parseArrayField(mem.evidence_raw_ids);
      if (!ids.length) return { content: [{ type: 'text', text: JSON.stringify({ evidence: [], count: 0 }, null, 2) }] };
      const placeholders = ids.map(() => '?').join(',');
      const rows = db.prepare(`SELECT * FROM raw_events WHERE id IN (${placeholders}) ORDER BY timestamp ASC`).all(...ids);
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({ evidence: rows.map(fmtRawEvent), count: rows.length }, null, 2),
        }],
      };
    }
  );

  // 16. propose_memory_candidates — raw_events -> reviewed candidate proposals only
  mcp.tool(
    'propose_memory_candidates',
    'Propose memory candidates from raw_events. Does not write memories. dry_run=true returns proposals without saving candidates.',
    {
      since_hours: z.number().min(1).max(168).default(24).describe('Look back this many hours'),
      source: z.string().default('all').describe('Filter source, e.g. kechat-light/telegram/all'),
      channel: z.enum(['cc', 'daily', 'intimate', 'private', 'group', 'normal', 'all']).default('all').describe('Filter channel, or all'),
      limit: z.number().int().min(1).max(50).default(20),
      dry_run: z.boolean().default(true).describe('true returns proposals only; false writes memory_candidates'),
    },
    async ({ since_hours = 24, source = 'all', channel = 'all', limit = 20, dry_run = true }) => {
      const now = new Date().toISOString();
      markStaleCandidates(now);
      const since = new Date(Date.now() - Number(since_hours) * 60 * 60 * 1000).toISOString();
      let sql = "SELECT * FROM raw_events WHERE timestamp >= ? AND role = 'user'";
      const params = [since];
      if (source && source !== 'all') { sql += ' AND source = ?'; params.push(source); }
      if (channel !== 'all') { sql += ' AND channel = ?'; params.push(channel); }
      sql += ' ORDER BY timestamp DESC LIMIT ?';
      params.push(Math.max(limit * 5, limit));

      const rows = db.prepare(sql).all(...params);
      const proposals = [];
      const seen = new Set();
      const grouped = new Map();
      for (const row of rows) {
        const classification = classifyRawEvent(row);
        if (!classification) continue;
        const fingerprint = candidateFingerprint(row);
        const group = grouped.get(fingerprint);
        if (group) {
          group.raw_event_ids.push(row.id);
          group.source = group.source || row.source || '';
          group.channel = group.channel || row.channel || '';
          group.speaker = group.speaker || row.speaker || '';
          group.confidence = Math.max(group.confidence, classification.confidence);
          continue;
        }
        grouped.set(fingerprint, {
          row,
          raw_event_ids: [row.id],
          dedupe_key: fingerprint,
          source: row.source || '',
          channel: row.channel || '',
          speaker: row.speaker || '',
          classification,
          confidence: classification.confidence,
        });
      }

      const insert = db.prepare(
        'INSERT OR IGNORE INTO memory_candidates (id,raw_event_ids,dedupe_key,source,channel,speaker,summary,suggested_category,reason,confidence,status,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
      );
      for (const group of grouped.values()) {
        if (proposals.length >= limit) break;
        if (seen.has(group.dedupe_key)) continue;
        seen.add(group.dedupe_key);
        const existing = db.prepare('SELECT * FROM memory_candidates WHERE dedupe_key = ?').get(group.dedupe_key);
        if (existing || candidateHasExistingRawEvents(group.raw_event_ids)) continue;
        const summary = buildCandidateSummary(group.row, group.classification.category);
        const candidate = {
          id: uuidv4(),
          raw_event_ids: group.raw_event_ids,
          dedupe_key: group.dedupe_key,
          source: group.source,
          channel: group.channel,
          speaker: group.speaker,
          summary,
          suggested_category: group.classification.category,
          suggested_category_label: candidateCategoryLabel(group.classification.category),
          reason: group.classification.reason,
          confidence: group.confidence,
          status: 'pending',
          created_at: now,
          updated_at: now,
          reviewed_at: null,
          review_note: null,
          expires_at: candidateExpiresAt(now, 7),
        };
        proposals.push(candidate);
        if (!dry_run) {
          insert.run(
            candidate.id,
            JSON.stringify(candidate.raw_event_ids),
            candidate.dedupe_key,
            candidate.source,
            candidate.channel,
            candidate.speaker,
            candidate.summary,
            candidate.suggested_category,
            candidate.reason,
            candidate.confidence,
            candidate.status,
            candidate.created_at,
            candidate.updated_at,
            candidate.expires_at
          );
        }
      }
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            dry_run,
            proposed_count: proposals.length,
            candidates: proposals,
            note: dry_run ? 'dry_run=true: no candidates were written.' : 'Candidates were written to memory_candidates; memories were not written.',
          }, null, 2),
        }],
      };
    }
  );

  // 17. list_memory_candidates — review queue
  mcp.tool(
    'list_memory_candidates',
    'List reviewed or pending memory candidates. Does not write memories.',
    {
      status: z.enum(['pending', 'accepted', 'rejected', 'merged', 'stale', 'all']).default('pending'),
      limit: z.number().int().min(1).max(100).default(20),
    },
    async ({ status = 'pending', limit = 20 }) => {
      const now = new Date().toISOString();
      markStaleCandidates(now);
      const rows = status === 'all'
        ? db.prepare('SELECT * FROM memory_candidates ORDER BY created_at DESC LIMIT ?').all(limit)
        : db.prepare('SELECT * FROM memory_candidates WHERE status = ? ORDER BY created_at DESC LIMIT ?').all(status, limit);
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({ candidates: rows.map(fmtMemoryCandidate), count: rows.length }, null, 2),
        }],
      };
    }
  );

  // 18. review_memory_candidate — mark candidate status only, never writes memories
  mcp.tool(
    'review_memory_candidate',
    'Review a memory candidate by changing its status. Does not write memories.',
    {
      id: z.string().describe('memory_candidates id'),
      status: z.enum(['pending', 'accepted', 'rejected', 'merged', 'stale']).describe('New review status'),
      review_note: z.string().optional().describe('Optional review note'),
    },
    async ({ id, status, review_note = '' }) => {
      if (!VALID_CANDIDATE_STATUS.has(status)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'invalid status' }, null, 2) }] };
      }
      const row = db.prepare('SELECT * FROM memory_candidates WHERE id = ?').get(id);
      if (!row) return { content: [{ type: 'text', text: JSON.stringify({ error: 'candidate not found' }, null, 2) }] };
      const now = new Date().toISOString();
      const reviewedAt = status === 'pending' ? null : now;
      db.prepare('UPDATE memory_candidates SET status=?, review_note=?, reviewed_at=?, updated_at=? WHERE id=?')
        .run(status, review_note || null, reviewedAt, now, id);
      const updated = db.prepare('SELECT * FROM memory_candidates WHERE id = ?').get(id);
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({ candidate: fmtMemoryCandidate(updated), note: 'Candidate status updated; memories were not written.' }, null, 2),
        }],
      };
    }
  );

  // 19. batch_review_memory_candidates — batch mark candidate status only, never writes memories
  mcp.tool(
    'batch_review_memory_candidates',
    'Review multiple memory candidates by changing their status. Does not write memories.',
    {
      ids: z.array(z.string()).min(1).max(100).describe('memory_candidates ids'),
      status: z.enum(['pending', 'accepted', 'rejected', 'merged', 'stale']).describe('New review status'),
      review_note: z.string().optional().describe('Optional review note applied to all ids'),
    },
    async ({ ids, status, review_note = '' }) => {
      if (!VALID_CANDIDATE_STATUS.has(status)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'invalid status' }, null, 2) }] };
      }
      const uniqueIds = [...new Set(ids.map(id => String(id || '').trim()).filter(Boolean))];
      const now = new Date().toISOString();
      const reviewedAt = status === 'pending' ? null : now;
      const select = db.prepare('SELECT id FROM memory_candidates WHERE id = ?');
      const update = db.prepare('UPDATE memory_candidates SET status=?, review_note=?, reviewed_at=?, updated_at=? WHERE id=?');
      const updated = [];
      const missing = [];
      for (const id of uniqueIds) {
        const row = select.get(id);
        if (!row) {
          missing.push(id);
          continue;
        }
        update.run(status, review_note || null, reviewedAt, now, id);
        updated.push(id);
      }
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({ updated_count: updated.length, missing_count: missing.length, updated_ids: updated, missing_ids: missing, status, note: 'Candidate statuses updated; memories were not written.' }, null, 2),
        }],
      };
    }
  );

  // 20. somatic_ignite — shared short-lived body-state trigger
  mcp.tool(
    'somatic_ignite',
    'Shared somatic state: record a short-lived body sensation trigger for ke. Caller must parse text and pass labels; this server stores/decays state only and does not write ordinary memories.',
    {
      target:       z.string().optional().describe('Target body. v1 only supports ke.'),
      actor:        z.string().optional().describe('Who caused the trigger, default moon'),
      source:       z.string().min(1).describe('Entry source, e.g. telegram/cc-local/wechat'),
      channel:      z.string().optional().describe('Optional channel/group/private label'),
      modality:     z.enum(['touch', 'smell', 'taste', 'sound']).describe('Somatic modality'),
      action:       z.string().optional().describe('Parsed action, e.g. 捏/抱/闻到'),
      zone:         z.string().optional().describe('Parsed body zone or cue zone, e.g. 脸/耳后/雨味'),
      labels:       z.array(z.string()).optional().describe('Caller-generated labels, e.g. ["脸","指尖","短促","温","有肉感"]'),
      intensity:    z.number().min(0.05).max(2).optional().describe('Trigger intensity, default 1.0'),
      valence:      z.string().optional().describe('Optional emotional valence'),
      text_excerpt: z.string().max(300).optional().describe('Short source excerpt only; do not pass full chat logs'),
      half_life_sec:z.number().int().min(60).max(86400).optional().describe('Override decay half-life'),
      ttl_sec:      z.number().int().min(60).max(604800).optional().describe('Override hard expiry'),
    },
    async ({ target, actor = 'moon', source, channel, modality, action, zone, labels = [], intensity = 1.0, valence, text_excerpt, half_life_sec, ttl_sec }) => {
      try {
        const resolvedTarget = ensureSomaticTarget(target);
        const resolvedModality = ensureSomaticModality(modality);
        const now = new Date().toISOString();
        const halfLife = somaticHalfLife(resolvedModality, half_life_sec);
        const expiresAt = somaticExpiresAt(now, halfLife, ttl_sec);
        const cleanLabels = labels.map(v => String(v || '').trim()).filter(Boolean).slice(0, 12);
        const label = cleanLabels.length ? cleanLabels.join('·') : [zone, action, resolvedModality].filter(Boolean).join('·');
        if (!label) throw new Error('labels, zone, or action is required');

        const eventId = uuidv4();
        const stateId = somaticStateId(resolvedTarget, resolvedModality, zone || '', label);
        db.exec('BEGIN IMMEDIATE');
        try {
          db.prepare(
            'INSERT INTO somatic_events (id,actor,target,source,channel,modality,action,zone,labels,intensity,valence,text_excerpt,created_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
          ).run(eventId, actor, resolvedTarget, source, channel || null, resolvedModality, action || null, zone || null,
            JSON.stringify(cleanLabels), Number(intensity), valence || null, text_excerpt || null, now, expiresAt);

          const existing = db.prepare('SELECT * FROM somatic_state WHERE id = ?').get(stateId);
          const currentStrength = existing ? somaticDecayedStrength(existing, now) : 0;
          const nextStrength = Math.min(SOMATIC_MAX_STRENGTH, currentStrength + Number(intensity));
          if (existing) {
            db.prepare(
              'UPDATE somatic_state SET strength=?, half_life_sec=?, last_event_id=?, updated_at=?, expires_at=? WHERE id=?'
            ).run(nextStrength, halfLife, eventId, now, expiresAt, stateId);
          } else {
            db.prepare(
              'INSERT INTO somatic_state (id,target,modality,zone,label,strength,half_life_sec,last_event_id,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?)'
            ).run(stateId, resolvedTarget, resolvedModality, zone || null, label, nextStrength, halfLife, eventId, now, expiresAt);
          }
          db.exec('COMMIT');
        } catch (err) {
          db.exec('ROLLBACK');
          throw err;
        }

        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              event_id: eventId,
              state_id: stateId,
              target: resolvedTarget,
              modality: resolvedModality,
              zone: zone || null,
              label,
              labels: cleanLabels,
              strength: +Math.min(SOMATIC_MAX_STRENGTH, Number(intensity)).toFixed(4),
              half_life_sec: halfLife,
              expires_at: expiresAt,
              state_updated: true,
            }, null, 2),
          }],
        };
      } catch (err) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: err.message }, null, 2) }] };
      }
    }
  );

  // 21. somatic_snapshot — read decayed shared body-state before a reply
  mcp.tool(
    'somatic_snapshot',
    'Shared somatic state: read current decayed body sensations for ke. Use before replying to inject prompt_text. Does not write ordinary memories.',
    {
      target:              z.string().optional().describe('Target body. v1 only supports ke.'),
      source:              z.string().optional().describe('Entry source requesting the snapshot'),
      include_prompt_text: z.boolean().optional().describe('Include Chinese prompt block for system prompt injection'),
      min_strength:        z.number().min(0.01).max(1).optional().describe('Minimum active strength, default 0.12'),
    },
    async ({ target, source, include_prompt_text = true, min_strength = SOMATIC_MIN_STRENGTH }) => {
      try {
        const resolvedTarget = ensureSomaticTarget(target);
        const states = activeSomaticStates(resolvedTarget, min_strength).map(row => {
          const hooks = matchingSomaticHooks(row);
          return {
            id: row.id,
            target: row.target,
            modality: row.modality,
            zone: row.zone || null,
            label: row.label,
            strength: row.strength,
            half_life_sec: Number(row.half_life_sec),
            last_event_id: row.last_event_id || null,
            updated_at: row.updated_at,
            expires_at: row.expires_at,
            hooks,
          };
        });
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              target: resolvedTarget,
              source: source || null,
              active: states.length > 0,
              count: states.length,
              prompt_text: include_prompt_text ? somaticPromptText(states) : '',
              items: states,
            }, null, 2),
          }],
        };
      } catch (err) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: err.message }, null, 2) }] };
      }
    }
  );

  // 22. somatic_clear — manually clear stuck or unwanted body-state
  mcp.tool(
    'somatic_clear',
    'Shared somatic state: clear current short-lived body sensations. Use for stuck states; does not delete ordinary memories.',
    {
      target:       z.string().optional().describe('Target body. v1 only supports ke.'),
      modality:     z.enum(['touch', 'smell', 'taste', 'sound']).optional().describe('Optional modality filter'),
      zone:         z.string().optional().describe('Optional zone filter'),
      label:        z.string().optional().describe('Optional exact label filter'),
      clear_events: z.boolean().optional().describe('Also delete short-lived matching events, default false'),
    },
    async ({ target, modality, zone, label, clear_events = false }) => {
      try {
        const resolvedTarget = ensureSomaticTarget(target);
        let sql = 'DELETE FROM somatic_state WHERE target = ?';
        const p = [resolvedTarget];
        if (modality) { sql += ' AND modality = ?'; p.push(ensureSomaticModality(modality)); }
        if (zone !== undefined) { sql += ' AND zone = ?'; p.push(zone || null); }
        if (label) { sql += ' AND label = ?'; p.push(label); }
        const stateResult = db.prepare(sql).run(...p);

        let eventChanges = 0;
        if (clear_events) {
          let eSql = 'DELETE FROM somatic_events WHERE target = ?';
          const ep = [resolvedTarget];
          if (modality) { eSql += ' AND modality = ?'; ep.push(ensureSomaticModality(modality)); }
          if (zone !== undefined) { eSql += ' AND zone = ?'; ep.push(zone || null); }
          eventChanges = db.prepare(eSql).run(...ep).changes;
        }

        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              target: resolvedTarget,
              cleared_state: stateResult.changes,
              cleared_events: eventChanges,
            }, null, 2),
          }],
        };
      } catch (err) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: err.message }, null, 2) }] };
      }
    }
  );

  // 23. somatic_hook_upsert — maintain long-lived Proust hooks
  mcp.tool(
    'somatic_hook_upsert',
    'Shared somatic state: create/update a long-lived Proust hook. Prefer fact_key over memory_id; this is the only somatic table backed up long-term.',
    {
      target:    z.string().optional().describe('Target body. v1 only supports ke.'),
      modality:  z.enum(['touch', 'smell', 'taste', 'sound']).describe('Cue modality'),
      cue:       z.string().min(1).describe('Stable cue, e.g. 雨味/洗发水/耳边低声'),
      fact_key:  z.string().optional().describe('Preferred stable reference into ordinary memories/facts'),
      memory_id: z.string().optional().describe('Optional auxiliary memory id; fact_key is preferred because merge may change ids'),
      note:      z.string().optional().describe('Human-readable hook note'),
      weight:    z.number().min(0.05).max(5).optional().describe('Hook weight, default 1.0'),
    },
    async ({ target, modality, cue, fact_key, memory_id, note, weight = 1.0 }) => {
      try {
        const resolvedTarget = ensureSomaticTarget(target);
        const resolvedModality = ensureSomaticModality(modality);
        const now = new Date().toISOString();
        const cleanCue = String(cue).trim();
        const existing = db.prepare('SELECT * FROM somatic_hooks WHERE target = ? AND modality = ? AND cue = ?')
          .get(resolvedTarget, resolvedModality, cleanCue);
        let id = existing?.id || uuidv4();
        if (existing) {
          db.prepare(
            'UPDATE somatic_hooks SET fact_key=?, memory_id=?, note=?, weight=?, updated_at=? WHERE id=?'
          ).run(fact_key || null, memory_id || null, note || null, Number(weight), now, id);
        } else {
          db.prepare(
            'INSERT INTO somatic_hooks (id,target,modality,cue,fact_key,memory_id,note,weight,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)'
          ).run(id, resolvedTarget, resolvedModality, cleanCue, fact_key || null, memory_id || null, note || null, Number(weight), now, now);
        }
        const row = db.prepare('SELECT * FROM somatic_hooks WHERE id = ?').get(id);
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              upserted: fmtSomaticHook(row),
              note: fact_key ? 'fact_key is the primary stable reference.' : 'No fact_key set; consider adding one for merge-safe linkage.',
            }, null, 2),
          }],
        };
      } catch (err) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: err.message }, null, 2) }] };
      }
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
  const { content, category, tags = [], source = '', mood = null, pinned = false, fact_key = null, protected: protectedArg, evidence_raw_ids = [] } = req.body;
  if (!content || !category) return res.status(400).json({ error: 'content and category required' });
  const hash = contentHash(content, category);
  const dupe = db.prepare('SELECT * FROM memories WHERE content_hash = ? AND (expires_at IS NULL OR expires_at > ?)').get(hash, new Date().toISOString());
  if (dupe) return res.json({ id: dupe.id, duplicate: true, message: 'Duplicate content, skipped' });
  const id = uuidv4(), now = new Date().toISOString();
  const expires_at = pinned ? null : expiryFor(category);
  const protectedFlag = resolveProtectedFlag(category, protectedArg, 0);
  const evidenceIds = JSON.stringify(evidence_raw_ids);
  const embedding = await getEmbedding(content);
  const related = findRelated(embedding, id);

  let oldFactRows = [];
  if (fact_key) {
    oldFactRows = db.prepare('SELECT * FROM memories WHERE fact_key = ? AND id != ? AND superseded_by IS NULL').all(fact_key, id);
    const blocked = oldFactRows.filter(rowIsProtected);
    if (blocked.length) {
      return res.status(409).json({
        error: 'protected_fact_conflict',
        fact_key,
        protected_ids: blocked.map(m => m.id),
        message: 'Refusing to auto-supersede protected memories.',
      });
    }
  }

  db.prepare('INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at,embedding,pinned,content_hash,fact_key,protected,evidence_raw_ids) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
    .run(id, content, category, JSON.stringify(tags), source, mood, now, now, expires_at,
      embedding ? JSON.stringify(embedding) : null, pinned ? 1 : 0, hash, fact_key, protectedFlag, evidenceIds);
  // Z-axis: auto-supersede old versions
  let superseded = [];
  if (fact_key) {
    const superStmt = db.prepare('UPDATE memories SET superseded_by = ?, updated_at = ? WHERE id = ?');
    for (const o of oldFactRows) { superStmt.run(id, now, o.id); superseded.push(o.id); }
  }
  if (related.length) {
    const edgeStmt = db.prepare('INSERT OR IGNORE INTO memory_edges (source_id, target_id, weight, created_at) VALUES (?, ?, ?, ?)');
    for (const r of related) {
      edgeStmt.run(id, r.id, r.similarity ?? 0.5, now);
      edgeStmt.run(r.id, id, r.similarity ?? 0.5, now);
    }
  }
  res.json({ id, message: 'Memory saved', fact_key, superseded_ids: superseded, related_memories: related, edges_created: related.length * 2 });
});

app.put('/api/memories/:id', auth, (req, res) => {
  const row = db.prepare('SELECT * FROM memories WHERE id = ?').get(req.params.id);
  if (!row) return res.status(404).json({ error: 'Not found' });
  const { content, category, tags, source, mood, pinned, fact_key, protected: protectedArg, evidence_raw_ids } = req.body;
  const newCategory = category ?? row.category;
  const newPinned = pinned !== undefined ? (pinned ? 1 : 0) : row.pinned;
  const newProtected = resolveProtectedFlag(newCategory, protectedArg, row.protected);
  const newEvidenceIds = evidence_raw_ids !== undefined ? JSON.stringify(evidence_raw_ids) : row.evidence_raw_ids;
  let expires_at = row.expires_at;
  const categoryChanged = category !== undefined && category !== row.category;
  const pinnedChanged = pinned !== undefined && (pinned ? 1 : 0) !== row.pinned;
  if (categoryChanged || pinnedChanged) {
    expires_at = newPinned ? null : expiryFor(newCategory);
  }
  const newFactKey = fact_key !== undefined ? fact_key : row.fact_key;
  db.prepare('UPDATE memories SET content=?,category=?,tags=?,source=?,mood=?,pinned=?,updated_at=?,expires_at=?,content_hash=?,fact_key=?,protected=?,evidence_raw_ids=? WHERE id=?').run(
    content ?? row.content,
    newCategory,
    tags !== undefined ? JSON.stringify(tags) : row.tags,
    source ?? row.source,
    mood !== undefined ? mood : row.mood,
    newPinned,
    new Date().toISOString(),
    expires_at,
    contentHash(content ?? row.content, newCategory),
    newFactKey ?? null,
    newProtected,
    newEvidenceIds,
    req.params.id
  );
  res.json({ message: 'Updated' });
});

app.delete('/api/memories/:id', auth, (req, res) => {
  const r = db.prepare('DELETE FROM memories WHERE id = ?').run(req.params.id);
  if (r.changes) {
    db.prepare('DELETE FROM memory_edges WHERE source_id = ? OR target_id = ?').run(req.params.id, req.params.id);
  }
  res.json({ deleted: r.changes > 0 });
});

app.get('/api/stats', auth, (_req, res) => {
  cleanExpired();
  const total = db.prepare('SELECT COUNT(*) as c FROM memories').get().c;
  const byCat = db.prepare('SELECT category, COUNT(*) as c FROM memories GROUP BY category').all();
  res.json({ total, by_category: Object.fromEntries(byCat.map(r => [r.category, r.c])) });
});

// Z-axis fact check endpoint
app.get('/api/facts', auth, (req, res) => {
  cleanExpired();
  const now = new Date().toISOString();
  const { fact_key } = req.query;
  if (fact_key) {
    const rows = db.prepare(
      'SELECT * FROM memories WHERE fact_key = ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY created_at DESC'
    ).all(fact_key, now).map(fmt);
    const current = rows.filter(r => !r.superseded_by);
    return res.json({ fact_key, total_versions: rows.length, current_count: current.length, conflict: current.length > 1, current, superseded: rows.filter(r => r.superseded_by) });
  }
  const groups = db.prepare(
    "SELECT fact_key, COUNT(*) as total, SUM(CASE WHEN superseded_by IS NULL THEN 1 ELSE 0 END) as current_count FROM memories WHERE fact_key IS NOT NULL AND (expires_at IS NULL OR expires_at > ?) GROUP BY fact_key ORDER BY fact_key"
  ).all(now);
  res.json({ total_fact_keys: groups.length, conflicts: groups.filter(g => g.current_count > 1).length, fact_keys: groups });
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

    // 命中升温
    if (fused.length) {
      const heat = db.prepare('UPDATE memories SET activation_score = MIN(activation_score + 0.2, 8.0) WHERE id = ? AND protected = 0');
      for (const r of fused) heat.run(r.id);
    }

    res.json({ results: fused, count: fused.length, semantic_enabled: !!embedding });
  } catch (err) {
    console.error('[api/search/hybrid] error:', err);
    res.status(500).json({ error: 'Internal error' });
  }
});

// ── GitHub Backup ─────────────────────────────────────────────────────────────

const BACKUP_REPO  = process.env.BACKUP_REPO  || 'lanmoyinyue/mcp-memory-server';
const BACKUP_PATH  = 'backups/memories.jsonl';
const EDGES_BACKUP_PATH = 'backups/memory_edges.jsonl';
const RAW_EVENTS_BACKUP_PATH = 'backups/raw_events.jsonl';
const MEMORY_CANDIDATES_BACKUP_PATH = 'backups/memory_candidates.jsonl';
const SOMATIC_HOOKS_BACKUP_PATH = 'backups/somatic_hooks.jsonl';
const BACKUP_TOKEN = process.env.BACKUP_GITHUB_TOKEN;
const BACKUP_INCLUDE_INTIMATE = ['1', 'true', 'yes', 'on'].includes(String(process.env.BACKUP_INCLUDE_INTIMATE || '').toLowerCase());

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

    const edgeRows = db.prepare('SELECT * FROM memory_edges').all();
    const edgeContentB64 = Buffer.from(edgeRows.map(r => JSON.stringify(r)).join('\n')).toString('base64');
    let edgeSha = null;
    const existingEdges = await ghRequest('GET', EDGES_BACKUP_PATH);
    if (existingEdges.ok) edgeSha = (await existingEdges.json()).sha;
    await ghRequest('PUT', EDGES_BACKUP_PATH, {
      message: `backup: ${new Date().toISOString().slice(0, 10)} (${edgeRows.length} memory edges) [zeabur skip]`,
      content: edgeContentB64,
      ...(edgeSha ? { sha: edgeSha } : {}),
    });

    const rawSql = BACKUP_INCLUDE_INTIMATE
      ? 'SELECT * FROM raw_events ORDER BY timestamp DESC'
      : "SELECT * FROM raw_events WHERE channel != 'intimate' ORDER BY timestamp DESC";
    const rawRows = db.prepare(rawSql).all().map(fmtRawEvent);
    const rawContentB64 = Buffer.from(rawRows.map(r => JSON.stringify(r)).join('\n')).toString('base64');
    let rawSha = null;
    const existingRaw = await ghRequest('GET', RAW_EVENTS_BACKUP_PATH);
    if (existingRaw.ok) rawSha = (await existingRaw.json()).sha;
    await ghRequest('PUT', RAW_EVENTS_BACKUP_PATH, {
      message: `backup: ${new Date().toISOString().slice(0, 10)} (${rawRows.length} raw events) [zeabur skip]`,
      content: rawContentB64,
      ...(rawSha ? { sha: rawSha } : {}),
    });

    const candidateSql = BACKUP_INCLUDE_INTIMATE
      ? 'SELECT * FROM memory_candidates ORDER BY created_at DESC'
      : "SELECT * FROM memory_candidates WHERE channel != 'intimate' ORDER BY created_at DESC";
    const candidateRows = db.prepare(candidateSql).all().map(fmtMemoryCandidate);
    const candidateContentB64 = Buffer.from(candidateRows.map(r => JSON.stringify(r)).join('\n')).toString('base64');
    let candidateSha = null;
    const existingCandidates = await ghRequest('GET', MEMORY_CANDIDATES_BACKUP_PATH);
    if (existingCandidates.ok) candidateSha = (await existingCandidates.json()).sha;
    await ghRequest('PUT', MEMORY_CANDIDATES_BACKUP_PATH, {
      message: `backup: ${new Date().toISOString().slice(0, 10)} (${candidateRows.length} memory candidates) [zeabur skip]`,
      content: candidateContentB64,
      ...(candidateSha ? { sha: candidateSha } : {}),
    });

    const hookRows = db.prepare('SELECT * FROM somatic_hooks').all().map(fmtSomaticHook);
    const hookContentB64 = Buffer.from(hookRows.map(r => JSON.stringify(r)).join('\n')).toString('base64');
    let hookSha = null;
    const existingHooks = await ghRequest('GET', SOMATIC_HOOKS_BACKUP_PATH);
    if (existingHooks.ok) hookSha = (await existingHooks.json()).sha;
    await ghRequest('PUT', SOMATIC_HOOKS_BACKUP_PATH, {
      message: `backup: ${new Date().toISOString().slice(0, 10)} (${hookRows.length} somatic hooks) [zeabur skip]`,
      content: hookContentB64,
      ...(hookSha ? { sha: hookSha } : {}),
    });

    console.log(`[backup] ${rows.length} memories, ${edgeRows.length} edges, ${rawRows.length} raw events (${BACKUP_INCLUDE_INTIMATE ? 'including' : 'excluding'} intimate), ${candidateRows.length} memory candidates, and ${hookRows.length} somatic hooks backed up to GitHub`);
  } catch (e) {
    console.error('[backup] failed:', e.message);
  }
}

async function restoreSomaticHooksIfEmpty() {
  if (!BACKUP_TOKEN) return;
  const count = db.prepare('SELECT COUNT(*) as c FROM somatic_hooks').get().c;
  if (count > 0) {
    console.log(`[backup] DB has ${count} somatic hooks — skipping somatic hook restore`);
    return;
  }
  try {
    const r = await ghRequest('GET', SOMATIC_HOOKS_BACKUP_PATH);
    if (!r.ok) return;
    const data = await r.json();
    const content = Buffer.from(data.content.replace(/\n/g, ''), 'base64').toString('utf-8');
    const lines = content.split('\n').filter(l => l.trim());
    const stmt = db.prepare(
      'INSERT OR IGNORE INTO somatic_hooks (id,target,modality,cue,fact_key,memory_id,note,weight,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)'
    );
    let restored = 0;
    for (const line of lines) {
      const h = JSON.parse(line);
      stmt.run(h.id, h.target || 'ke', h.modality, h.cue, h.fact_key || null, h.memory_id || null, h.note || null, Number(h.weight || 1), h.created_at, h.updated_at);
      restored++;
    }
    console.log(`[backup] auto-restored ${restored} somatic hooks from GitHub`);
  } catch (e) {
    console.error('[backup] somatic hook restore failed:', e.message);
  }
}

async function restoreRawEventsIfEmpty() {
  if (!BACKUP_TOKEN) return;
  const count = db.prepare('SELECT COUNT(*) as c FROM raw_events').get().c;
  if (count > 0) {
    console.log(`[backup] DB has ${count} raw events — skipping raw event restore`);
    return;
  }
  try {
    const r = await ghRequest('GET', RAW_EVENTS_BACKUP_PATH);
    if (!r.ok) return;
    const data = await r.json();
    const content = Buffer.from(data.content.replace(/\n/g, ''), 'base64').toString('utf-8');
    const lines = content.split('\n').filter(l => l.trim());
    const stmt = db.prepare(
      'INSERT OR IGNORE INTO raw_events (id,session_id,source,channel,role,speaker,content,timestamp,linked_memory_ids,metadata) VALUES (?,?,?,?,?,?,?,?,?,?)'
    );
    let restored = 0;
    for (const line of lines) {
      const e = JSON.parse(line);
      const linked = Array.isArray(e.linked_memory_ids) ? e.linked_memory_ids : parseArrayField(e.linked_memory_ids);
      const metadata = parseObjectField(e.metadata);
      stmt.run(e.id, e.session_id || '', e.source || '', e.channel || 'cc', e.role || 'user', e.speaker || '', e.content, e.timestamp, JSON.stringify(linked), JSON.stringify(metadata));
      restored++;
    }
    console.log(`[backup] auto-restored ${restored} raw events from GitHub`);
  } catch (e) {
    console.error('[backup] raw event restore failed:', e.message);
  }
}

async function restoreMemoryCandidatesIfEmpty() {
  if (!BACKUP_TOKEN) return;
  const count = db.prepare('SELECT COUNT(*) as c FROM memory_candidates').get().c;
  if (count > 0) {
    console.log(`[backup] DB has ${count} memory candidates — skipping candidate restore`);
    return;
  }
  try {
    const r = await ghRequest('GET', MEMORY_CANDIDATES_BACKUP_PATH);
    if (!r.ok) return;
    const data = await r.json();
    const content = Buffer.from(data.content.replace(/\n/g, ''), 'base64').toString('utf-8');
    const lines = content.split('\n').filter(l => l.trim());
    const stmt = db.prepare(
      'INSERT OR IGNORE INTO memory_candidates (id,raw_event_ids,dedupe_key,source,channel,speaker,summary,suggested_category,reason,confidence,status,created_at,updated_at,reviewed_at,review_note,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    );
    let restored = 0;
    for (const line of lines) {
      const c = JSON.parse(line);
      const rawIds = Array.isArray(c.raw_event_ids) ? c.raw_event_ids : parseArrayField(c.raw_event_ids);
      const now = new Date().toISOString();
      const status = VALID_CANDIDATE_STATUS.has(c.status) ? c.status : 'pending';
      stmt.run(
        c.id || uuidv4(),
        JSON.stringify(rawIds),
        c.dedupe_key || candidateDedupeKey(rawIds),
        c.source || '',
        c.channel || '',
        c.speaker || '',
        c.summary || '',
        c.suggested_category || 'daily',
        c.reason || '',
        Number(c.confidence || 0),
        status,
        c.created_at || now,
        c.updated_at || c.created_at || now,
        c.reviewed_at || null,
        c.review_note || null,
        c.expires_at || candidateExpiresAt(now, 7)
      );
      restored++;
    }
    console.log(`[backup] auto-restored ${restored} memory candidates from GitHub`);
  } catch (e) {
    console.error('[backup] memory candidate restore failed:', e.message);
  }
}

async function autoRestore() {
  if (!BACKUP_TOKEN) return;
  await restoreSomaticHooksIfEmpty();
  await restoreRawEventsIfEmpty();
  await restoreMemoryCandidatesIfEmpty();
  // Disaster recovery only: restore when the DB is EMPTY. Restoring into a
  // populated DB would resurrect deliberately deleted/expired memories.
  const count = db.prepare('SELECT COUNT(*) as c FROM memories').get().c;
  if (count > 0) {
    console.log(`[backup] DB has ${count} memories — skipping auto-restore`);
    return;
  }
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
        db.prepare('INSERT INTO memories (id,content,category,tags,source,mood,created_at,updated_at,expires_at,pinned,content_hash,activation_score,fact_key,superseded_by,protected,evidence_raw_ids) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
          .run(m.id, m.content, m.category, JSON.stringify(m.tags || []), m.source || '', m.mood, m.created_at, m.updated_at, m.expires_at || null, m.pinned ? 1 : 0, m.content_hash || contentHash(m.content, m.category), Number(m.activation_score || 0), m.fact_key || null, m.superseded_by || null, resolveProtectedFlag(m.category, m.protected, 0), JSON.stringify(m.evidence_raw_ids || []));
        restored++;
      }
    }
    let restoredEdges = 0;
    const er = await ghRequest('GET', EDGES_BACKUP_PATH);
    if (er.ok) {
      const edgeData = await er.json();
      const edgeContent = Buffer.from(edgeData.content.replace(/\n/g, ''), 'base64').toString('utf-8');
      const edgeLines = edgeContent.split('\n').filter(l => l.trim());
      const edgeStmt = db.prepare('INSERT OR IGNORE INTO memory_edges (source_id,target_id,weight,created_at) VALUES (?,?,?,?)');
      for (const line of edgeLines) {
        const e = JSON.parse(line);
        edgeStmt.run(e.source_id, e.target_id, Number(e.weight || 0), e.created_at);
        restoredEdges++;
      }
    }
    console.log(`[backup] auto-restored ${restored} memories and ${restoredEdges} edges from GitHub`);
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
