import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const nowIso = () => new Date().toISOString();
const clamp = (value, min, max) => Math.max(min, Math.min(max, Number(value) || 0));
const safeJsonArray = (value) => {
  try {
    const parsed = typeof value === 'string' ? JSON.parse(value || '[]') : value;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};
const hash = (value) => crypto.createHash('sha256').update(Buffer.isBuffer(value) ? value : String(value || ''), 'utf8').digest('hex');
const truncate = (value, max = 180) => {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  return text.length <= max ? text : `${text.slice(0, max - 1)}...`;
};

const REMOTE_SECRET_PATTERNS = [
  [/\b(?:sk|pa|ghp|github_pat|xoxb|xoxp|AIza)[-_A-Za-z0-9]{12,}\b/g, '[REDACTED_TOKEN]'],
  [/\bBearer\s+[A-Za-z0-9._~+/=-]{10,}\b/gi, 'Bearer [REDACTED]'],
  [/(api[_-]?key|token|secret|password|passwd|authorization)\s*[:=]\s*["']?[^\s,"'}]{6,}/gi, '$1=[REDACTED]'],
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, '[REDACTED_PRIVATE_KEY]'],
];

export function redactForRemote(text, { maxChars = 12000 } = {}) {
  let clean = String(text || '')
    .replace(/<[^>]+_context>[\s\S]*?<\/[^>]+_context>/gi, ' ')
    .replace(/(?:WECHAT SESSION INSTRUCTIONS|SYSTEM ACTION MODE|<codex_delegation>)[\s\S]{0,8000}/gi, ' ')
    .replace(/```(?:json|sql|diff|log|text)?[\s\S]{2000,}?```/gi, '[OMITTED_LONG_BLOCK]');
  for (const [pattern, replacement] of REMOTE_SECRET_PATTERNS) clean = clean.replace(pattern, replacement);
  return clean.replace(/\s+/g, ' ').trim().slice(0, maxChars);
}

export function prepareMemoryStorage({ dataDir, dbPath }) {
  const snapshotDir = process.env.MEMORY_SNAPSHOT_DIR || path.join(dataDir, 'snapshots');
  const pendingRestorePath = path.join(dataDir, 'pending-restore.json');
  fs.mkdirSync(dataDir, { recursive: true });
  fs.mkdirSync(snapshotDir, { recursive: true });
  if (!fs.existsSync(pendingRestorePath)) return { snapshotDir, pendingRestorePath, restored: null };

  const marker = JSON.parse(fs.readFileSync(pendingRestorePath, 'utf8'));
  const requestedPath = path.resolve(String(marker.snapshot_path || ''));
  const allowedRoot = `${path.resolve(snapshotDir)}${path.sep}`;
  if (!requestedPath.startsWith(allowedRoot) || !fs.existsSync(requestedPath)) {
    throw new Error('Pending memory snapshot restore is missing or outside the snapshot directory.');
  }
  const actualHash = hash(fs.readFileSync(requestedPath));
  if (!marker.sha256 || marker.sha256 !== actualHash) throw new Error('Pending memory snapshot restore failed hash verification.');
  if (fs.existsSync(dbPath)) fs.copyFileSync(dbPath, path.join(snapshotDir, `pre-restore-${Date.now()}.db`));
  fs.copyFileSync(requestedPath, dbPath);
  for (const suffix of ['-wal', '-shm']) {
    const sidecar = `${dbPath}${suffix}`;
    if (fs.existsSync(sidecar)) fs.rmSync(sidecar, { force: true });
  }
  fs.rmSync(pendingRestorePath, { force: true });
  return { snapshotDir, pendingRestorePath, restored: { snapshot_path: requestedPath, requested_at: marker.requested_at || null } };
}

export function installLmcClosureSchema(db) {
  for (const sql of [
    'ALTER TABLE memories ADD COLUMN deleted_at TEXT',
    "ALTER TABLE memories ADD COLUMN thread TEXT NOT NULL DEFAULT 'other'",
    'ALTER TABLE memories ADD COLUMN importance REAL NOT NULL DEFAULT 0.5',
    'ALTER TABLE memories ADD COLUMN weight REAL NOT NULL DEFAULT 1.0',
    "ALTER TABLE memories ADD COLUMN lifecycle_bucket TEXT NOT NULL DEFAULT 'retain'",
    'ALTER TABLE memories ADD COLUMN resolved INTEGER NOT NULL DEFAULT 0',
  ]) {
    try { db.exec(sql); } catch {}
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS memory_snapshots (
      id TEXT PRIMARY KEY, file_name TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '',
      size_bytes INTEGER NOT NULL DEFAULT 0, sha256 TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'ready', created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_snapshots_created ON memory_snapshots(created_at);

    CREATE TABLE IF NOT EXISTS narrative_summaries (
      id TEXT PRIMARY KEY, period_type TEXT NOT NULL, period_key TEXT NOT NULL,
      thread TEXT NOT NULL DEFAULT 'other', title TEXT NOT NULL, summary TEXT NOT NULL,
      memory_ids TEXT NOT NULL DEFAULT '[]', start_time TEXT NOT NULL, end_time TEXT NOT NULL,
      created_at TEXT NOT NULL, UNIQUE(period_type, period_key, thread)
    );
    CREATE INDEX IF NOT EXISTS idx_narratives_period ON narrative_summaries(period_type, period_key);

    CREATE TABLE IF NOT EXISTS relation_reviews (
      id TEXT PRIMARY KEY, source_id TEXT NOT NULL, target_id TEXT NOT NULL,
      relation TEXT NOT NULL, weight REAL NOT NULL DEFAULT 0.5, reason TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
      reviewed_at TEXT, review_note TEXT NOT NULL DEFAULT '',
      UNIQUE(source_id, target_id, relation, status)
    );
    CREATE INDEX IF NOT EXISTS idx_relation_reviews_status ON relation_reviews(status, created_at);

    CREATE TABLE IF NOT EXISTS spontaneous_cache (
      id TEXT PRIMARY KEY, memory_id TEXT NOT NULL UNIQUE, reason TEXT NOT NULL DEFAULT '',
      score REAL NOT NULL DEFAULT 0, surfaced_count INTEGER NOT NULL DEFAULT 0,
      last_surfaced_at TEXT, expires_at TEXT NOT NULL, created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_spontaneous_expires ON spontaneous_cache(expires_at);

    CREATE TABLE IF NOT EXISTS spontaneous_history (
      id TEXT PRIMARY KEY, memory_id TEXT NOT NULL, surfaced_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_spontaneous_history_time ON spontaneous_history(surfaced_at);

    CREATE TABLE IF NOT EXISTS recall_traces (
      id TEXT PRIMARY KEY, query_hash TEXT NOT NULL, query_preview TEXT NOT NULL DEFAULT '',
      channels TEXT NOT NULL DEFAULT '[]', result_ids TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_recall_traces_created ON recall_traces(created_at);

    CREATE TABLE IF NOT EXISTS recall_trace_items (
      id TEXT PRIMARY KEY, trace_id TEXT NOT NULL, memory_id TEXT, rank INTEGER NOT NULL,
      recall_layer TEXT NOT NULL, evidence_role TEXT NOT NULL, injected INTEGER NOT NULL DEFAULT 0,
      score REAL NOT NULL DEFAULT 0, score_breakdown TEXT NOT NULL DEFAULT '{}',
      reasons TEXT NOT NULL DEFAULT '[]', related_from TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_recall_trace_items_trace ON recall_trace_items(trace_id, rank);

    CREATE TABLE IF NOT EXISTS recall_feedback (
      id TEXT PRIMARY KEY, trace_id TEXT NOT NULL, memory_id TEXT,
      outcome TEXT NOT NULL, note TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_recall_feedback_trace ON recall_feedback(trace_id, created_at);

    CREATE TABLE IF NOT EXISTS dream_runs (
      id TEXT PRIMARY KEY, mode TEXT NOT NULL DEFAULT 'night_dream', status TEXT NOT NULL,
      dry_run INTEGER NOT NULL DEFAULT 1, started_at TEXT NOT NULL, finished_at TEXT,
      step_results TEXT NOT NULL DEFAULT '[]', error TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX IF NOT EXISTS idx_dream_runs_started ON dream_runs(started_at);
  `);
}

function categoryThread(category) {
  const c = String(category || '').trim();
  if (['identity', 'anchor', 'corridor', 'deep'].includes(c)) return 'identity';
  if (['relationship', '日常', '日记', 'diary', 'cc-diary', '私藏', '心动', 'dream'].includes(c)) return 'relationship';
  if (['工作', 'work', 'project', '工具'].includes(c)) return 'engineering';
  if (c === 'writing') return 'writing';
  return 'other';
}

const TECHNICAL_CATEGORIES = new Set(['工作', 'work', 'project', '工具', 'knowledge', 'notebook']);
const PROTECTED_CATEGORIES = new Set(['diary', 'deep', 'anchor', '私藏', '心动', 'cc-diary']);
const E_RULES = {
  version: 'rules-v4',
  positive: ['开心', '喜欢', '爱', '稳', '完成', '成功', '亲', '抱', '心动', '温柔'],
  negative: ['生气', '难过', '失落', '推开', '回避', '空心', '不理', '失联'],
  arousal: ['亲密', '欲望', '触觉', '心动', '生气', '紧张'],
  tension: ['冲突', '害怕', '失联', '推开', '回避', '空心'],
  risk: ['越权', '红线', '伤害', '失联', '空心', '不理', '推开', '回避', '害怕失去'],
  urgency: ['现在', '立刻', '马上', '紧急', '挂了', '掉了', '炸了'],
  negators: ['假装', '没有', '不', '没', '别'],
};

function termHits(text, terms) {
  let positive = 0;
  let negated = 0;
  for (const term of terms) {
    let start = 0;
    while (start < text.length) {
      const index = text.indexOf(term, start);
      if (index < 0) break;
      const before = text.slice(Math.max(0, index - 3), index);
      if (E_RULES.negators.some((word) => before.includes(word))) negated += 1;
      else positive += 1;
      start = index + Math.max(1, term.length);
    }
  }
  return { positive, negated };
}

function shouldScoreEAxis(row) {
  const tags = safeJsonArray(row.tags);
  const explicit = tags.some((tag) => /心跳|情绪|关系|亲密|偏好|relationship|heartbeat/i.test(String(tag)));
  if (TECHNICAL_CATEGORIES.has(String(row.category || '').trim()) && !explicit) return { score: false, reason: 'technical_without_e_axis_tag' };
  if (explicit || ['relationship', '日常', '日记', 'diary', 'cc-diary', '私藏', '心动'].includes(row.category)) {
    return { score: true, reason: explicit ? 'explicit_e_axis_tag' : 'relationship_or_diary' };
  }
  const text = `${row.content || ''} ${tags.join(' ')}`;
  const any = [...E_RULES.positive, ...E_RULES.negative, ...E_RULES.arousal, ...E_RULES.tension, ...E_RULES.risk, ...E_RULES.urgency]
    .some((term) => text.includes(term));
  return any ? { score: true, reason: 'emotion_keyword' } : { score: false, reason: 'no_e_axis_trigger' };
}

function scoreEAxisRow(row) {
  const text = `${row.content || ''} ${safeJsonArray(row.tags).join(' ')}`;
  const positive = termHits(text, E_RULES.positive);
  const negative = termHits(text, E_RULES.negative);
  const arousal = termHits(text, E_RULES.arousal);
  const tension = termHits(text, E_RULES.tension);
  const risk = termHits(text, E_RULES.risk);
  const urgency = termHits(text, E_RULES.urgency);
  const evidence = [positive, negative, arousal, tension, risk, urgency]
    .reduce((sum, hits) => sum + hits.positive + hits.negated, 0);
  return {
    memory_id: row.id,
    valence: clamp(positive.positive * 0.35 + negative.negated * 0.25 - negative.positive * 0.4 - positive.negated * 0.25, -1, 1),
    arousal: arousal.positive ? 0.7 : 0.25,
    tension: tension.positive ? 0.75 : 0.2,
    confidence: clamp(0.45 + evidence * 0.08, 0.45, 0.9),
    risk_level: risk.positive ? 0.7 : 0.15,
    urgency: urgency.positive ? 0.75 : 0.2,
    scorer_version: E_RULES.version,
    shadow: true,
    updated_at: nowIso(),
  };
}

function memoryHalfLife(row) {
  if (row.protected || PROTECTED_CATEGORIES.has(row.category)) return null;
  const map = { daily: 14, work: 30, 工作: 30, project: 30, writing: 60, dream: 60 };
  return map[row.category] ?? 45;
}

function metabolicState(row, eScore, at = new Date()) {
  const ageDays = Math.max(0, (at.getTime() - new Date(row.created_at || at).getTime()) / 86400000);
  const halfLife = memoryHalfLife(row);
  const decay = halfLife ? Math.pow(0.5, ageDays / halfLife) : 1;
  const importance = clamp(row.importance ?? 0.5, 0.05, 1);
  const activation = Math.max(1, Number(row.activation_score || 0) + 1);
  const emotion = 1 + clamp(eScore?.arousal || 0, 0, 1) * 0.8;
  const resolved = row.resolved ? 0.7 : 1;
  const storedWeight = clamp(row.weight ?? 1, 0.05, 10);
  const score = importance * Math.pow(activation, 0.3) * decay * emotion * resolved * storedWeight;
  const bucket = row.lifecycle_bucket || 'retain';
  const suggested = !row.protected && bucket === 'retain' && halfLife && ageDays > halfLife * 3 && score < 0.3 ? 'cold' : bucket;
  return {
    memory_id: row.id,
    category: row.category,
    age_days: Number(ageDays.toFixed(2)),
    half_life_days: halfLife,
    score: Number(score.toFixed(4)),
    bucket,
    suggested_bucket: suggested,
    protected: !!row.protected,
  };
}

function metabolicGate(row, mode = 'recall', at = new Date()) {
  const status = String(row.status || 'current').toLowerCase();
  const source = String(row.source || '').toLowerCase();
  const category = String(row.category || '').toLowerCase();
  const protectedRow = !!row.protected || PROTECTED_CATEGORIES.has(row.category)
    || ['identity', 'relationship', 'corridor'].includes(category);
  if (status !== 'current' || row.deleted_at || row.superseded_by) {
    return { bucket: 'quarantine', allowed: false, factor: 0, reason: `status:${status}` };
  }
  if (protectedRow) return { bucket: 'retain', allowed: true, factor: 1, reason: 'protected' };
  const noise = new Set(['debug', 'log', 'logs', 'scratch', 'temp', 'transient', 'working', 'worklog']);
  if (noise.has(source)) return { bucket: 'quarantine', allowed: false, factor: 0, reason: `noise_source:${source}` };
  if (noise.has(category)) return { bucket: 'quarantine', allowed: false, factor: 0, reason: `noise_category:${category}` };
  const state = metabolicState(row, row, at);
  const bucket = state.suggested_bucket || state.bucket || 'retain';
  const factor = bucket === 'cold' ? 0.45 : bucket === 'quarantine' ? 0 : 1;
  if (mode === 'surface' && bucket !== 'retain') return { bucket, allowed: false, factor: 0, reason: `surface_blocks_${bucket}` };
  if (mode === 'surface' && ['conversation', 'raw', 'raw_event'].includes(source)) {
    return { bucket: 'quarantine', allowed: false, factor: 0, reason: `surface_block_source:${source}` };
  }
  return { bucket, allowed: factor > 0, factor, reason: bucket };
}

function isoWeekKey(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const week = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}

function periodRange(type, reference = new Date()) {
  if (type === 'month') {
    const start = new Date(Date.UTC(reference.getUTCFullYear(), reference.getUTCMonth(), 1));
    const end = new Date(Date.UTC(reference.getUTCFullYear(), reference.getUTCMonth() + 1, 1));
    return { key: reference.toISOString().slice(0, 7), start, end };
  }
  const end = new Date(reference);
  const day = end.getUTCDay() || 7;
  const start = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate() - day + 1));
  const next = new Date(start.getTime() + 7 * 86400000);
  return { key: isoWeekKey(reference), start, end: next };
}

function candidateCategory(chunk) {
  if (['private', 'intimate'].includes(chunk.channel)) return 'private_candidate';
  if (/部署|代码|修复|测试|服务|VPS|Zeabur|Git|接口|数据库/i.test(chunk.summary || '')) return 'work';
  return 'daily';
}

export function createLmcClosureService({ db, dataDir, dbPath, embedText = null }) {
  const snapshotDir = process.env.MEMORY_SNAPSHOT_DIR || path.join(dataDir, 'snapshots');
  const pendingRestorePath = path.join(dataDir, 'pending-restore.json');
  const retention = clamp(process.env.MEMORY_SNAPSHOT_RETENTION || 14, 3, 90);

  function createSnapshot({ reason = 'manual', dry_run = true } = {}) {
    const id = crypto.randomUUID();
    const stamp = nowIso().replace(/[:.]/g, '-');
    const filePath = path.join(snapshotDir, `${stamp}-${id.slice(0, 8)}.db`);
    if (dry_run) return { dry_run: true, id, reason, snapshot_path: filePath, source_db: dbPath };
    db.exec(`VACUUM INTO '${filePath.replaceAll("'", "''")}'`);
    const bytes = fs.readFileSync(filePath);
    const sha256 = crypto.createHash('sha256').update(bytes).digest('hex');
    db.prepare('INSERT INTO memory_snapshots (id,file_name,reason,size_bytes,sha256,status,created_at) VALUES (?,?,?,?,?,?,?)')
      .run(id, path.basename(filePath), String(reason || ''), bytes.length, sha256, 'ready', nowIso());
    const old = db.prepare("SELECT id,file_name FROM memory_snapshots WHERE status='ready' ORDER BY created_at DESC").all();
    for (const row of old.slice(retention)) {
      const oldPath = path.join(snapshotDir, path.basename(row.file_name));
      if (fs.existsSync(oldPath)) fs.rmSync(oldPath, { force: true });
      db.prepare("UPDATE memory_snapshots SET status='pruned' WHERE id=?").run(row.id);
    }
    return { dry_run: false, id, reason, snapshot_path: filePath, size_bytes: bytes.length, sha256, created_at: nowIso() };
  }

  function listSnapshots({ limit = 20 } = {}) {
    return db.prepare('SELECT * FROM memory_snapshots ORDER BY created_at DESC LIMIT ?').all(clamp(limit, 1, 100)).map((row) => ({
      ...row,
      snapshot_path: path.join(snapshotDir, path.basename(row.file_name)),
      file_exists: fs.existsSync(path.join(snapshotDir, path.basename(row.file_name))),
    }));
  }

  function restoreSnapshot({ id, dry_run = true } = {}) {
    const row = db.prepare("SELECT * FROM memory_snapshots WHERE id=? AND status='ready'").get(id);
    if (!row) return { error: 'snapshot_not_found', id };
    const snapshotPath = path.join(snapshotDir, path.basename(row.file_name));
    if (!fs.existsSync(snapshotPath)) return { error: 'snapshot_file_missing', id };
    const actualHash = crypto.createHash('sha256').update(fs.readFileSync(snapshotPath)).digest('hex');
    if (actualHash !== row.sha256) return { error: 'snapshot_hash_mismatch', id };
    const plan = { id, snapshot_path: snapshotPath, sha256: actualHash, requested_at: nowIso(), restart_required: true };
    if (dry_run) return { dry_run: true, would_schedule: plan };
    if (process.env.MEMORY_ALLOW_SNAPSHOT_RESTORE !== 'true') {
      return { error: 'snapshot_restore_disabled', note: 'Approved restore requires MEMORY_ALLOW_SNAPSHOT_RESTORE=true.' };
    }
    fs.writeFileSync(pendingRestorePath, `${JSON.stringify(plan, null, 2)}\n`, 'utf8');
    return { dry_run: false, scheduled: plan };
  }

  function inspectMetabolism({ limit = 50 } = {}) {
    const rows = db.prepare(`
      SELECT m.*,e.arousal FROM memories m LEFT JOIN e_axis_scores e ON e.memory_id=m.id
      WHERE m.deleted_at IS NULL ORDER BY m.created_at DESC LIMIT ?
    `).all(clamp(limit, 1, 200));
    const items = rows.map((row) => metabolicState(row, row));
    return {
      count: items.length,
      suggested_cold: items.filter((item) => item.suggested_bucket === 'cold').length,
      items,
      note: 'Read-only metabolism audit; no memory is archived or deleted.',
    };
  }

  function scoreEAxis({ memory_id, limit = 50, dry_run = true } = {}) {
    const safeLimit = clamp(limit, 1, 200);
    const rows = memory_id ? db.prepare(`
      SELECT m.* FROM memories m WHERE m.id=? AND m.deleted_at IS NULL
    `).all(memory_id) : db.prepare(`
      SELECT m.* FROM memories m LEFT JOIN e_axis_scores e ON e.memory_id=m.id
      WHERE m.deleted_at IS NULL AND m.superseded_by IS NULL AND COALESCE(m.status,'current')='current'
      ORDER BY CASE WHEN e.memory_id IS NULL OR e.scorer_version != ? THEN 0 ELSE 1 END,m.created_at DESC
      LIMIT ?
    `).all(E_RULES.version, Math.max(safeLimit * 10, 200));
    const scores = [];
    const skipped = [];
    for (const row of rows) {
      const gate = shouldScoreEAxis(row);
      if (!gate.score) {
        skipped.push({ memory_id: row.id, reason: gate.reason });
        if (!dry_run) db.prepare('DELETE FROM e_axis_scores WHERE memory_id=?').run(row.id);
        continue;
      }
      if (scores.length >= safeLimit) continue;
      scores.push({ ...scoreEAxisRow(row), trigger_reason: gate.reason });
    }
    if (!dry_run) {
      const upsert = db.prepare(`
        INSERT INTO e_axis_scores (memory_id,valence,arousal,tension,confidence,risk_level,urgency,scorer_version,shadow,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(memory_id) DO UPDATE SET
        valence=excluded.valence,arousal=excluded.arousal,tension=excluded.tension,confidence=excluded.confidence,
        risk_level=excluded.risk_level,urgency=excluded.urgency,scorer_version=excluded.scorer_version,
        shadow=excluded.shadow,updated_at=excluded.updated_at
      `);
      for (const score of scores) upsert.run(score.memory_id, score.valence, score.arousal, score.tension, score.confidence, score.risk_level, score.urgency, score.scorer_version, 1, score.updated_at);
    }
    return { dry_run, version: E_RULES.version, count: scores.length, skipped_count: skipped.length, scores, skipped };
  }

  function runZAxisAudit({ limit = 100, dry_run = true } = {}) {
    const groups = db.prepare(`
      SELECT fact_key,GROUP_CONCAT(id) AS ids,COUNT(*) AS count FROM memories
      WHERE fact_key IS NOT NULL AND deleted_at IS NULL AND superseded_by IS NULL AND COALESCE(status,'current')='current'
      GROUP BY fact_key HAVING COUNT(*) > 1 LIMIT ?
    `).all(clamp(limit, 1, 500));
    const planned = [];
    for (const group of groups) {
      const ids = String(group.ids || '').split(',').filter(Boolean);
      const exists = db.prepare("SELECT id FROM z_conflict_audits WHERE fact_key=? AND status='pending'").get(group.fact_key);
      if (exists) continue;
      const item = { id: crypto.randomUUID(), fact_key: group.fact_key, memory_ids: ids, reason: 'multiple current memories share one fact_key' };
      planned.push(item);
      if (!dry_run) db.prepare(`
        INSERT INTO z_conflict_audits (id,fact_key,stale_id,current_id,protected_ids,reason,status,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?)
      `).run(item.id, item.fact_key, ids[1] || null, ids[0] || null, JSON.stringify(ids), item.reason, 'pending', nowIso(), nowIso());
    }
    return { dry_run, conflict_count: groups.length, created_count: dry_run ? 0 : planned.length, planned };
  }

  function buildSafeRelations({ since_hours = 168, limit = 200, dry_run = true } = {}) {
    const since = new Date(Date.now() - clamp(since_hours, 1, 8760) * 3600000).toISOString();
    const now = nowIso();
    const rows = db.prepare(`
      SELECT * FROM memories WHERE deleted_at IS NULL AND superseded_by IS NULL
      AND COALESCE(status,'current')='current' AND (expires_at IS NULL OR expires_at>?)
      AND created_at>=? ORDER BY created_at DESC LIMIT ?
    `).all(now, since, clamp(limit, 1, 1000));
    const plans = [];
    for (let i = 0; i < rows.length; i += 1) {
      const source = rows[i];
      const sourceTags = new Set(safeJsonArray(source.tags).filter((tag) => {
        const value = String(tag).trim();
        return value.length >= 3 && !/^(测试|日常|日记|记录|记忆|私藏|心动|关系|工作|坐标|月亮|克|闻川|第\d+天)$/i.test(value);
      }));
      const sourceEvidence = new Set([...safeJsonArray(source.evidence_raw_ids), ...safeJsonArray(source.evidence_chunk_ids)]);
      let temporalAdded = false;
      let sourcePlanCount = 0;
      for (let j = i + 1; j < Math.min(rows.length, i + 25); j += 1) {
        const target = rows[j];
        if (source.id === target.id) continue;
        const sharedTags = safeJsonArray(target.tags).filter((tag) => sourceTags.has(tag));
        const sharedEvidence = [...safeJsonArray(target.evidence_raw_ids), ...safeJsonArray(target.evidence_chunk_ids)].filter((id) => sourceEvidence.has(id));
        const sameProject = source.source && target.source && source.source === target.source && categoryThread(source.category) === 'engineering';
        const timeGapHours = Math.abs(new Date(source.created_at) - new Date(target.created_at)) / 3600000;
        const temporalSequence = !temporalAdded && source.source && source.source === target.source && categoryThread(source.category) === categoryThread(target.category) && timeGapHours <= 6;
        if (!sharedEvidence.length && !sharedTags.length && !sameProject && !temporalSequence) continue;
        const relation = sharedEvidence.length ? 'same_event' : sameProject ? 'same_project' : temporalSequence ? 'temporal_sequence' : 'same_topic';
        const weight = clamp(0.45 + sharedTags.length * 0.1 + sharedEvidence.length * 0.2 + (sameProject ? 0.15 : 0) + (temporalSequence ? 0.1 : 0), 0.45, 0.95);
        const [sourceId, targetId] = relation === 'temporal_sequence'
          ? (new Date(source.created_at) <= new Date(target.created_at) ? [source.id, target.id] : [target.id, source.id])
          : (source.id < target.id ? [source.id, target.id] : [target.id, source.id]);
        if (relation === 'temporal_sequence') temporalAdded = true;
        const exists = db.prepare('SELECT 1 FROM memory_edges WHERE source_id=? AND target_id=?').get(sourceId, targetId);
        if (exists) continue;
        plans.push({ source_id: sourceId, target_id: targetId, relation_type: relation, strength: weight, status: 'safe', reason: sharedEvidence.length ? `shared evidence: ${sharedEvidence.length}` : sameProject ? 'same source project' : temporalSequence ? `same source within ${timeGapHours.toFixed(1)}h` : `shared tags: ${sharedTags.join(',')}` });
        sourcePlanCount += 1;
        if (sourcePlanCount >= 4) break;
        if (plans.length >= clamp(limit, 1, 1000)) break;
      }
      if (plans.length >= clamp(limit, 1, 1000)) break;
    }
    if (!dry_run) {
      const insert = db.prepare(`
        INSERT OR IGNORE INTO memory_edges (source_id,target_id,weight,created_at,relation_type,strength,status,reason,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?)
      `);
      for (const edge of plans) insert.run(edge.source_id, edge.target_id, edge.strength, nowIso(), edge.relation_type, edge.strength, edge.status, edge.reason, nowIso());
    }
    return { dry_run, planned_count: plans.length, written_count: dry_run ? 0 : plans.length, relations: plans };
  }

  function addRelation({ source_id, target_id, relation = 'same_topic', weight = 0.5, reason = '', dry_run = true } = {}) {
    if (!source_id || !target_id || source_id === target_id) return { error: 'invalid_endpoints' };
    const endpoints = db.prepare(`SELECT id FROM memories WHERE id IN (?,?) AND deleted_at IS NULL
      AND superseded_by IS NULL AND COALESCE(status,'current')='current'
      AND (expires_at IS NULL OR expires_at>?)`).all(source_id, target_id, nowIso());
    if (endpoints.length !== 2) return { error: 'missing_or_deleted_endpoint' };
    const safeTypes = new Set(['same_topic', 'same_event', 'temporal_sequence', 'derived_from', 'same_project', 'semantic']);
    const reviewTypes = new Set(['supports', 'contradicts', 'cause_effect', 'relationship_moment', 'emotional_link']);
    const normalized = String(relation || 'same_topic');
    const status = safeTypes.has(normalized) && !reviewTypes.has(normalized) ? 'safe' : 'review';
    const plan = { source_id, target_id, relation: normalized, weight: clamp(weight, 0.05, 1), reason: String(reason || ''), status };
    if (dry_run) return { dry_run: true, plan };
    if (status === 'review') {
      const id = crypto.randomUUID();
      db.prepare('INSERT OR IGNORE INTO relation_reviews (id,source_id,target_id,relation,weight,reason,status,created_at) VALUES (?,?,?,?,?,?,?,?)')
        .run(id, source_id, target_id, normalized, plan.weight, plan.reason, 'pending', nowIso());
      return { dry_run: false, queued_review: id, plan };
    }
    db.prepare(`INSERT OR REPLACE INTO memory_edges
      (source_id,target_id,weight,created_at,relation_type,strength,status,reason,updated_at)
      VALUES (?,?,?,?,?,?,?,?,?)`).run(source_id, target_id, plan.weight, nowIso(), normalized, plan.weight, 'safe', plan.reason, nowIso());
    return { dry_run: false, written: true, plan };
  }

  function listRelationReviews({ status = 'pending', limit = 50 } = {}) {
    return db.prepare('SELECT * FROM relation_reviews WHERE (? = \'all\' OR status=?) ORDER BY created_at DESC LIMIT ?')
      .all(status, status, clamp(limit, 1, 200));
  }

  function reviewRelation({ id, action, review_note = '' } = {}) {
    const row = db.prepare("SELECT * FROM relation_reviews WHERE id=? AND status='pending'").get(id);
    if (!row) return { error: 'pending_relation_review_not_found', id };
    const status = action === 'approve' ? 'approved' : 'rejected';
    if (action === 'approve') {
      db.prepare(`INSERT OR REPLACE INTO memory_edges
        (source_id,target_id,weight,created_at,relation_type,strength,status,reason,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?)`).run(row.source_id, row.target_id, row.weight, nowIso(), row.relation, row.weight, 'safe', row.reason, nowIso());
    }
    db.prepare('UPDATE relation_reviews SET status=?,reviewed_at=?,review_note=? WHERE id=?').run(status, nowIso(), review_note, id);
    return { id, status, edge_written: action === 'approve' };
  }

  function runNarrative({ period_type = 'both', reference_time, thread = 'all', force = false, dry_run = true } = {}) {
    const reference = reference_time ? new Date(reference_time) : new Date();
    const types = period_type === 'both' ? ['week', 'month'] : [period_type];
    const planned = [];
    for (const type of types) {
      const period = periodRange(type, reference);
      const rows = db.prepare(`
        SELECT * FROM memories WHERE deleted_at IS NULL AND superseded_by IS NULL
        AND COALESCE(status,'current')='current' AND created_at>=? AND created_at<? ORDER BY created_at ASC
      `).all(period.start.toISOString(), period.end.toISOString());
      const groups = new Map();
      for (const row of rows) {
        const rowThread = row.thread && row.thread !== 'other' ? row.thread : categoryThread(row.category);
        if (thread !== 'all' && rowThread !== thread) continue;
        if (!groups.has(rowThread)) groups.set(rowThread, []);
        groups.get(rowThread).push(row);
      }
      for (const [rowThread, memories] of groups) {
        const exists = db.prepare('SELECT id FROM narrative_summaries WHERE period_type=? AND period_key=? AND thread=?').get(type, period.key, rowThread);
        if (exists && !force) continue;
        const selected = memories.slice(-12);
        const item = {
          id: exists?.id || crypto.randomUUID(), period_type: type, period_key: period.key, thread: rowThread,
          title: `${period.key} · ${rowThread}`,
          summary: selected.map((row) => `${row.category}：${truncate(row.content, 120)}`).join('\n').slice(0, 2400),
          memory_ids: selected.map((row) => row.id), start_time: period.start.toISOString(), end_time: period.end.toISOString(), created_at: nowIso(),
        };
        planned.push(item);
        if (!dry_run) db.prepare(`
          INSERT INTO narrative_summaries (id,period_type,period_key,thread,title,summary,memory_ids,start_time,end_time,created_at)
          VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(period_type,period_key,thread) DO UPDATE SET
          title=excluded.title,summary=excluded.summary,memory_ids=excluded.memory_ids,start_time=excluded.start_time,end_time=excluded.end_time,created_at=excluded.created_at
        `).run(item.id, item.period_type, item.period_key, item.thread, item.title, item.summary, JSON.stringify(item.memory_ids), item.start_time, item.end_time, item.created_at);
      }
    }
    return { dry_run, planned_count: planned.length, written_count: dry_run ? 0 : planned.length, narratives: planned };
  }

  function listNarratives({ period_type = 'all', thread = 'all', limit = 20 } = {}) {
    let sql = 'SELECT * FROM narrative_summaries WHERE 1=1';
    const params = [];
    if (period_type !== 'all') { sql += ' AND period_type=?'; params.push(period_type); }
    if (thread !== 'all') { sql += ' AND thread=?'; params.push(thread); }
    sql += ' ORDER BY end_time DESC LIMIT ?'; params.push(clamp(limit, 1, 100));
    return db.prepare(sql).all(...params).map((row) => ({ ...row, memory_ids: safeJsonArray(row.memory_ids) }));
  }

  function inspectOtherIncubation({ observe_threshold = 3, candidate_threshold = 5, formal_threshold = 8, formal_min_span_days = 14, formal_min_hits = 2 } = {}) {
    const rows = db.prepare(`SELECT id,category,tags,created_at,activation_score FROM memories
      WHERE thread='other' AND deleted_at IS NULL AND superseded_by IS NULL AND COALESCE(status,'current')='current'`).all();
    const groups = new Map();
    const add = (kind, key, row) => {
      if (!key) return;
      const id = `${kind}:${key}`;
      if (!groups.has(id)) groups.set(id, { kind, key, rows: [] });
      groups.get(id).rows.push(row);
    };
    for (const row of rows) {
      add('category', row.category, row);
      for (const tag of safeJsonArray(row.tags)) add('tag', String(tag), row);
    }
    const suggestions = [];
    for (const group of groups.values()) {
      const ordered = group.rows.slice().sort((a, b) => String(a.created_at).localeCompare(String(b.created_at)));
      const count = ordered.length;
      const spanDays = count > 1 ? Math.max(0, (new Date(ordered.at(-1).created_at) - new Date(ordered[0].created_at)) / 86400000) : 0;
      const hitTotal = ordered.reduce((sum, row) => sum + Number(row.activation_score || 0), 0);
      let stage = null;
      if (count >= formal_threshold && spanDays >= formal_min_span_days && hitTotal >= formal_min_hits) stage = 'formal_line_candidate';
      else if (count >= candidate_threshold || (count >= observe_threshold && hitTotal >= 3)) stage = 'candidate_line';
      else if (count >= observe_threshold) stage = 'observe_cluster';
      if (stage) suggestions.push({ action: 'split_thread', stage, thread: 'other', group_kind: group.kind, group_key: group.key, count, span_days: +spanDays.toFixed(2), hit_total: +hitTotal.toFixed(2), memory_ids: ordered.slice(0, 20).map((row) => row.id), note: 'Review-only incubation; no thread is changed automatically.' });
    }
    return { scanned_count: rows.length, suggestion_count: suggestions.length, suggestions };
  }

  function metabolicGateForRecall(row, { mode = 'recall' } = {}) {
    return metabolicGate(row, mode);
  }

  function recordRecallTrace({ query, channels = [], layers, requested_count = 0 }) {
    const id = crypto.randomUUID();
    const ordered = [
      ...(layers.main_recall || []).map((item) => ({ ...item, recall_layer: 'main_recall', evidence_role: 'authority' })),
      ...(layers.source_neighborhood || []).map((item) => ({ ...item, recall_layer: 'source_neighborhood', evidence_role: 'navigation' })),
      ...(layers.graph_expansion || []).map((item) => ({ ...item, recall_layer: 'graph_expansion', evidence_role: 'association' })),
      ...(layers.fallback_archive || []).map((item) => ({ ...item, recall_layer: 'fallback_archive', evidence_role: 'last_resort' })),
    ];
    db.prepare('INSERT INTO recall_traces (id,query_hash,query_preview,channels,result_ids,created_at) VALUES (?,?,?,?,?,?)')
      .run(id, hash(query), truncate(query, 160), JSON.stringify(channels), JSON.stringify(ordered.map((item) => item.id).filter(Boolean)), nowIso());
    const insert = db.prepare(`INSERT INTO recall_trace_items
      (id,trace_id,memory_id,rank,recall_layer,evidence_role,injected,score,score_breakdown,reasons,related_from,created_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`);
    ordered.forEach((item, index) => insert.run(crypto.randomUUID(), id, item.id || null, index + 1, item.recall_layer, item.evidence_role,
      item.recall_layer === 'main_recall' || item.recall_layer === 'graph_expansion' ? 1 : 0,
      Number(item.recall_score || item.graph_score || 0), JSON.stringify(item.score_breakdown || {}),
      JSON.stringify(item.recall_channels || [item.recall_layer]), JSON.stringify(item.bridge_from ? [item.bridge_from] : []), nowIso()));
    return { recall_run_id: id, requested_count, selected_count: ordered.length };
  }

  function listRecallTraces({ limit = 20, trace_id } = {}) {
    const traces = trace_id
      ? db.prepare('SELECT * FROM recall_traces WHERE id=?').all(trace_id)
      : db.prepare('SELECT * FROM recall_traces ORDER BY created_at DESC LIMIT ?').all(clamp(limit, 1, 100));
    return traces.map((trace) => ({ ...trace, channels: safeJsonArray(trace.channels), result_ids: safeJsonArray(trace.result_ids), items: db.prepare('SELECT * FROM recall_trace_items WHERE trace_id=? ORDER BY rank').all(trace.id).map((item) => ({ ...item, score_breakdown: JSON.parse(item.score_breakdown || '{}'), reasons: safeJsonArray(item.reasons), related_from: safeJsonArray(item.related_from) })) }));
  }

  function addRecallFeedback({ trace_id, memory_id, outcome, note = '' } = {}) {
    if (!db.prepare('SELECT id FROM recall_traces WHERE id=?').get(trace_id)) return { error: 'recall_trace_not_found', trace_id };
    const id = crypto.randomUUID();
    db.prepare('INSERT INTO recall_feedback (id,trace_id,memory_id,outcome,note,created_at) VALUES (?,?,?,?,?,?)')
      .run(id, trace_id, memory_id || null, outcome, String(note || ''), nowIso());
    return { id, trace_id, memory_id: memory_id || null, outcome, note: 'Feedback is telemetry only; it does not rewrite memories or personality.' };
  }

  function detectHeartbeatCandidates({ since_hours = 72, limit = 50, dry_run = true, extra_chunks = [] } = {}) {
    const since = new Date(Date.now() - clamp(since_hours, 1, 720) * 3600000).toISOString();
    const chunks = [...db.prepare(`SELECT * FROM event_chunks WHERE created_at>=? ORDER BY created_at DESC LIMIT ?`).all(since, clamp(limit, 1, 200) * 4), ...(Array.isArray(extra_chunks) ? extra_chunks : [])];
    const patterns = ['亲亲', '亲了', '亲你', '抱住', '抱抱', '摸摸', '摸你', '捏你', '蹭蹭', '想你', '爱你', '心跳', '脸红', '耳朵热'];
    const plans = [];
    for (const chunk of chunks) {
      const rawIds = Array.isArray(chunk.raw_event_ids)
        ? chunk.raw_event_ids
        : db.prepare('SELECT raw_event_id FROM chunk_events WHERE chunk_id=? ORDER BY position').all(chunk.id).map((row) => row.raw_event_id);
      const rawRows = rawIds.length ? db.prepare(`SELECT content FROM raw_events WHERE id IN (${rawIds.map(() => '?').join(',')})`).all(...rawIds) : [];
      const text = rawRows.map((row) => row.content || '').join('\n');
      const matched = patterns.filter((term) => text.includes(term));
      if (!matched.length) continue;
      const dedupeKey = hash(`heartbeat:${chunk.id}`);
      if (db.prepare('SELECT id FROM memory_candidates WHERE dedupe_key=?').get(dedupeKey)) continue;
      const isPrivate = ['private', 'intimate'].includes(chunk.channel);
      plans.push({
        id: crypto.randomUUID(), raw_event_ids: rawIds,
        source_chunk_ids: [chunk.id], dedupe_key: dedupeKey, source: chunk.source, channel: chunk.channel, speaker: '',
        summary: isPrivate ? `一段可能值得记住的亲密时刻（${chunk.start_time} 至 ${chunk.end_time}）` : `可能值得记住的关系时刻：${truncate(chunk.summary, 140)}`,
        suggested_category: isPrivate ? 'private_candidate' : 'relationship', candidate_type: 'relationship_moment',
        reason: `batch heartbeat detector: ${matched.slice(0, 6).join('/')}`, importance: Math.min(1, 0.65 + matched.length * 0.05),
        suggested_tags: ['heartbeat-candidate', 'review-required'], evidence_preview: isPrivate ? '私密原文仅保留在 raw_events' : truncate(chunk.summary, 180),
        relation_hints: ['emotional_link', 'same_event'],
      });
      if (plans.length >= clamp(limit, 1, 200)) break;
    }
    if (!dry_run) {
      const insert = db.prepare(`INSERT OR IGNORE INTO memory_candidates
        (id,raw_event_ids,dedupe_key,source,channel,speaker,summary,suggested_category,reason,confidence,status,created_at,updated_at,expires_at,source_chunk_ids,candidate_type,importance,suggested_tags,evidence_preview,relation_hints)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      const expires = new Date(Date.now() + 7 * 86400000).toISOString();
      for (const item of plans) insert.run(item.id, JSON.stringify(item.raw_event_ids), item.dedupe_key, item.source, item.channel, item.speaker, item.summary, item.suggested_category, item.reason, 0.62, 'pending', nowIso(), nowIso(), expires, JSON.stringify(item.source_chunk_ids), item.candidate_type, item.importance, JSON.stringify(item.suggested_tags), item.evidence_preview, JSON.stringify(item.relation_hints));
    }
    return { dry_run, scanned_chunks: chunks.length, planned_count: plans.length, written_count: dry_run ? 0 : plans.length, candidates: plans, note: 'Review-gated only; no formal memory is written and no BPM is fabricated.' };
  }

  function quarantineHeartbeatPollution({ dry_run = true, limit = 200 } = {}) {
    const rows = db.prepare(`SELECT id,category,source,status,protected FROM memories
      WHERE source='heartbeat_detector' AND deleted_at IS NULL AND COALESCE(status,'current')='current'
      ORDER BY created_at DESC LIMIT ?`).all(clamp(limit, 1, 1000));
    if (!dry_run) {
      const update = db.prepare("UPDATE memories SET status='review',lifecycle_bucket='quarantine',updated_at=? WHERE id=? AND source='heartbeat_detector' AND COALESCE(status,'current')='current'");
      for (const row of rows) update.run(nowIso(), row.id);
    }
    return { dry_run, planned_count: rows.length, quarantined_count: dry_run ? 0 : rows.length, memories: rows, note: 'Quarantine only: rows are never deleted and require human review.' };
  }

  async function runNap({ dry_run = true, limit = 25 } = {}) {
    const missingVectors = db.prepare(`SELECT id,category FROM memories WHERE deleted_at IS NULL AND superseded_by IS NULL AND COALESCE(status,'current')='current' AND embedding IS NULL ORDER BY created_at DESC LIMIT ?`).all(clamp(limit, 1, 100));
    const orphanRows = db.prepare(`SELECT m.id,m.category FROM memories m WHERE m.deleted_at IS NULL AND m.superseded_by IS NULL AND COALESCE(m.status,'current')='current' AND NOT EXISTS (SELECT 1 FROM memory_edges e WHERE e.source_id=m.id OR e.target_id=m.id) ORDER BY m.created_at DESC LIMIT ?`).all(clamp(limit, 1, 100));
    const relations = buildSafeRelations({ since_hours: 168, limit: clamp(limit, 1, 100), dry_run });
    let vectorsWritten = 0;
    const errors = [];
    const skipped = [];
    if (!embedText) skipped.push('vectors: embedding writer not configured');
    else if (!dry_run) {
      const update = db.prepare('UPDATE memories SET embedding=?,updated_at=? WHERE id=? AND embedding IS NULL');
      for (const row of missingVectors) {
        try {
          const memory = db.prepare('SELECT content FROM memories WHERE id=?').get(row.id);
          const embedding = await embedText(memory?.content || '');
          if (Array.isArray(embedding) && embedding.length) {
            vectorsWritten += update.run(JSON.stringify(embedding), nowIso(), row.id).changes;
          } else skipped.push(`vector:${row.id}: provider unavailable`);
        } catch (error) {
          errors.push(`vector:${row.id}:${error.message}`);
        }
      }
    }
    return { dry_run, ok: errors.length === 0, scanned_memories: missingVectors.length, vectors_written: vectorsWritten, missing_vectors: missingVectors, orphan_memories_scanned: orphanRows.length, orphan_memories: orphanRows, relations, skipped, errors, note: 'Nap never promotes durable memories.' };
  }

  function inspectDreamReadiness() {
    const last = db.prepare('SELECT * FROM dream_runs ORDER BY started_at DESC LIMIT 1').get() || null;
    const active = last?.status === 'running' && Date.now() - new Date(last.started_at).getTime() < 2 * 3600000;
    return { ready: !active, busy: !!active, schedule: 'existing patrol cron', last_run: last ? { ...last, step_results: safeJsonArray(last.step_results) } : null, pending_candidates: db.prepare("SELECT COUNT(*) AS c FROM memory_candidates WHERE status='pending'").get().c, open_chunks: db.prepare("SELECT COUNT(*) AS c FROM event_chunks WHERE status='open'").get().c };
  }

  function listDreamRuns({ limit = 20 } = {}) {
    return db.prepare('SELECT * FROM dream_runs ORDER BY started_at DESC LIMIT ?').all(clamp(limit, 1, 100)).map((row) => ({ ...row, step_results: safeJsonArray(row.step_results) }));
  }

  function refreshSpontaneous({ limit = 6, dry_run = true } = {}) {
    const rows = db.prepare(`
      SELECT m.*,e.arousal,e.valence FROM memories m LEFT JOIN e_axis_scores e ON e.memory_id=m.id
      WHERE m.deleted_at IS NULL AND m.superseded_by IS NULL AND COALESCE(m.status,'current')='current'
      AND (m.expires_at IS NULL OR m.expires_at>?) ORDER BY m.created_at DESC LIMIT 500
    `).all(nowIso()).map((row) => ({ ...row, metabolism: metabolicState(row, row) }));
    const selected = rows
      .map((row) => ({ row, score: row.metabolism.score * (0.75 + Math.min(0.5, row.metabolism.age_days / 180)) }))
      .sort((a, b) => b.score - a.score)
      .slice(0, clamp(limit, 1, 20));
    if (!dry_run) {
      db.prepare('DELETE FROM spontaneous_cache WHERE expires_at<?').run(nowIso());
      const expires = new Date(Date.now() + 24 * 3600000).toISOString();
      const upsert = db.prepare(`
        INSERT INTO spontaneous_cache (id,memory_id,reason,score,surfaced_count,last_surfaced_at,expires_at,created_at)
        VALUES (?,?,?,?,0,NULL,?,?) ON CONFLICT(memory_id) DO UPDATE SET reason=excluded.reason,score=excluded.score,expires_at=excluded.expires_at
      `);
      for (const item of selected) upsert.run(crypto.randomUUID(), item.row.id, `vitality=${item.row.metabolism.score}; drift=${item.row.metabolism.age_days}`, item.score, expires, nowIso());
    }
    return { dry_run, planned_count: selected.length, written_count: dry_run ? 0 : selected.length, items: selected.map((item) => ({ memory_id: item.row.id, category: item.row.category, score: Number(item.score.toFixed(4)) })) };
  }

  function surfaceSpontaneous({ limit = 1, consume = true, exclude_ids = [], no_repeat_rounds = 5 } = {}) {
    const excluded = new Set(exclude_ids || []);
    for (const row of db.prepare('SELECT memory_id FROM spontaneous_history ORDER BY surfaced_at DESC LIMIT ?').all(clamp(no_repeat_rounds, 1, 50))) excluded.add(row.memory_id);
    const rows = db.prepare(`
      SELECT m.*,c.score AS spontaneous_score,c.reason AS spontaneous_reason
      FROM spontaneous_cache c JOIN memories m ON m.id=c.memory_id
      WHERE c.expires_at>? AND m.deleted_at IS NULL AND m.superseded_by IS NULL AND COALESCE(m.status,'current')='current'
      ORDER BY c.score DESC,c.surfaced_count ASC LIMIT 100
    `).all(nowIso()).filter((row) => !excluded.has(row.id) && metabolicGate(row, 'surface').allowed).slice(0, clamp(limit, 1, 3));
    if (consume) {
      const ts = nowIso();
      for (const row of rows) {
        db.prepare('UPDATE spontaneous_cache SET surfaced_count=surfaced_count+1,last_surfaced_at=? WHERE memory_id=?').run(ts, row.id);
        db.prepare('INSERT INTO spontaneous_history (id,memory_id,surfaced_at) VALUES (?,?,?)').run(crypto.randomUUID(), row.id, ts);
      }
      db.prepare(`DELETE FROM spontaneous_history WHERE id NOT IN (SELECT id FROM spontaneous_history ORDER BY surfaced_at DESC LIMIT 100)`).run();
    }
    return rows.map((row) => ({ id: row.id, content: row.content, category: row.category, tags: safeJsonArray(row.tags), spontaneous_score: row.spontaneous_score, spontaneous_reason: row.spontaneous_reason }));
  }

  function buildCarryover({ since_hours = 72, tail_limit = 8, memory_limit = 10, include_private = false, max_chars = 9000 } = {}) {
    const since = new Date(Date.now() - clamp(since_hours, 1, 720) * 3600000).toISOString();
    let eventSql = 'SELECT * FROM raw_events WHERE timestamp>=?';
    const eventParams = [since];
    if (!include_private) eventSql += " AND channel NOT IN ('private','intimate')";
    eventSql += ' ORDER BY timestamp DESC LIMIT ?'; eventParams.push(clamp(tail_limit, 1, 30) * 4);
    const noise = /^(ok|好的|收到|测试|test|嗯|哈哈)+[。！!~ ]*$/i;
    const events = db.prepare(eventSql).all(...eventParams).filter((row) => !noise.test(String(row.content || '').trim())).slice(0, clamp(tail_limit, 1, 30)).reverse();
    let memorySql = `SELECT * FROM memories WHERE deleted_at IS NULL AND superseded_by IS NULL AND COALESCE(status,'current')='current'
      AND (expires_at IS NULL OR expires_at>?)`;
    const memoryParams = [nowIso()];
    if (!include_private) memorySql += " AND category NOT IN ('私藏','心动') AND source NOT LIKE '%intimate%'";
    memorySql += ' ORDER BY pinned DESC,activation_score DESC,created_at DESC LIMIT ?';
    memoryParams.push(clamp(memory_limit, 1, 30));
    const memories = db.prepare(memorySql).all(...memoryParams);
    const lines = ['[精炼交接]', ...memories.map((row) => `- ${row.category}：${truncate(row.content, 180)}`), '[最近自然尾巴]', ...events.map((row) => `- ${row.speaker || row.role}：${truncate(row.content, 160)}`)];
    return { text: lines.join('\n').slice(0, clamp(max_chars, 1000, 20000)), memories: memories.map((row) => row.id), raw_event_ids: events.map((row) => row.id) };
  }

  function consolidate({ since_hours = 24, source = 'all', channel = 'all', limit = 500, dry_run = true } = {}) {
    const since = new Date(Date.now() - clamp(since_hours, 1, 720) * 3600000).toISOString();
    let sql = `SELECT r.* FROM raw_events r LEFT JOIN chunk_events ce ON ce.raw_event_id=r.id WHERE ce.raw_event_id IS NULL AND r.timestamp>=?`;
    const params = [since];
    if (source !== 'all') { sql += ' AND r.source=?'; params.push(source); }
    if (channel !== 'all') { sql += ' AND r.channel=?'; params.push(channel); }
    sql += ' ORDER BY r.source,r.channel,r.session_id,r.timestamp LIMIT ?'; params.push(clamp(limit, 1, 5000));
    const rows = db.prepare(sql).all(...params);
    const chunks = [];
    let current = [];
    const flush = () => {
      if (!current.length) return;
      const privateChunk = ['private', 'intimate'].includes(current[0].channel);
      const ids = current.map((row) => row.id);
      chunks.push({
        id: crypto.randomUUID(), dedupe_key: hash(ids.sort().join('\0')), source: current[0].source || '', channel: current[0].channel || '', session_id: current[0].session_id || '',
        start_event_id: current[0].id, end_event_id: current.at(-1).id, start_time: current[0].timestamp, end_time: current.at(-1).timestamp,
        event_count: current.length, raw_event_ids: ids,
        summary: privateChunk ? `${current[0].speaker || '月亮'}在${current[0].source || '私密入口'}留下了${current.length}条私密消息（${current[0].timestamp}）` : current.map((row) => truncate(row.content, 80)).join(' / ').slice(0, 600),
      });
      current = [];
    };
    for (const row of rows) {
      const previous = current.at(-1);
      const boundary = previous && (row.source !== previous.source || row.channel !== previous.channel || row.session_id !== previous.session_id || new Date(row.timestamp) - new Date(previous.timestamp) > 30 * 60000 || current.length >= 30);
      if (boundary) flush();
      current.push(row);
    }
    flush();
    if (!dry_run) {
      const insertChunk = db.prepare(`INSERT OR IGNORE INTO event_chunks
        (id,dedupe_key,source,channel,session_id,start_event_id,end_event_id,start_time,end_time,event_count,summary,status,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      const link = db.prepare('INSERT OR IGNORE INTO chunk_events (chunk_id,raw_event_id,position) VALUES (?,?,?)');
      for (const chunk of chunks) {
        const result = insertChunk.run(chunk.id, chunk.dedupe_key, chunk.source, chunk.channel, chunk.session_id, chunk.start_event_id, chunk.end_event_id, chunk.start_time, chunk.end_time, chunk.event_count, chunk.summary, 'open', nowIso(), nowIso());
        if (!result.changes) continue;
        rows.filter((row) => row.source === chunk.source && row.channel === chunk.channel && row.session_id === chunk.session_id && row.timestamp >= chunk.start_time && row.timestamp <= chunk.end_time).forEach((row, index) => link.run(chunk.id, row.id, index));
      }
      db.prepare(`INSERT INTO consolidation_runs (id,source,channel,since_time,until_time,dry_run,raw_count,chunk_count,candidate_count,error,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`)
        .run(crypto.randomUUID(), source, channel, since, nowIso(), 0, rows.length, chunks.length, 0, null, nowIso());
    }
    return { dry_run, raw_count: rows.length, chunk_count: chunks.length, chunks };
  }

  function proposeChunkCandidates({ limit = 50, dry_run = true, extra_chunks = [], exclude_chunk_ids = [] } = {}) {
    const rows = [
      ...db.prepare("SELECT * FROM event_chunks WHERE status='open' ORDER BY created_at ASC LIMIT ?").all(Math.max(500, clamp(limit, 1, 200) * 10)),
      ...(Array.isArray(extra_chunks) ? extra_chunks : []),
    ];
    const existingCandidates = db.prepare('SELECT raw_event_ids,source_chunk_ids FROM memory_candidates').all();
    const existingRawSignatures = new Set(existingCandidates.map((row) => safeJsonArray(row.raw_event_ids).sort().join('\0')).filter(Boolean));
    const usedRawIds = new Set(existingCandidates.flatMap((row) => safeJsonArray(row.raw_event_ids)));
    const existingChunkIds = new Set(existingCandidates.flatMap((row) => safeJsonArray(row.source_chunk_ids)));
    const plans = [];
    const excludedChunkIds = new Set(exclude_chunk_ids || []);
    for (const chunk of rows) {
      if (excludedChunkIds.has(chunk.id)) continue;
      if (existingChunkIds.has(chunk.id)) continue;
      const type = candidateCategory(chunk);
      const rawIds = Array.isArray(chunk.raw_event_ids)
        ? chunk.raw_event_ids
        : db.prepare('SELECT raw_event_id FROM chunk_events WHERE chunk_id=? ORDER BY position').all(chunk.id).map((row) => row.raw_event_id);
      const rawSignature = [...rawIds].sort().join('\0');
      if (rawSignature && existingRawSignatures.has(rawSignature)) continue;
      if (rawIds.some((id) => usedRawIds.has(id))) continue;
      const dedupeKey = hash(`chunk:${chunk.id}:${type}`);
      if (db.prepare('SELECT id FROM memory_candidates WHERE dedupe_key=?').get(dedupeKey)) continue;
      const privateCandidate = type === 'private_candidate';
      plans.push({
        id: crypto.randomUUID(), raw_event_ids: rawIds,
        source_chunk_ids: [chunk.id], dedupe_key: dedupeKey, source: chunk.source, channel: chunk.channel, speaker: '',
        summary: privateCandidate ? `一段私密对话（${chunk.start_time} 至 ${chunk.end_time}，${chunk.event_count}条）` : chunk.summary,
        suggested_category: type, candidate_type: type, reason: 'nightly chunk proposal', importance: privateCandidate ? 0.7 : 0.55,
        suggested_tags: ['chunk-candidate'], evidence_preview: privateCandidate ? '私密原文仅保留在 raw_events' : truncate(chunk.summary, 220),
      });
      if (plans.length >= clamp(limit, 1, 200)) break;
    }
    if (!dry_run) {
      const insert = db.prepare(`INSERT OR IGNORE INTO memory_candidates
        (id,raw_event_ids,dedupe_key,source,channel,speaker,summary,suggested_category,reason,confidence,status,created_at,updated_at,expires_at,source_chunk_ids,candidate_type,importance,suggested_tags,evidence_preview)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
      const expires = new Date(Date.now() + 7 * 86400000).toISOString();
      for (const item of plans) insert.run(item.id, JSON.stringify(item.raw_event_ids), item.dedupe_key, item.source, item.channel, item.speaker, item.summary, item.suggested_category, item.reason, 0.65, 'pending', nowIso(), nowIso(), expires, JSON.stringify(item.source_chunk_ids), item.candidate_type, item.importance, JSON.stringify(item.suggested_tags), item.evidence_preview);
    }
    return { dry_run, planned_count: plans.length, written_count: dry_run ? 0 : plans.length, candidates: plans };
  }

  function edgeHealth() {
    const rows = db.prepare(`
      SELECT e.source_id,e.target_id,e.relation_type,e.status FROM memory_edges e
      LEFT JOIN memories s ON s.id=e.source_id LEFT JOIN memories t ON t.id=e.target_id
      WHERE s.id IS NULL OR t.id IS NULL OR s.deleted_at IS NOT NULL OR t.deleted_at IS NOT NULL
      OR s.superseded_by IS NOT NULL OR t.superseded_by IS NOT NULL OR COALESCE(s.status,'current')!='current' OR COALESCE(t.status,'current')!='current'
    `).all();
    return { issue_count: rows.length, issues: rows.slice(0, 100) };
  }

  function cleanupEdges({ dry_run = true, limit = 200 } = {}) {
    const health = edgeHealth();
    const planned = health.issues.slice(0, clamp(limit, 1, 1000));
    if (!dry_run) {
      const remove = db.prepare('DELETE FROM memory_edges WHERE source_id=? AND target_id=?');
      for (const edge of planned) remove.run(edge.source_id, edge.target_id);
    }
    return { dry_run, planned_count: planned.length, deleted_count: dry_run ? 0 : planned.length, issues: planned };
  }

  async function runNight({ since_hours = 24, source = 'all', channel = 'all', dry_run = true, save_report = true } = {}) {
    const dreamRunId = crypto.randomUUID();
    const startedAt = nowIso();
    if (!dry_run) {
      const active = db.prepare("SELECT id,started_at FROM dream_runs WHERE status='running' AND started_at>? ORDER BY started_at DESC LIMIT 1")
        .get(new Date(Date.now() - 2 * 3600000).toISOString());
      if (active) return { dry_run: false, status: 'busy', active_run: active, note: 'A recent night-dream run is still active; no second run was started.' };
    }
    if (!dry_run) db.prepare("INSERT INTO dream_runs (id,mode,status,dry_run,started_at,step_results,error) VALUES (?,'night_dream','running',0,?,'[]','')").run(dreamRunId, startedAt);
    try {
    const snapshot = createSnapshot({ reason: 'nightly LMC maintenance', dry_run });
    const expiredRows = db.prepare("SELECT id FROM memories WHERE deleted_at IS NULL AND protected=0 AND expires_at IS NOT NULL AND expires_at<?").all(nowIso());
    if (!dry_run) {
      const softDelete = db.prepare('UPDATE memories SET deleted_at=?,updated_at=? WHERE id=? AND deleted_at IS NULL');
      for (const row of expiredRows) softDelete.run(nowIso(), nowIso(), row.id);
    }
    const consolidation = consolidate({ since_hours, source, channel, dry_run });
    const nap = await runNap({ dry_run, limit: 25 });
    const heartbeat = detectHeartbeatCandidates({ since_hours, limit: 50, dry_run, extra_chunks: dry_run ? consolidation.chunks : [] });
    const candidates = proposeChunkCandidates({ limit: 50, dry_run, extra_chunks: dry_run ? consolidation.chunks : [], exclude_chunk_ids: heartbeat.candidates.flatMap((item) => item.source_chunk_ids) });
    if (!dry_run) db.prepare("UPDATE memory_candidates SET status='stale',updated_at=? WHERE status='pending' AND expires_at IS NOT NULL AND expires_at<?").run(nowIso(), nowIso());
    const relations = buildSafeRelations({ since_hours: Math.max(168, since_hours), limit: 200, dry_run });
    const zAxis = runZAxisAudit({ limit: 100, dry_run });
    const eAxis = scoreEAxis({ limit: 50, dry_run });
    const narrative = runNarrative({ period_type: 'both', dry_run });
    const spontaneous = refreshSpontaneous({ limit: 6, dry_run });
    const metabolism = inspectMetabolism({ limit: 50 });
    const otherIncubation = inspectOtherIncubation();
    const edges = edgeHealth();
    const status = edges.issue_count || zAxis.conflict_count ? 'needs_review' : 'ok';
    const summary = [
      `夜间闭环：raw ${consolidation.raw_count} 条，chunk ${consolidation.chunk_count} 段，候选 ${candidates.planned_count} 条，短期过期 ${expiredRows.length} 条。`,
      `小睡检查：缺向量 ${nap.scanned_memories} 条，孤立记忆 ${nap.orphan_memories_scanned} 条；心跳待审 ${heartbeat.planned_count} 条。`,
      `Y轴计划 ${relations.planned_count} 条；Z轴冲突 ${zAxis.conflict_count} 组；E轴评分 ${eAxis.count} 条。`,
      `叙事 ${narrative.planned_count} 条；other孵化建议 ${otherIncubation.suggestion_count} 条；自发缓存 ${spontaneous.planned_count} 条；坏边 ${edges.issue_count} 条。`,
    ].join('\n');
    let reportId = null;
    if (!dry_run && save_report) {
      reportId = crypto.randomUUID();
      db.prepare('INSERT INTO memory_patrol_reports (id,status,summary,payload,created_at) VALUES (?,?,?,?,?)')
        .run(reportId, status, summary, JSON.stringify({ snapshot, consolidation, nap, heartbeat, candidates, relations, z_axis: zAxis, e_axis: eAxis, narrative, other_incubation: otherIncubation, spontaneous, metabolism, edge_health: edges }), nowIso());
    }
    const stepResults = [
      ['snapshot', snapshot], ['consolidate', consolidation], ['nap', nap], ['heartbeat_detect', heartbeat],
      ['hippocampus', candidates], ['y_axis', relations], ['z_audit', zAxis], ['e_axis', eAxis],
      ['narrative', narrative], ['other_incubation', otherIncubation], ['spontaneous', spontaneous], ['patrol', edges],
    ].map(([name, output]) => ({ name, status: 'ok', output }));
    if (!dry_run) db.prepare('UPDATE dream_runs SET status=?,finished_at=?,step_results=? WHERE id=?').run(status, nowIso(), JSON.stringify(stepResults), dreamRunId);
    return { dry_run, dream_run_id: dry_run ? null : dreamRunId, report_id: reportId, status, summary, snapshot, expired: { planned_count: expiredRows.length, soft_deleted_count: dry_run ? 0 : expiredRows.length }, consolidation, nap, heartbeat, candidates, relations, z_axis: zAxis, e_axis: eAxis, narrative, other_incubation: otherIncubation, spontaneous, metabolism, edge_health: edges, note: 'No candidate was published as a formal memory.' };
    } catch (error) {
      if (!dry_run) db.prepare("UPDATE dream_runs SET status='error',finished_at=?,error=? WHERE id=?").run(nowIso(), String(error?.message || error), dreamRunId);
      throw error;
    }
  }

  return {
    createSnapshot, listSnapshots, restoreSnapshot, inspectMetabolism, scoreEAxis, runZAxisAudit,
    buildSafeRelations, addRelation, listRelationReviews, reviewRelation, runNarrative, listNarratives,
    refreshSpontaneous, surfaceSpontaneous, buildCarryover, consolidate, proposeChunkCandidates,
    metabolicGateForRecall, recordRecallTrace, listRecallTraces, addRecallFeedback,
    inspectOtherIncubation, detectHeartbeatCandidates, quarantineHeartbeatPollution, runNap, inspectDreamReadiness, listDreamRuns,
    edgeHealth, cleanupEdges, runNight,
  };
}

export function registerLmcClosureTools(mcp, z, service) {
  const tool = (name, description, schema, handler) => mcp.tool(name, description, schema, async (input) => ({
    content: [{ type: 'text', text: JSON.stringify(await handler(input || {}), null, 2) }],
  }));

  tool('create_memory_snapshot', 'Create a verified SQLite snapshot. dry_run previews without writing.', {
    reason: z.string().max(200).optional(), dry_run: z.boolean().default(true),
  }, service.createSnapshot);
  tool('list_memory_snapshots', 'List verified SQLite snapshots.', { limit: z.number().int().min(1).max(100).optional() }, service.listSnapshots);
  tool('restore_memory_snapshot', 'Plan or schedule a verified restore. Apply requires explicit environment approval and restart.', {
    id: z.string().min(1), dry_run: z.boolean().default(true),
  }, service.restoreSnapshot);
  tool('inspect_memory_metabolism', 'Read-only M-axis half-life and lifecycle audit.', { limit: z.number().int().min(1).max(200).optional() }, service.inspectMetabolism);
  tool('score_e_axis_v4', 'E-axis rules-v4 shadow scorer with technical-memory gating.', {
    memory_id: z.string().optional(), limit: z.number().int().min(1).max(200).optional(), dry_run: z.boolean().default(true),
  }, service.scoreEAxis);
  tool('run_z_axis_audit', 'Audit current fact conflicts. Never resolves facts automatically.', {
    limit: z.number().int().min(1).max(500).optional(), dry_run: z.boolean().default(true),
  }, service.runZAxisAudit);
  tool('build_safe_typed_relations', 'Build only deterministic safe Y-axis relations.', {
    since_hours: z.number().min(1).max(8760).optional(), limit: z.number().int().min(1).max(1000).optional(), dry_run: z.boolean().default(true),
  }, service.buildSafeRelations);
  tool('add_memory_relation', 'Add a safe relation or queue a risky relation for review.', {
    source_id: z.string().min(1), target_id: z.string().min(1), relation: z.string().optional(), weight: z.number().min(0.05).max(1).optional(), reason: z.string().max(500).optional(), dry_run: z.boolean().default(true),
  }, service.addRelation);
  tool('list_relation_reviews', 'List queued typed relations.', {
    status: z.enum(['pending', 'approved', 'rejected', 'all']).optional(), limit: z.number().int().min(1).max(200).optional(),
  }, service.listRelationReviews);
  tool('review_memory_relation', 'Approve or reject one queued relation.', {
    id: z.string().min(1), action: z.enum(['approve', 'reject']), review_note: z.string().max(500).optional(),
  }, service.reviewRelation);
  tool('run_narrative_sweep', 'Build deterministic weekly/monthly X-axis summaries without changing formal memories.', {
    period_type: z.enum(['week', 'month', 'both']).optional(), reference_time: z.string().optional(), thread: z.string().optional(), force: z.boolean().optional(), dry_run: z.boolean().default(true),
  }, service.runNarrative);
  tool('list_narrative_summaries', 'List X-axis narrative summaries.', {
    period_type: z.enum(['week', 'month', 'all']).optional(), thread: z.string().optional(), limit: z.number().int().min(1).max(100).optional(),
  }, service.listNarratives);
  tool('refresh_spontaneous_cache', 'Build a low-noise spontaneous-memory cache.', {
    limit: z.number().int().min(1).max(20).optional(), dry_run: z.boolean().default(true),
  }, service.refreshSpontaneous);
  tool('surface_spontaneous_memory', 'Surface cached memories while avoiding recent repeats.', {
    limit: z.number().int().min(1).max(3).optional(), consume: z.boolean().optional(), exclude_ids: z.array(z.string()).optional(), no_repeat_rounds: z.number().int().min(1).max(50).optional(),
  }, async (input) => ({ memories: service.surfaceSpontaneous(input) }));
  tool('build_refined_carryover', 'Read-only refined carryover. Does not change wakeup category rules.', {
    since_hours: z.number().min(1).max(720).optional(), tail_limit: z.number().int().min(1).max(30).optional(), memory_limit: z.number().int().min(1).max(30).optional(), include_private: z.boolean().optional(), max_chars: z.number().int().min(1000).max(20000).optional(),
  }, service.buildCarryover);
  tool('list_recall_traces', 'List explainable recall runs and per-hit layer/score evidence.', {
    trace_id: z.string().optional(), limit: z.number().int().min(1).max(100).optional(),
  }, service.listRecallTraces);
  tool('record_recall_feedback', 'Attach review feedback to a recall trace. Telemetry only; never rewrites memory or personality.', {
    trace_id: z.string().min(1), memory_id: z.string().optional(), outcome: z.enum(['useful', 'irrelevant', 'misleading']), note: z.string().max(500).optional(),
  }, service.addRecallFeedback);
  tool('inspect_other_incubation', 'Read-only X-axis three-stage incubation report for the other thread.', {
    observe_threshold: z.number().int().min(2).max(20).optional(), candidate_threshold: z.number().int().min(3).max(30).optional(), formal_threshold: z.number().int().min(5).max(50).optional(), formal_min_span_days: z.number().int().min(1).max(365).optional(), formal_min_hits: z.number().int().min(0).max(100).optional(),
  }, service.inspectOtherIncubation);
  tool('detect_heartbeat_candidates', 'Batch-detect relationship moments into review-gated candidates only.', {
    since_hours: z.number().min(1).max(720).optional(), limit: z.number().int().min(1).max(200).optional(), dry_run: z.boolean().default(true),
  }, service.detectHeartbeatCandidates);
  tool('quarantine_heartbeat_pollution', 'Find or quarantine legacy heartbeat_detector rows that bypassed candidate review. Never deletes.', {
    limit: z.number().int().min(1).max(1000).optional(), dry_run: z.boolean().default(true),
  }, service.quarantineHeartbeatPollution);
  tool('run_lmc_nap', 'Run lightweight vector/relation readiness checks without promoting memories.', {
    limit: z.number().int().min(1).max(100).optional(), dry_run: z.boolean().default(true),
  }, service.runNap);
  tool('inspect_dream_readiness', 'Inspect night-dream readiness and the most recent run.', {}, service.inspectDreamReadiness);
  tool('list_dream_runs', 'List observable night-dream runs and step results.', {
    limit: z.number().int().min(1).max(100).optional(),
  }, service.listDreamRuns);
  tool('run_lmc_night_maintenance', 'Run the complete LMC night loop without publishing formal memories.', {
    since_hours: z.number().min(1).max(720).optional(), source: z.string().optional(), channel: z.enum(['cc', 'daily', 'intimate', 'private', 'group', 'normal', 'all']).optional(), dry_run: z.boolean().default(true), save_report: z.boolean().optional(),
  }, service.runNight);
}
