import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync as Database } from 'node:sqlite';
import {
  createLmcClosureService,
  installLmcClosureSchema,
  prepareMemoryStorage,
  redactForRemote,
} from './lmc_closure.js';

const dataDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'ke-lmc-closure-'));
const dbPath = path.join(dataDir, 'memories.db');

try {
  prepareMemoryStorage({ dataDir, dbPath });
  let db = new Database(dbPath);
  db.exec(`
    CREATE TABLE memories (
      id TEXT PRIMARY KEY, content TEXT NOT NULL, category TEXT NOT NULL,
      tags TEXT NOT NULL DEFAULT '[]', source TEXT NOT NULL DEFAULT '', mood TEXT,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, expires_at TEXT,
      protected INTEGER NOT NULL DEFAULT 0, activation_score REAL NOT NULL DEFAULT 0,
      superseded_by TEXT, status TEXT NOT NULL DEFAULT 'current'
    );
    CREATE TABLE e_axis_scores (
      memory_id TEXT PRIMARY KEY, valence REAL, arousal REAL, tension REAL,
      confidence REAL, risk_level REAL, urgency REAL,
      scorer_version TEXT NOT NULL DEFAULT 'rules-v1', shadow INTEGER NOT NULL DEFAULT 1,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE raw_events (
      id TEXT PRIMARY KEY, session_id TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT '',
      channel TEXT NOT NULL DEFAULT 'normal', role TEXT NOT NULL, speaker TEXT NOT NULL DEFAULT '',
      content TEXT NOT NULL, timestamp TEXT NOT NULL
    );
    CREATE TABLE memory_edges (
      source_id TEXT NOT NULL, target_id TEXT NOT NULL, weight REAL NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL, relation_type TEXT NOT NULL DEFAULT 'semantic',
      strength REAL NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'safe',
      reason TEXT NOT NULL DEFAULT '', updated_at TEXT, PRIMARY KEY(source_id,target_id)
    );
    CREATE TABLE event_chunks (
      id TEXT PRIMARY KEY, dedupe_key TEXT NOT NULL UNIQUE, source TEXT NOT NULL DEFAULT '',
      channel TEXT NOT NULL DEFAULT '', session_id TEXT NOT NULL DEFAULT '', start_event_id TEXT,
      end_event_id TEXT, start_time TEXT NOT NULL, end_time TEXT NOT NULL,
      event_count INTEGER NOT NULL DEFAULT 0, summary TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'open', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE TABLE chunk_events (chunk_id TEXT NOT NULL, raw_event_id TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(chunk_id,raw_event_id));
    CREATE TABLE consolidation_runs (
      id TEXT PRIMARY KEY, source TEXT NOT NULL DEFAULT '', channel TEXT NOT NULL DEFAULT '',
      since_time TEXT, until_time TEXT, dry_run INTEGER NOT NULL DEFAULT 1,
      raw_count INTEGER NOT NULL DEFAULT 0, chunk_count INTEGER NOT NULL DEFAULT 0,
      candidate_count INTEGER NOT NULL DEFAULT 0, error TEXT, created_at TEXT NOT NULL
    );
    CREATE TABLE memory_candidates (
      id TEXT PRIMARY KEY, raw_event_ids TEXT NOT NULL DEFAULT '[]', dedupe_key TEXT NOT NULL UNIQUE,
      source TEXT NOT NULL DEFAULT '', channel TEXT NOT NULL DEFAULT '', speaker TEXT NOT NULL DEFAULT '',
      summary TEXT NOT NULL, suggested_category TEXT NOT NULL DEFAULT 'daily', reason TEXT NOT NULL DEFAULT '',
      confidence REAL NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL, expires_at TEXT, source_chunk_ids TEXT NOT NULL DEFAULT '[]',
      candidate_type TEXT NOT NULL DEFAULT '', importance REAL NOT NULL DEFAULT 0,
      suggested_tags TEXT NOT NULL DEFAULT '[]', evidence_preview TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE z_conflict_audits (
      id TEXT PRIMARY KEY, fact_key TEXT NOT NULL, stale_id TEXT, current_id TEXT,
      protected_ids TEXT NOT NULL DEFAULT '[]', reason TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    );
    CREATE TABLE memory_patrol_reports (
      id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'ok', summary TEXT NOT NULL DEFAULT '',
      payload TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL
    );
  `);
  installLmcClosureSchema(db);
  const service = createLmcClosureService({ db, dataDir, dbPath });
  const ts = new Date().toISOString();
  db.prepare('INSERT INTO memories (id,content,category,tags,created_at,updated_at) VALUES (?,?,?,?,?,?)')
    .run('m1', '快照前的真实内容', 'deep', '[]', ts, ts);
  db.prepare('INSERT INTO memories (id,content,category,tags,created_at,updated_at) VALUES (?,?,?,?,?,?)')
    .run('m2', '同分类但没有共同证据的另一条内容', 'deep', '[]', ts, ts);
  db.prepare('INSERT INTO memories (id,content,category,tags,created_at,updated_at) VALUES (?,?,?,?,?,?)')
    .run('m3', '只有日期型标签的内容一', 'diary', '["第78天"]', ts, ts);
  db.prepare('INSERT INTO memories (id,content,category,tags,created_at,updated_at) VALUES (?,?,?,?,?,?)')
    .run('m4', '只有日期型标签的内容二', 'diary', '["第78天"]', ts, ts);
  const noBroadCategoryEdges = service.buildSafeRelations({ since_hours: 24, limit: 20, dry_run: true });
  assert.equal(noBroadCategoryEdges.planned_count, 0, JSON.stringify(noBroadCategoryEdges, null, 2));

  const snapshot = service.createSnapshot({ reason: 'restore test', dry_run: false });
  assert.ok(fs.existsSync(snapshot.snapshot_path));
  db.prepare('UPDATE memories SET content=? WHERE id=?').run('被改坏的内容', 'm1');

  process.env.MEMORY_ALLOW_SNAPSHOT_RESTORE = 'true';
  const scheduled = service.restoreSnapshot({ id: snapshot.id, dry_run: false });
  assert.equal(scheduled.scheduled.id, snapshot.id);
  db.close();

  const restored = prepareMemoryStorage({ dataDir, dbPath });
  assert.equal(restored.restored.snapshot_path, snapshot.snapshot_path);
  db = new Database(dbPath);
  assert.equal(db.prepare('SELECT content FROM memories WHERE id=?').get('m1').content, '快照前的真实内容');
  db.close();

  const secret = 'token=pa-abcdefghijklmnopqrstuvwxyz123456';
  const redacted = redactForRemote(`普通文字 ${secret}`);
  assert.ok(!redacted.includes('abcdefghijklmnopqrstuvwxyz'));
  assert.ok(redacted.includes('[REDACTED]'));

  console.log('LMC closure snapshot/restore test passed');
} finally {
  delete process.env.MEMORY_ALLOW_SNAPSHOT_RESTORE;
  await fsp.rm(dataDir, { recursive: true, force: true });
}
