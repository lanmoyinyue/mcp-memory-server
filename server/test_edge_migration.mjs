import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync as Database } from 'node:sqlite';
import { createEdgeMigrationService, excludeSupersededRelated } from './edge_migration.js';

const dataDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'edge-migration-'));
const dbPath = path.join(dataDir, 'test.db');
const db = new Database(dbPath);
const ts = '2026-08-10T00:00:00.000Z';

const addMemory = ({ id, status = 'current', superseded_by = null, protected: protectedFlag = 0, expires_at = null, deleted_at = null }) => {
  db.prepare(`
    INSERT INTO memories (id,status,superseded_by,protected,expires_at,deleted_at)
    VALUES (?,?,?,?,?,?)
  `).run(id, status, superseded_by, protectedFlag, expires_at, deleted_at);
};

const addEdge = ({ source, target, type = 'semantic', strength = 0.5, status = 'safe', reason = '' }) => {
  db.prepare(`
    INSERT INTO memory_edges
      (source_id,target_id,weight,created_at,relation_type,strength,status,reason,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?)
  `).run(source, target, strength, ts, type, strength, status, reason, ts);
};

try {
  db.exec(`
    CREATE TABLE memories (
      id TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'current',
      superseded_by TEXT,
      protected INTEGER NOT NULL DEFAULT 0,
      expires_at TEXT,
      deleted_at TEXT
    );
    CREATE TABLE memory_edges (
      source_id TEXT NOT NULL,
      target_id TEXT NOT NULL,
      weight REAL NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      relation_type TEXT NOT NULL DEFAULT 'semantic',
      strength REAL NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'safe',
      reason TEXT NOT NULL DEFAULT '',
      updated_at TEXT,
      PRIMARY KEY(source_id,target_id)
    );
  `);
  const service = createEdgeMigrationService({ db, now: () => ts });

  assert.deepEqual(
    excludeSupersededRelated([{ id: 'old' }, { id: 'live' }], ['old']),
    [{ id: 'live' }],
  );

  addMemory({ id: 'a' });
  addMemory({ id: 'old-1', status: 'historical', superseded_by: 'new-1' });
  addMemory({ id: 'new-1' });
  addEdge({ source: 'a', target: 'old-1', type: 'same_topic', strength: 0.61 });
  addEdge({ source: 'old-1', target: 'a', type: 'same_project', strength: 0.72 });
  const bothDirections = service.migrateOne({ old_id: 'old-1', successor_id: 'new-1' });
  assert.equal(bothDirections.migrated.length, 2);
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='a' AND target_id='new-1'").get());
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='new-1' AND target_id='a'").get());
  assert.equal(db.prepare("SELECT COUNT(*) AS c FROM memory_edges WHERE source_id='old-1' OR target_id='old-1'").get().c, 0);

  addMemory({ id: 'old-2', status: 'historical', superseded_by: 'new-2' });
  addMemory({ id: 'new-2' });
  addEdge({ source: 'old-2', target: 'a', type: 'same_topic', strength: 0.4 });
  addEdge({ source: 'new-2', target: 'a', type: 'same_project', strength: 0.9 });
  const dedupe = service.migrateOne({ old_id: 'old-2', successor_id: 'new-2' });
  assert.equal(dedupe.deduped.length, 1);
  const dedupedEdge = db.prepare("SELECT * FROM memory_edges WHERE source_id='new-2' AND target_id='a'").get();
  assert.equal(dedupedEdge.relation_type, 'same_project');
  assert.equal(dedupedEdge.strength, 0.9);
  assert.equal(dedupedEdge.weight, 0.9);
  assert.equal(db.prepare("SELECT COUNT(*) AS c FROM memory_edges WHERE source_id='old-2'").get().c, 0);

  addMemory({ id: 'old-3', status: 'historical', superseded_by: 'new-3' });
  addMemory({ id: 'new-3' });
  addEdge({ source: 'old-3', target: 'a', type: 'derived_from', strength: 0.8 });
  addEdge({ source: 'a', target: 'old-3', type: 'temporal_sequence', strength: 0.7 });
  const historicalRelations = service.migrateOne({ old_id: 'old-3', successor_id: 'new-3' });
  assert.deepEqual(
    new Set(historicalRelations.skipped.map(row => row.reason)),
    new Set(['derived_from_not_migrated', 'temporal_sequence_not_migrated']),
  );
  assert.equal(db.prepare("SELECT COUNT(*) AS c FROM memory_edges WHERE source_id='old-3' OR target_id='old-3'").get().c, 2);

  addMemory({ id: 'old-4', protected: 1 });
  addMemory({ id: 'new-4' });
  addEdge({ source: 'old-4', target: 'a', type: 'semantic' });
  const protectedCurrent = service.migrateOne({ old_id: 'old-4', successor_id: 'new-4' });
  assert.equal(protectedCurrent.eligible, false);
  assert.equal(protectedCurrent.reason, 'old_memory_not_confirmed_superseded');
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='old-4' AND target_id='a'").get());

  addMemory({ id: 'old-5', status: 'historical', superseded_by: 'new-5' });
  addMemory({ id: 'new-5' });
  addMemory({ id: 'other-old', status: 'historical', superseded_by: 'other-new' });
  addMemory({ id: 'other-new' });
  addEdge({ source: 'old-5', target: 'other-old', type: 'same_topic' });
  const historicalEndpoint = service.migrateOne({ old_id: 'old-5', successor_id: 'new-5' });
  assert.equal(historicalEndpoint.skipped[0].reason, 'other_endpoint_historical');
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='old-5' AND target_id='other-old'").get());

  addMemory({ id: 'old-6', status: 'historical', superseded_by: 'new-6' });
  addMemory({ id: 'new-6' });
  addEdge({ source: 'old-6', target: 'a', type: 'same_topic', strength: 0.5 });
  db.exec('BEGIN IMMEDIATE');
  const nested = service.migrateOne({ old_id: 'old-6', successor_id: 'new-6' });
  assert.equal(nested.migrated.length, 1);
  db.exec('ROLLBACK');
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='old-6' AND target_id='a'").get());
  assert.equal(db.prepare("SELECT COUNT(*) AS c FROM memory_edges WHERE source_id='new-6' AND target_id='a'").get().c, 0);

  const preview = service.runStoredMigration({ dry_run: true });
  const beforePreview = db.prepare('SELECT COUNT(*) AS c FROM memory_edges').get().c;
  const previewAgain = service.runStoredMigration({ dry_run: true });
  assert.equal(preview.plan_fingerprint, previewAgain.plan_fingerprint);
  assert.equal(db.prepare('SELECT COUNT(*) AS c FROM memory_edges').get().c, beforePreview);
  const mismatch = service.runStoredMigration({ dry_run: false, plan_fingerprint: 'wrong' });
  assert.equal(mismatch.error, 'plan_fingerprint_mismatch');
  assert.equal(db.prepare('SELECT COUNT(*) AS c FROM memory_edges').get().c, beforePreview);

  addMemory({ id: 'old-fail', status: 'historical', superseded_by: 'new-fail' });
  addMemory({ id: 'new-fail' });
  addEdge({ source: 'old-fail', target: 'a', type: 'same_topic', strength: 0.6 });
  db.exec(`
    CREATE TRIGGER fail_edge_migration
    BEFORE INSERT ON memory_edges
    WHEN NEW.reason LIKE 'migrated from superseded memory%'
    BEGIN
      SELECT RAISE(ABORT, 'forced migration failure');
    END;
  `);
  const failurePreview = service.runStoredMigration({ dry_run: true });
  assert.throws(
    () => service.runStoredMigration({ dry_run: false, plan_fingerprint: failurePreview.plan_fingerprint }),
    /forced migration failure/,
  );
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='old-fail' AND target_id='a'").get());
  assert.equal(db.prepare("SELECT COUNT(*) AS c FROM memory_edges WHERE source_id='new-fail' AND target_id='a'").get().c, 0);
  db.exec('DROP TRIGGER fail_edge_migration');

  const finalPreview = service.runStoredMigration({ dry_run: true });
  const applied = service.runStoredMigration({ dry_run: false, plan_fingerprint: finalPreview.plan_fingerprint });
  assert.equal(applied.error, undefined);
  assert.ok(applied.migrated_count >= 1);
  assert.ok(db.prepare("SELECT 1 FROM memory_edges WHERE source_id='new-fail' AND target_id='a'").get());

  const health = service.inspectHealth({ limit: 20 });
  assert.equal(typeof health.superseded_edge_migration_candidate_count, 'number');
  assert.equal(typeof health.superseded_edge_unmigratable_count, 'number');
  assert.ok(health.reason_counts.derived_from_not_migrated >= 1);

  console.log('Edge migration unit test passed');
} finally {
  try { db.close(); } catch {}
  await fsp.rm(dataDir, { recursive: true, force: true });
}
