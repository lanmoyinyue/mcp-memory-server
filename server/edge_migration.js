import crypto from 'node:crypto';

const MIGRATABLE_RELATION_TYPES = new Set([
  'semantic',
  'same_topic',
  'same_event',
  'same_project',
]);

const HISTORICAL_RELATION_REASONS = {
  derived_from: 'derived_from_not_migrated',
  temporal_sequence: 'temporal_sequence_not_migrated',
};

const normalizeRelationType = (value) => {
  const type = String(value || 'semantic').trim();
  return type === 'contradiction' ? 'contradicts' : (type || 'semantic');
};

const finiteStrength = (edge) => {
  const value = Number(edge?.strength ?? edge?.weight ?? 1);
  return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 1;
};

const stableHash = (value) => crypto.createHash('sha256')
  .update(JSON.stringify(value))
  .digest('hex');

export function excludeSupersededRelated(related = [], supersededIds = []) {
  if (!related.length || !supersededIds.length) return related;
  const superseded = new Set(supersededIds);
  return related.filter(memory => !superseded.has(memory.id));
}

export function createEdgeMigrationService({ db, now = () => new Date().toISOString() }) {
  let savepointCounter = 0;

  const memoryById = db.prepare('SELECT * FROM memories WHERE id = ?');
  const edgesForMemory = db.prepare(`
    SELECT * FROM memory_edges
    WHERE source_id = ? OR target_id = ?
    ORDER BY source_id, target_id
  `);
  const edgeByIds = db.prepare('SELECT * FROM memory_edges WHERE source_id = ? AND target_id = ?');
  const deleteEdge = db.prepare('DELETE FROM memory_edges WHERE source_id = ? AND target_id = ?');
  const insertEdge = db.prepare(`
    INSERT INTO memory_edges
      (source_id,target_id,weight,created_at,relation_type,strength,status,reason,updated_at)
    VALUES (?,?,?,?,?,?,?,?,?)
  `);
  const updateEquivalentEdge = db.prepare(`
    UPDATE memory_edges
    SET weight = ?, strength = ?, updated_at = ?
    WHERE source_id = ? AND target_id = ?
  `);

  function endpointState(memory, checkedAt) {
    if (!memory) return 'other_endpoint_missing';
    if (memory.deleted_at) return 'other_endpoint_deleted';
    if (memory.superseded_by || (memory.status || 'current') === 'historical') {
      return 'other_endpoint_historical';
    }
    if ((memory.status || 'current') !== 'current') return 'other_endpoint_not_current';
    if (memory.expires_at && memory.expires_at <= checkedAt) return 'other_endpoint_expired';
    return 'live';
  }

  function analyzeEdge(edge, oldId, newId, checkedAt) {
    const relationType = normalizeRelationType(edge.relation_type);
    const base = {
      old_source_id: edge.source_id,
      old_target_id: edge.target_id,
      source_id: edge.source_id,
      target_id: edge.target_id,
      old_id: oldId,
      successor_id: newId,
      relation_type: relationType,
      strength: finiteStrength(edge),
    };

    if ((edge.status || 'safe') !== 'safe') {
      return { ...base, action: 'skip', reason: 'edge_status_not_safe' };
    }
    if (HISTORICAL_RELATION_REASONS[relationType]) {
      return { ...base, action: 'skip', reason: HISTORICAL_RELATION_REASONS[relationType] };
    }
    if (!MIGRATABLE_RELATION_TYPES.has(relationType)) {
      return { ...base, action: 'skip', reason: 'relation_type_not_migratable' };
    }

    const incoming = edge.target_id === oldId;
    const otherId = incoming ? edge.source_id : edge.target_id;
    const sourceId = incoming ? otherId : newId;
    const targetId = incoming ? newId : otherId;
    const directed = { ...base, source_id: sourceId, target_id: targetId, other_id: otherId };
    if (sourceId === targetId) return { ...directed, action: 'skip', reason: 'self_loop' };

    const otherState = endpointState(memoryById.get(otherId), checkedAt);
    if (otherState !== 'live') return { ...directed, action: 'skip', reason: otherState };

    const existing = edgeByIds.get(sourceId, targetId);
    if (!existing) return { ...directed, action: 'migrate', reason: 'new_edge' };
    const existingType = normalizeRelationType(existing.relation_type);
    if ((existing.status || 'safe') !== 'safe' || !MIGRATABLE_RELATION_TYPES.has(existingType)) {
      return {
        ...directed,
        action: 'skip',
        reason: 'existing_edge_not_equivalent',
        existing_relation_type: existingType,
        existing_status: existing.status || 'safe',
      };
    }
    return {
      ...directed,
      action: 'dedupe',
      reason: 'equivalent_edge_exists',
      existing_relation_type: existingType,
      existing_strength: finiteStrength(existing),
    };
  }

  function inspectOne(oldId, newId, checkedAt = now()) {
    const oldMemory = memoryById.get(oldId);
    const successor = memoryById.get(newId);
    if (!oldMemory) return { eligible: false, reason: 'old_memory_missing', old_id: oldId, successor_id: newId, actions: [] };
    if ((oldMemory.status || 'current') !== 'historical' || oldMemory.superseded_by !== newId) {
      return { eligible: false, reason: 'old_memory_not_confirmed_superseded', old_id: oldId, successor_id: newId, actions: [] };
    }
    if (endpointState(successor, checkedAt) !== 'live') {
      return { eligible: false, reason: 'successor_not_live', old_id: oldId, successor_id: newId, actions: [] };
    }
    return {
      eligible: true,
      old_id: oldId,
      successor_id: newId,
      actions: edgesForMemory.all(oldId, oldId).map(edge => analyzeEdge(edge, oldId, newId, checkedAt)),
    };
  }

  function applyAction(action, checkedAt) {
    if (action.action === 'skip') return { kind: 'skipped', action };
    if (action.action === 'migrate') {
      insertEdge.run(
        action.source_id,
        action.target_id,
        action.strength,
        checkedAt,
        action.relation_type,
        action.strength,
        'safe',
        `migrated from superseded memory ${action.old_id}`,
        checkedAt,
      );
      deleteEdge.run(action.old_source_id, action.old_target_id);
      return { kind: 'migrated', action };
    }

    const finalStrength = Math.max(action.strength, action.existing_strength);
    updateEquivalentEdge.run(finalStrength, finalStrength, checkedAt, action.source_id, action.target_id);
    deleteEdge.run(action.old_source_id, action.old_target_id);
    return { kind: 'deduped', action: { ...action, final_strength: finalStrength } };
  }

  function migrateOne({ old_id, successor_id, checked_at = now() }) {
    const suffix = crypto.createHash('sha1').update(String(old_id)).digest('hex').slice(0, 10);
    const savepoint = `edge_migration_${++savepointCounter}_${suffix}`;
    db.exec(`SAVEPOINT ${savepoint}`);
    try {
      const inspection = inspectOne(old_id, successor_id, checked_at);
      if (!inspection.eligible) {
        db.exec(`RELEASE SAVEPOINT ${savepoint}`);
        return { ...inspection, migrated: [], deduped: [], skipped: [{ reason: inspection.reason, old_id, successor_id }] };
      }
      const result = { ...inspection, migrated: [], deduped: [], skipped: [] };
      for (const action of inspection.actions) {
        const applied = applyAction(action, checked_at);
        result[applied.kind].push(applied.action);
      }
      db.exec(`RELEASE SAVEPOINT ${savepoint}`);
      return result;
    } catch (error) {
      try { db.exec(`ROLLBACK TO SAVEPOINT ${savepoint}`); } catch {}
      try { db.exec(`RELEASE SAVEPOINT ${savepoint}`); } catch {}
      throw error;
    }
  }

  function buildPlan({ limit = 5000, checked_at = now() } = {}) {
    const successors = db.prepare(`
      SELECT h.id AS old_id, h.superseded_by AS successor_id
      FROM memories h
      JOIN memories s ON s.id = h.superseded_by
      WHERE h.deleted_at IS NULL
        AND COALESCE(h.status, 'current') = 'historical'
        AND h.superseded_by IS NOT NULL
        AND s.deleted_at IS NULL
        AND COALESCE(s.status, 'current') = 'current'
        AND s.superseded_by IS NULL
        AND (s.expires_at IS NULL OR s.expires_at > ?)
      ORDER BY h.id
      LIMIT ?
    `).all(checked_at, limit);

    const planned = [];
    const skipped = [];
    for (const pair of successors) {
      const inspection = inspectOne(pair.old_id, pair.successor_id, checked_at);
      for (const action of inspection.actions) {
        if (action.action === 'skip') skipped.push(action);
        else planned.push(action);
      }
    }
    const canonical = planned
      .map(action => ({
        action: action.action,
        old_source_id: action.old_source_id,
        old_target_id: action.old_target_id,
        source_id: action.source_id,
        target_id: action.target_id,
        relation_type: action.relation_type,
        strength: action.strength,
        existing_relation_type: action.existing_relation_type || null,
        existing_strength: action.existing_strength ?? null,
      }))
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    return {
      checked_at,
      successor_count: successors.length,
      successors,
      planned,
      skipped,
      planned_count: planned.length,
      skipped_count: skipped.length,
      plan_fingerprint: stableHash(canonical),
      note: 'When several superseded memories converge on the same target edge, apply may merge later migrate actions as dedupe actions.',
    };
  }

  function runStoredMigration({ dry_run = true, plan_fingerprint = '', limit = 5000 } = {}) {
    if (dry_run) return { dry_run: true, ...buildPlan({ limit }) };
    if (!plan_fingerprint) {
      return { dry_run: false, error: 'plan_fingerprint_required' };
    }

    db.exec('BEGIN IMMEDIATE');
    try {
      const plan = buildPlan({ limit });
      if (plan.plan_fingerprint !== plan_fingerprint) {
        db.exec('ROLLBACK');
        return {
          dry_run: false,
          error: 'plan_fingerprint_mismatch',
          expected: plan.plan_fingerprint,
          received: plan_fingerprint,
          current_plan: plan,
        };
      }
      const migrated = [];
      const deduped = [];
      const skipped = [];
      for (const pair of plan.successors) {
        const result = migrateOne({ ...pair, checked_at: plan.checked_at });
        migrated.push(...result.migrated);
        deduped.push(...result.deduped);
        skipped.push(...result.skipped);
      }
      db.exec('COMMIT');
      return {
        dry_run: false,
        plan_fingerprint,
        migrated_count: migrated.length,
        deduped_count: deduped.length,
        skipped_count: skipped.length,
        migrated,
        deduped,
        skipped,
      };
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch {}
      throw error;
    }
  }

  function inspectHealth({ sample_limit = 50, limit = sample_limit } = {}) {
    const plan = buildPlan({ limit: 5000 });
    const reasonCounts = {};
    for (const item of plan.skipped) reasonCounts[item.reason] = (reasonCounts[item.reason] || 0) + 1;
    return {
      checked_at: plan.checked_at,
      superseded_edge_migration_candidate_count: plan.planned_count,
      superseded_edge_unmigratable_count: plan.skipped_count,
      reason_counts: reasonCounts,
      samples: plan.skipped.slice(0, Math.max(1, limit)),
      note: 'Read-only relation-integrity metrics; excluded from temporalIssueCount.',
    };
  }

  return {
    buildPlan,
    inspectHealth,
    inspectOne,
    migrateOne,
    runStoredMigration,
  };
}

export { MIGRATABLE_RELATION_TYPES };
