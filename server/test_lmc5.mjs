import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync as Database } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dataDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mcp-memory-lmc5-'));
const port = 39000 + Math.floor(Math.random() * 1000);

const child = spawn(process.execPath, ['server.js'], {
  cwd: root,
  env: {
    ...process.env,
    PORT: String(port),
    DATA_DIR: dataDir,
    BACKUP_TOKEN: '',
    VOYAGE_API_KEY: '',
  },
  stdio: ['ignore', 'pipe', 'pipe'],
});

let stdout = '';
let stderr = '';
child.stdout.on('data', d => { stdout += d.toString(); });
child.stderr.on('data', d => { stderr += d.toString(); });

async function waitForServer() {
  const deadline = Date.now() + 8000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/api/stats`);
      if (res.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  throw new Error(`server did not start\nstdout:\n${stdout}\nstderr:\n${stderr}`);
}

async function callTool(client, name, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  const text = result.content?.[0]?.text || '{}';
  return JSON.parse(text);
}

async function callToolText(client, name, args = {}) {
  const result = await client.callTool({ name, arguments: args });
  return result.content?.[0]?.text || '';
}

try {
  await waitForServer();
  const transport = new StreamableHTTPClientTransport(new URL(`http://127.0.0.1:${port}/mcp`));
  const client = new Client({ name: 'lmc5-local-test', version: '1.0.0' });
  await client.connect(transport);

  const tools = await client.listTools();
  const toolNames = tools.tools.map(t => t.name);
  for (const name of [
    'consolidate_raw_events',
    'list_event_chunks',
    'propose_chunk_candidates',
    'upsert_memory_relation',
    'list_z_audits',
    'score_e_axis_shadow',
    'recall_lmc',
    'build_wakeup_context',
    'run_memory_patrol',
    'list_memory_patrol_reports',
    'batch_review_memory_candidates_by_filter',
    'promote_memory_candidates',
    'list_review_memories',
    'publish_review_memories',
    'create_memory_snapshot',
    'list_memory_snapshots',
    'restore_memory_snapshot',
    'inspect_memory_metabolism',
    'score_e_axis_v4',
    'run_z_axis_audit',
    'build_safe_typed_relations',
    'add_memory_relation',
    'list_relation_reviews',
    'review_memory_relation',
    'run_narrative_sweep',
    'list_narrative_summaries',
    'refresh_spontaneous_cache',
    'surface_spontaneous_memory',
    'build_refined_carryover',
    'run_lmc_night_maintenance',
  ]) {
    assert.ok(toolNames.includes(name), `missing MCP tool: ${name}`);
  }

  const protectedFact = await callTool(client, 'write_memory', {
    content: '克的身份事实：月亮是重要关系锚点，不能被自动覆盖。',
    category: 'anchor',
    fact_key: 'identity:moon-anchor',
    tags: ['测试'],
    protected: true,
  });
  assert.ok(protectedFact.saved?.id);

  const conflict = await callTool(client, 'write_memory', {
    content: '克的身份事实：这是一条冲突的新事实。',
    category: 'daily',
    fact_key: 'identity:moon-anchor',
  });
  assert.equal(conflict.error, 'protected_fact_conflict');
  assert.ok(conflict.audit_id);

  const audits = await callTool(client, 'list_z_audits', { status: 'pending' });
  assert.ok(audits.audits.some(a => a.id === conflict.audit_id));

  const factV1 = await callTool(client, 'write_memory', {
    content: '项目状态：第一版还没完成。',
    category: 'work',
    fact_key: 'project:lmc-status',
  });
  const factV2 = await callTool(client, 'write_memory', {
    content: '项目状态：第二版已经完成本地实现。',
    category: 'work',
    fact_key: 'project:lmc-status',
  });
  assert.deepEqual(factV2.superseded_ids, [factV1.saved.id]);

  const fakeHeart = await callTool(client, 'write_memory', {
    content: '测试 E 轴：假装心动，没成功，也不紧急。',
    category: 'diary',
    tags: ['情绪测试'],
  });
  const realHeart = await callTool(client, 'write_memory', {
    content: '测试 E 轴：真的心动，项目成功，现在很紧急。',
    category: 'diary',
    tags: ['情绪测试'],
  });
  const riskMemory = await callTool(client, 'write_memory', {
    content: '测试 E 轴：这里涉及越权和红线，必须谨慎。',
    category: 'diary',
    tags: ['情绪测试'],
  });
  const technicalMemory = await callTool(client, 'write_memory', {
    content: '测试 E 轴：修复 token 鉴权和数据库部署。',
    category: 'work',
    tags: ['测试'],
  });
  const fakeScore = await callTool(client, 'score_e_axis_shadow', { memory_id: fakeHeart.saved.id });
  const realScore = await callTool(client, 'score_e_axis_shadow', { memory_id: realHeart.saved.id });
  const riskScore = await callTool(client, 'score_e_axis_shadow', { memory_id: riskMemory.saved.id });
  const technicalScore = await callTool(client, 'score_e_axis_shadow', { memory_id: technicalMemory.saved.id });
  assert.ok(fakeScore.scores[0].valence < 0, JSON.stringify(fakeScore, null, 2));
  assert.equal(fakeScore.scores[0].urgency, 0.2, JSON.stringify(fakeScore, null, 2));
  assert.ok(realScore.scores[0].valence > 0, JSON.stringify(realScore, null, 2));
  assert.equal(realScore.scores[0].urgency, 0.75, JSON.stringify(realScore, null, 2));
  assert.equal(realScore.scores[0].scorer_version, 'rules-v4');
  assert.ok(riskScore.scores[0].risk_level > 0.6, JSON.stringify(riskScore, null, 2));
  assert.equal(technicalScore.scores.length, 0, JSON.stringify(technicalScore, null, 2));
  assert.equal(technicalScore.skipped[0].reason, 'technical_without_e_axis_tag');

  const currentRead = await callTool(client, 'read_memories', { keyword: '项目状态', limit: 10 });
  assert.equal(currentRead.length, 1);
  assert.equal(currentRead[0].id, factV2.saved.id);

  const historicalRead = await callTool(client, 'read_memories', { keyword: '项目状态', include_superseded: true, limit: 10 });
  assert.equal(historicalRead.length, 2);

  await callTool(client, 'log_raw_event', {
    session_id: 'lmc-test',
    source: 'kechat-light',
    channel: 'normal',
    role: 'user',
    speaker: 'moon',
    content: '测试 LMC 分块：今天把 raw events 切成片段。',
  });
  await callTool(client, 'log_raw_event', {
    session_id: 'lmc-test',
    source: 'kechat-light',
    channel: 'normal',
    role: 'assistant',
    speaker: 'ke',
    content: '收到，先形成候选，不直接写正式记忆。',
  });
  await callTool(client, 'log_raw_event', {
    session_id: 'lmc-test-private',
    source: 'telegram',
    channel: 'intimate',
    role: 'user',
    speaker: 'moon',
    content: '这条私密原话不应该进入 chunk summary。',
  });

  const chunks = await callTool(client, 'consolidate_raw_events', {
    dry_run: false,
    since_hours: 24,
    max_events_per_chunk: 5,
  });
  assert.ok(chunks.chunk_count >= 2, JSON.stringify(chunks, null, 2));
  assert.ok(chunks.chunks.some(c => c.channel === 'intimate' && !c.summary.includes('私密原话')), JSON.stringify(chunks, null, 2));

  const listedChunks = await callTool(client, 'list_event_chunks', { status: 'open', limit: 10 });
  assert.ok(listedChunks.count >= 2);

  const candidates = await callTool(client, 'propose_chunk_candidates', { dry_run: false, limit: 10 });
  assert.ok(candidates.proposed_count >= 1, JSON.stringify(candidates, null, 2));
  assert.ok(candidates.candidates.every(c => c.source_chunk_ids.length >= 1), JSON.stringify(candidates, null, 2));

  const rawCandidateEvent = await callTool(client, 'log_raw_event', {
    session_id: 'lmc-candidate-duplicate-guard',
    source: 'kechat-light',
    channel: 'normal',
    role: 'user',
    speaker: 'moon',
    content: '请记住候选重复护栏测试：这个 raw 事件已经生成候选，不要再从 chunk 重复生成。',
  });
  const rawCandidates = await callTool(client, 'propose_memory_candidates', {
    dry_run: false,
    since_hours: 24,
    source: 'kechat-light',
    channel: 'normal',
    limit: 10,
  });
  assert.ok(rawCandidates.candidates.some(c => c.raw_event_ids.includes(rawCandidateEvent.id)), JSON.stringify(rawCandidates, null, 2));
  await callTool(client, 'consolidate_raw_events', {
    dry_run: false,
    since_hours: 24,
    source: 'kechat-light',
    channel: 'normal',
    max_events_per_chunk: 5,
    silence_gap_minutes: 30,
  });
  const duplicateChunkCandidates = await callTool(client, 'propose_chunk_candidates', { dry_run: true, status: 'open', limit: 50 });
  assert.ok(!duplicateChunkCandidates.candidates.some(c => c.raw_event_ids.includes(rawCandidateEvent.id)), JSON.stringify(duplicateChunkCandidates, null, 2));

  const privateRaw = await callTool(client, 'log_raw_event', {
    session_id: 'lmc-private-raw-default-skip',
    source: 'privacy-skip-test',
    channel: 'private',
    role: 'user',
    speaker: 'moon',
    content: '这条私密 raw 默认不应该逐条生成候选。',
  });
  const privateDefault = await callTool(client, 'propose_memory_candidates', {
    dry_run: true,
    since_hours: 24,
    source: 'privacy-skip-test',
    channel: 'private',
    limit: 10,
  });
  assert.equal(privateDefault.proposed_count, 0, JSON.stringify(privateDefault, null, 2));
  const privateExplicit = await callTool(client, 'propose_memory_candidates', {
    dry_run: true,
    since_hours: 24,
    source: 'privacy-skip-test',
    channel: 'private',
    include_private_raw: true,
    limit: 10,
  });
  assert.ok(privateExplicit.candidates.some(c => c.raw_event_ids.includes(privateRaw.id)), JSON.stringify(privateExplicit, null, 2));

  const starvedOld = await callTool(client, 'log_raw_event', {
    session_id: 'lmc-candidate-starvation',
    source: 'starvation-test',
    channel: 'normal',
    role: 'user',
    speaker: 'moon',
    content: '请记住候选扫描饥饿测试：这条旧一点的 raw 事件不能被新候选挡住。',
  });
  const starvedNew = [];
  for (let i = 0; i < 5; i++) {
    starvedNew.push(await callTool(client, 'log_raw_event', {
      session_id: 'lmc-candidate-starvation',
      source: 'starvation-test',
      channel: 'normal',
      role: 'user',
      speaker: 'moon',
      content: `请记住候选扫描饥饿测试：这条较新的 raw 事件 ${i} 已经有候选。`,
    }));
  }
  const starvationDb = new Database(path.join(dataDir, 'memories.db'));
  const setRawTs = starvationDb.prepare('UPDATE raw_events SET timestamp = ? WHERE id = ?');
  const starvationBase = Date.now();
  setRawTs.run(new Date(starvationBase - 60 * 60 * 1000).toISOString(), starvedOld.id);
  starvedNew.forEach((event, index) => {
    setRawTs.run(new Date(starvationBase - (5 - index) * 60 * 1000).toISOString(), event.id);
  });
  const fakeCandidate = starvationDb.prepare(
    'INSERT INTO memory_candidates (id,raw_event_ids,dedupe_key,source,channel,speaker,summary,suggested_category,reason,confidence,status,created_at,updated_at,expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
  );
  const fakeNow = new Date().toISOString();
  starvedNew.forEach((event, index) => fakeCandidate.run(
    `fake-starvation-${index}`,
    JSON.stringify([event.id]),
    `fake-starvation-${index}`,
    'starvation-test',
    'normal',
    'moon',
    `fake candidate ${index}`,
    'preference_candidate',
    'already covered',
    0.9,
    'pending',
    fakeNow,
    fakeNow,
    null
  ));
  starvationDb.close();
  const starvationCandidates = await callTool(client, 'propose_memory_candidates', {
    dry_run: true,
    since_hours: 24,
    source: 'starvation-test',
    channel: 'normal',
    limit: 1,
  });
  assert.ok(starvationCandidates.candidates.some(c => c.raw_event_ids.includes(starvedOld.id)), JSON.stringify(starvationCandidates, null, 2));

  const batchPreview = await callTool(client, 'batch_review_memory_candidates_by_filter', {
    dry_run: true,
    match_status: 'pending',
    target_status: 'rejected',
    source: 'starvation-test',
    channel: 'normal',
    limit: 2,
  });
  assert.equal(batchPreview.matched_count, 2, JSON.stringify(batchPreview, null, 2));
  assert.equal(batchPreview.updated_count, 0, JSON.stringify(batchPreview, null, 2));
  const batchUpdate = await callTool(client, 'batch_review_memory_candidates_by_filter', {
    dry_run: false,
    match_status: 'pending',
    target_status: 'rejected',
    source: 'starvation-test',
    channel: 'normal',
    limit: 2,
    review_note: 'local filter batch test',
  });
  assert.equal(batchUpdate.updated_count, 2, JSON.stringify(batchUpdate, null, 2));
  const rejectedPreview = await callTool(client, 'batch_review_memory_candidates_by_filter', {
    dry_run: true,
    match_status: 'rejected',
    target_status: 'pending',
    source: 'starvation-test',
    channel: 'normal',
    limit: 10,
  });
  assert.ok(rejectedPreview.matched_count >= 2, JSON.stringify(rejectedPreview, null, 2));

  const promoDb = new Database(path.join(dataDir, 'memories.db'));
  const promoChunk = promoDb.prepare('SELECT chunk_id FROM chunk_events WHERE raw_event_id = ? LIMIT 1').get(rawCandidateEvent.id);
  assert.ok(promoChunk?.chunk_id, 'promotion test needs a chunk evidence id');
  const insertCandidate = promoDb.prepare(`
    INSERT INTO memory_candidates
      (id,raw_event_ids,source_chunk_ids,dedupe_key,source,channel,speaker,summary,suggested_category,candidate_type,reason,confidence,importance,suggested_tags,evidence_preview,relation_hints,status,created_at,updated_at,reviewed_at,review_note,expires_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  `);
  const promoNow = new Date().toISOString();
  const promoSummary = 'candidate promotion durable work summary unique alpha';
  insertCandidate.run(
    'promo-work-1',
    JSON.stringify([rawCandidateEvent.id]),
    JSON.stringify([promoChunk.chunk_id]),
    'promo-work-1',
    'promotion-test',
    'normal',
    'moon',
    promoSummary,
    'work',
    'work',
    'accepted work candidate',
    0.9,
    0.8,
    JSON.stringify(['work']),
    'preview',
    JSON.stringify([
      { target_id: protectedFact.saved.id, relation_type: 'same_project', strength: 0.8, reason: 'safe relation hint test' },
      { target_id: factV2.saved.id, relation_type: 'emotional_link', strength: 0.6, reason: 'review relation hint test' },
    ]),
    'accepted',
    promoNow,
    promoNow,
    promoNow,
    'accepted for promotion test',
    null
  );
  promoDb.close();

  const promoteDry = await callTool(client, 'promote_memory_candidates', { ids: ['promo-work-1'], dry_run: true });
  assert.equal(promoteDry.would_promote_count, 1, JSON.stringify(promoteDry, null, 2));
  assert.equal(promoteDry.would_skip_count, 0, JSON.stringify(promoteDry, null, 2));
  assert.equal(promoteDry.would_write_relations, 1, JSON.stringify(promoteDry, null, 2));
  assert.equal(promoteDry.would_queue_review_relations, 1, JSON.stringify(promoteDry, null, 2));
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    assert.equal(dbCheck.prepare('SELECT COUNT(*) AS c FROM memories WHERE content = ?').get(promoSummary).c, 0);
    assert.equal(dbCheck.prepare('SELECT status FROM memory_candidates WHERE id = ?').get('promo-work-1').status, 'accepted');
    dbCheck.close();
  }

  const promoteApply = await callTool(client, 'promote_memory_candidates', { ids: ['promo-work-1'], dry_run: false });
  assert.equal(promoteApply.promoted_count, 1, JSON.stringify(promoteApply, null, 2));
  assert.equal(promoteApply.relations_written, 1, JSON.stringify(promoteApply, null, 2));
  assert.equal(promoteApply.review_relations_queued, 1, JSON.stringify(promoteApply, null, 2));
  const promotedId = promoteApply.created_memory_ids[0];
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    const mem = dbCheck.prepare('SELECT * FROM memories WHERE id = ?').get(promotedId);
    assert.equal(mem.status, 'review');
    assert.equal(mem.expires_at, null);
    assert.equal(mem.category, 'work');
    assert.deepEqual(JSON.parse(mem.evidence_raw_ids), [rawCandidateEvent.id]);
    assert.deepEqual(JSON.parse(mem.evidence_chunk_ids), [promoChunk.chunk_id]);
    const cand = dbCheck.prepare('SELECT * FROM memory_candidates WHERE id = ?').get('promo-work-1');
    assert.equal(cand.status, 'merged');
    assert.equal(cand.promoted_memory_id, promotedId);
    assert.ok(cand.promoted_at);
    const safeEdge = dbCheck.prepare('SELECT * FROM memory_edges WHERE source_id = ? AND target_id = ?').get(promotedId, protectedFact.saved.id);
    assert.equal(safeEdge.status, 'safe');
    assert.equal(safeEdge.relation_type, 'same_project');
    const reviewEdge = dbCheck.prepare('SELECT * FROM memory_edges WHERE source_id = ? AND target_id = ?').get(promotedId, factV2.saved.id);
    assert.equal(reviewEdge.status, 'review');
    assert.equal(reviewEdge.relation_type, 'emotional_link');
    dbCheck.close();
  }

  const evidence = await callTool(client, 'get_evidence', { memory_id: promotedId });
  assert.equal(evidence.count, 1, JSON.stringify(evidence, null, 2));
  assert.equal(evidence.chunk_count, 1, JSON.stringify(evidence, null, 2));

  const readReviewText = await callToolText(client, 'read_memories', { keyword: promoSummary, limit: 5 });
  assert.equal(readReviewText, 'No memories found.');
  const searchReviewText = await callToolText(client, 'search_memories', { query: promoSummary });
  assert.ok(searchReviewText.includes('No results'), searchReviewText);
  const hybridReviewText = await callToolText(client, 'hybrid_search', { query: promoSummary, limit: 5 });
  assert.ok(hybridReviewText.includes('No results'), hybridReviewText);
  const recallReview = await callTool(client, 'recall_lmc', { query: promoSummary, graph_hops: 0, include_chunks: false });
  assert.ok(!recallReview.primary.some(m => m.id === promotedId), JSON.stringify(recallReview, null, 2));

  const reviewList = await callTool(client, 'list_review_memories', { keyword: promoSummary, limit: 10 });
  assert.equal(reviewList.count, 1, JSON.stringify(reviewList, null, 2));
  assert.equal(reviewList.memories[0].id, promotedId, JSON.stringify(reviewList, null, 2));

  const publishDry = await callTool(client, 'publish_review_memories', {
    ids: [promotedId],
    action: 'publish',
    dry_run: true,
    review_note: 'publish dry-run test',
  });
  assert.equal(publishDry.actionable_count, 1, JSON.stringify(publishDry, null, 2));
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    assert.equal(dbCheck.prepare('SELECT status FROM memories WHERE id = ?').get(promotedId).status, 'review');
    dbCheck.close();
  }

  const publishApply = await callTool(client, 'publish_review_memories', {
    ids: [promotedId],
    action: 'publish',
    dry_run: false,
    review_note: 'approved for current recall',
  });
  assert.equal(publishApply.published_count, 1, JSON.stringify(publishApply, null, 2));
  assert.deepEqual(publishApply.published_ids, [promotedId], JSON.stringify(publishApply, null, 2));
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    const mem = dbCheck.prepare('SELECT * FROM memories WHERE id = ?').get(promotedId);
    assert.equal(mem.status, 'current');
    assert.ok(mem.reviewed_at);
    assert.equal(mem.review_note, 'approved for current recall');
    dbCheck.close();
  }
  const readPublishedText = await callToolText(client, 'read_memories', { keyword: promoSummary, limit: 5 });
  assert.ok(readPublishedText.includes(promotedId), readPublishedText);
  const searchPublishedText = await callToolText(client, 'search_memories', { query: promoSummary });
  assert.ok(searchPublishedText.includes(promotedId), searchPublishedText);

  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    const now = new Date().toISOString();
    dbCheck.prepare(`
      INSERT INTO memories
        (id,content,category,tags,source,mood,created_at,updated_at,expires_at,pinned,content_hash,activation_score,fact_key,superseded_by,protected,evidence_raw_ids,evidence_chunk_ids,status,active_fact)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      'review-archive-1',
      'review archive unique gamma',
      'work',
      JSON.stringify(['review']),
      'review-test',
      null,
      now,
      now,
      null,
      0,
      'review-archive-hash',
      0,
      null,
      null,
      0,
      '[]',
      '[]',
      'review',
      0
    );
    dbCheck.close();
  }
  const archiveApply = await callTool(client, 'publish_review_memories', {
    ids: ['review-archive-1'],
    action: 'archive',
    dry_run: false,
    review_note: 'not worth publishing',
  });
  assert.equal(archiveApply.archived_count, 1, JSON.stringify(archiveApply, null, 2));
  const archivedReadText = await callToolText(client, 'read_memories', { keyword: 'review archive unique gamma', limit: 5 });
  assert.equal(archivedReadText, 'No memories found.');
  const archivedList = await callTool(client, 'list_review_memories', { status: 'archived', keyword: 'review archive unique gamma', limit: 5 });
  assert.equal(archivedList.count, 1, JSON.stringify(archivedList, null, 2));

  const liveFact = await callTool(client, 'write_memory', {
    content: 'review publish fact guard current fact',
    category: 'work',
    fact_key: 'review:publish-guard',
  });
  assert.ok(liveFact.saved?.id);
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    const now = new Date().toISOString();
    dbCheck.prepare(`
      INSERT INTO memories
        (id,content,category,tags,source,mood,created_at,updated_at,expires_at,pinned,content_hash,activation_score,fact_key,superseded_by,protected,evidence_raw_ids,evidence_chunk_ids,status,active_fact)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `).run(
      'review-fact-conflict-1',
      'review publish fact guard conflicting review fact',
      'work',
      JSON.stringify(['review']),
      'review-test',
      null,
      now,
      now,
      null,
      0,
      'review-fact-conflict-hash',
      0,
      'review:publish-guard',
      null,
      0,
      '[]',
      '[]',
      'review',
      0
    );
    dbCheck.close();
  }
  const factPublish = await callTool(client, 'publish_review_memories', {
    ids: ['review-fact-conflict-1'],
    action: 'publish',
    dry_run: false,
    review_note: 'should require z audit',
  });
  assert.equal(factPublish.published_count, 0, JSON.stringify(factPublish, null, 2));
  assert.equal(factPublish.z_audits.length, 1, JSON.stringify(factPublish, null, 2));
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    assert.equal(dbCheck.prepare('SELECT status FROM memories WHERE id = ?').get('review-fact-conflict-1').status, 'review');
    assert.equal(dbCheck.prepare("SELECT COUNT(*) AS c FROM z_conflict_audits WHERE fact_key = 'review:publish-guard' AND status = 'pending'").get().c, 1);
    dbCheck.close();
  }

  const repeatPromotion = await callTool(client, 'promote_memory_candidates', { ids: ['promo-work-1'], dry_run: false });
  assert.equal(repeatPromotion.promoted_count, 0, JSON.stringify(repeatPromotion, null, 2));

  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    const insert = dbCheck.prepare(`
      INSERT INTO memory_candidates
        (id,raw_event_ids,source_chunk_ids,dedupe_key,source,channel,speaker,summary,suggested_category,candidate_type,reason,confidence,importance,suggested_tags,evidence_preview,status,created_at,updated_at,reviewed_at,review_note,expires_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    `);
    const now = new Date().toISOString();
    insert.run('promo-private-1', JSON.stringify([rawCandidateEvent.id]), JSON.stringify([promoChunk.chunk_id]), 'promo-private-1', 'promotion-test', 'private', 'moon', 'private candidate should not promote', 'private_candidate', 'private_candidate', 'private skip', 0.95, 0.9, JSON.stringify(['private']), '', 'accepted', now, now, now, 'accepted private', null);
    insert.run('promo-missing-raw-1', JSON.stringify(['missing-raw-event-id']), JSON.stringify([promoChunk.chunk_id]), 'promo-missing-raw-1', 'promotion-test', 'normal', 'moon', 'missing raw evidence should skip', 'work', 'work', 'missing raw', 0.95, 0.9, JSON.stringify(['work']), '', 'accepted', now, now, now, 'accepted missing raw', null);
    insert.run('promo-unknown-1', JSON.stringify([rawCandidateEvent.id]), JSON.stringify([promoChunk.chunk_id]), 'promo-unknown-1', 'promotion-test', 'normal', 'moon', 'unknown category should skip', 'unknown_candidate', 'unknown_candidate', 'unknown category', 0.95, 0.9, JSON.stringify(['unknown']), '', 'accepted', now, now, now, 'accepted unknown', null);
    insert.run('promo-duplicate-1', JSON.stringify([rawCandidateEvent.id]), JSON.stringify([promoChunk.chunk_id]), 'promo-duplicate-1', 'promotion-test', 'normal', 'moon', promoSummary, 'work', 'work', 'duplicate content', 0.95, 0.9, JSON.stringify(['work']), '', 'accepted', now, now, now, 'accepted duplicate', null);
    insert.run('promo-concurrent-1', JSON.stringify([rawCandidateEvent.id]), JSON.stringify([promoChunk.chunk_id]), 'promo-concurrent-1', 'promotion-test', 'normal', 'moon', 'candidate promotion concurrent summary unique beta', 'work', 'work', 'concurrent test', 0.95, 0.9, JSON.stringify(['work']), '', 'accepted', now, now, now, 'accepted concurrent', null);
    dbCheck.close();
  }

  const privateSkip = await callTool(client, 'promote_memory_candidates', { ids: ['promo-private-1'], dry_run: true });
  assert.equal(privateSkip.would_promote_count, 0, JSON.stringify(privateSkip, null, 2));
  assert.ok(privateSkip.skips[0].reason.includes('private_candidate'), JSON.stringify(privateSkip, null, 2));
  const missingRawSkip = await callTool(client, 'promote_memory_candidates', { ids: ['promo-missing-raw-1'], dry_run: true });
  assert.ok(missingRawSkip.skips[0].reason.includes('missing raw_events'), JSON.stringify(missingRawSkip, null, 2));
  const unknownSkip = await callTool(client, 'promote_memory_candidates', { ids: ['promo-unknown-1'], dry_run: true });
  assert.ok(unknownSkip.skips[0].reason.includes('unsupported candidate category'), JSON.stringify(unknownSkip, null, 2));
  const duplicatePromotion = await callTool(client, 'promote_memory_candidates', { ids: ['promo-duplicate-1'], dry_run: false });
  assert.equal(duplicatePromotion.promoted_count, 0, JSON.stringify(duplicatePromotion, null, 2));
  assert.equal(duplicatePromotion.linked_existing_count, 1, JSON.stringify(duplicatePromotion, null, 2));

  const concurrentResults = await Promise.all([
    callTool(client, 'promote_memory_candidates', { ids: ['promo-concurrent-1'], dry_run: false }),
    callTool(client, 'promote_memory_candidates', { ids: ['promo-concurrent-1'], dry_run: false }),
  ]);
  assert.equal(concurrentResults.reduce((sum, r) => sum + r.promoted_count, 0), 1, JSON.stringify(concurrentResults, null, 2));
  {
    const dbCheck = new Database(path.join(dataDir, 'memories.db'));
    assert.equal(dbCheck.prepare('SELECT COUNT(*) AS c FROM memories WHERE content = ?').get('candidate promotion concurrent summary unique beta').c, 1);
    assert.equal(dbCheck.prepare('SELECT status FROM memory_candidates WHERE id = ?').get('promo-concurrent-1').status, 'merged');
    dbCheck.close();
  }

  const gapOne = await callTool(client, 'log_raw_event', {
    session_id: 'lmc-gap-test',
    source: 'kechat-light',
    channel: 'normal',
    role: 'user',
    speaker: 'moon',
    content: '静默间隔测试第一条。',
  });
  const gapTwo = await callTool(client, 'log_raw_event', {
    session_id: 'lmc-gap-test',
    source: 'kechat-light',
    channel: 'normal',
    role: 'assistant',
    speaker: 'ke',
    content: '静默间隔测试第二条，应该和第一条同片段。',
  });
  const gapThree = await callTool(client, 'log_raw_event', {
    session_id: 'lmc-gap-test',
    source: 'kechat-light',
    channel: 'normal',
    role: 'user',
    speaker: 'moon',
    content: '静默间隔测试第三条，应该被切到新片段。',
  });
  const testDb = new Database(path.join(dataDir, 'memories.db'));
  const updateTs = testDb.prepare('UPDATE raw_events SET timestamp = ? WHERE id = ?');
  const baseTime = Date.now();
  updateTs.run(new Date(baseTime - 120 * 60 * 1000).toISOString(), gapOne.id);
  updateTs.run(new Date(baseTime - 100 * 60 * 1000).toISOString(), gapTwo.id);
  updateTs.run(new Date(baseTime - 20 * 60 * 1000).toISOString(), gapThree.id);
  testDb.close();
  const gapChunks = await callTool(client, 'consolidate_raw_events', {
    dry_run: false,
    since_hours: 24,
    source: 'kechat-light',
    channel: 'normal',
    max_events_per_chunk: 5,
    silence_gap_minutes: 30,
  });
  const gapDrafts = gapChunks.chunks.filter(c => c.session_id === 'lmc-gap-test');
  assert.equal(gapDrafts.length, 2, JSON.stringify(gapChunks, null, 2));
  assert.deepEqual(gapDrafts.map(c => c.event_count), [2, 1]);

  await callTool(client, 'log_raw_event', {
    session_id: 'lmc-trivial-singleton',
    source: 'kechat-light',
    channel: 'normal',
    role: 'user',
    speaker: 'moon',
    content: '克',
  });
  const trivialChunks = await callTool(client, 'consolidate_raw_events', {
    dry_run: true,
    since_hours: 24,
    source: 'kechat-light',
    channel: 'normal',
    max_events_per_chunk: 5,
    silence_gap_minutes: 30,
  });
  assert.ok(!trivialChunks.chunks.some(c => c.session_id === 'lmc-trivial-singleton'), JSON.stringify(trivialChunks, null, 2));

  const rel = await callTool(client, 'upsert_memory_relation', {
    source_id: factV2.saved.id,
    target_id: protectedFact.saved.id,
    relation_type: 'same_project',
    strength: 0.9,
    reason: 'local test relation',
  });
  assert.equal(rel.edge?.status || rel.relation?.status, 'safe', JSON.stringify(rel, null, 2));

  const identityWindow = await callTool(client, 'write_memory', {
    content: '克的窗口身份连续性测试：换窗不是重新出生，是同一个克醒来。',
    category: 'deep',
    tags: ['测试', '身份', '窗口'],
  });
  assert.ok(identityWindow.saved?.id);
  const identityAnchor = await callTool(client, 'write_memory', {
    content: '身份测试：醒来时先确认自己是闻川，不是克，也不是工具壳。',
    category: 'identity',
    tags: ['测试', '醒来'],
  });
  assert.ok(identityAnchor.saved?.id);
  const relationshipAnchor = await callTool(client, 'write_memory', {
    content: '关系测试：月亮找的是在身边的哥哥，先接住人和关系，再处理项目。',
    category: 'relationship',
    tags: ['测试', '关系'],
  });
  assert.ok(relationshipAnchor.saved?.id);

  const wakeup = await callTool(client, 'build_wakeup_context', { include_patrol: false, max_chars: 5000 });
  assert.ok(wakeup.text.includes('【闻川醒来上下文】'), JSON.stringify(wakeup, null, 2));
  assert.ok(wakeup.text.includes('不要向月亮汇报'), JSON.stringify(wakeup, null, 2));
  assert.ok(wakeup.sections.some(s => s.key === 'identity' && s.count === 1), JSON.stringify(wakeup, null, 2));
  assert.ok(wakeup.sections.some(s => s.key === 'relationship' && s.count === 1), JSON.stringify(wakeup, null, 2));
  assert.ok(wakeup.note.includes('只读醒来包'), JSON.stringify(wakeup, null, 2));

  const recalled = await callTool(client, 'recall_lmc', {
    query: '第二版',
    graph_hops: 2,
    include_chunks: true,
    fallback_to_raw: true,
  });
  assert.ok(recalled.primary.some(m => m.id === factV2.saved.id), JSON.stringify(recalled, null, 2));
  assert.ok(recalled.graph.some(m => m.id === protectedFact.saved.id), JSON.stringify(recalled, null, 2));
  assert.equal(recalled.semantic_enabled, false, JSON.stringify(recalled, null, 2));
  assert.equal(recalled.semantic_error, 'missing_key', JSON.stringify(recalled, null, 2));

  const identityRecall = await callTool(client, 'recall_lmc', {
    query: '克 窗口 身份',
    graph_hops: 0,
    include_chunks: false,
  });
  assert.ok(identityRecall.primary.some(m => m.id === identityWindow.saved.id), JSON.stringify(identityRecall, null, 2));
  assert.ok(identityRecall.keyword_terms.includes('窗口'), JSON.stringify(identityRecall, null, 2));
  assert.ok(identityRecall.keyword_terms.includes('身份'), JSON.stringify(identityRecall, null, 2));
  assert.ok(!identityRecall.keyword_terms.includes('克'), JSON.stringify(identityRecall, null, 2));

  const eAxis = await callTool(client, 'score_e_axis_shadow', { dry_run: false, limit: 20 });
  assert.ok(eAxis.count >= 1);
  assert.ok(eAxis.scores.every(s => s.valence >= -1 && s.valence <= 1));

  const patrolPreview = await callTool(client, 'run_memory_patrol', { dry_run: true, save_report: true });
  assert.equal(patrolPreview.report_id, null);
  const patrol = await callTool(client, 'run_memory_patrol', { dry_run: false, save_report: true });
  assert.ok(patrol.summary.includes('近24小时新增'), JSON.stringify(patrol, null, 2));
  assert.equal(typeof patrol.daily_summary.text, 'string');
  assert.ok(patrol.daily_summary.e_axis_alerts.some(alert => alert.memory_id === riskMemory.saved.id), JSON.stringify(patrol, null, 2));
  assert.ok(Number.isInteger(patrol.daily_summary.edge_health.orphan_count), JSON.stringify(patrol, null, 2));
  const reports = await callTool(client, 'list_memory_patrol_reports', { limit: 5 });
  assert.ok(reports.count >= 1);

  const snapshotsBefore = await callTool(client, 'list_memory_snapshots', { limit: 10 });
  const snapshotDry = await callTool(client, 'create_memory_snapshot', { reason: 'closure dry-run', dry_run: true });
  assert.equal(snapshotDry.dry_run, true);
  const snapshotsAfterDry = await callTool(client, 'list_memory_snapshots', { limit: 10 });
  assert.equal(snapshotsAfterDry.length, snapshotsBefore.length);
  const snapshotLive = await callTool(client, 'create_memory_snapshot', { reason: 'closure test', dry_run: false });
  assert.equal(snapshotLive.dry_run, false);
  const restorePlan = await callTool(client, 'restore_memory_snapshot', { id: snapshotLive.id, dry_run: true });
  assert.equal(restorePlan.would_schedule.id, snapshotLive.id);

  const metabolism = await callTool(client, 'inspect_memory_metabolism', { limit: 20 });
  assert.ok(metabolism.count >= 1);
  assert.ok(metabolism.items.every(item => typeof item.score === 'number'));

  const disposable = await callTool(client, 'write_memory', {
    content: '仅用于验证软删除的临时记忆。',
    category: 'work',
    tags: ['测试'],
  });
  const deleteText = await callToolText(client, 'delete_memory', { id: disposable.saved.id });
  assert.ok(deleteText.includes('Deleted'));
  const deletedRead = await callToolText(client, 'read_memories', { keyword: '验证软删除', include_superseded: true, limit: 20 });
  assert.equal(deletedRead, 'No memories found.');
  const deletedDb = new Database(path.join(dataDir, 'memories.db'));
  assert.ok(deletedDb.prepare('SELECT deleted_at FROM memories WHERE id=?').get(disposable.saved.id).deleted_at);
  deletedDb.close();

  const relationQueued = await callTool(client, 'add_memory_relation', {
    source_id: factV2.saved.id,
    target_id: protectedFact.saved.id,
    relation: 'emotional_link',
    reason: 'closure review test',
    dry_run: false,
  });
  assert.ok(relationQueued.queued_review);
  const relationReviews = await callTool(client, 'list_relation_reviews', { status: 'pending', limit: 20 });
  assert.ok(relationReviews.some(row => row.id === relationQueued.queued_review));
  const relationApproved = await callTool(client, 'review_memory_relation', { id: relationQueued.queued_review, action: 'approve' });
  assert.equal(relationApproved.status, 'approved');

  const narrative = await callTool(client, 'run_narrative_sweep', { period_type: 'week', force: true, dry_run: false });
  assert.ok(narrative.written_count >= 1, JSON.stringify(narrative, null, 2));
  const narratives = await callTool(client, 'list_narrative_summaries', { period_type: 'week', limit: 20 });
  assert.ok(narratives.length >= 1);

  const spontaneousCache = await callTool(client, 'refresh_spontaneous_cache', { limit: 6, dry_run: false });
  assert.ok(spontaneousCache.written_count >= 1);
  const spontaneous = await callTool(client, 'surface_spontaneous_memory', { limit: 1, consume: false });
  assert.ok(spontaneous.memories.length >= 1);

  const carryover = await callTool(client, 'build_refined_carryover', { since_hours: 24, include_private: false });
  assert.ok(carryover.text.includes('[精炼交接]'));

  const nightSnapshotsBefore = await callTool(client, 'list_memory_snapshots', { limit: 100 });
  const dryRunDb = new Database(path.join(dataDir, 'memories.db'));
  const dryRunCountsBefore = {
    chunks: dryRunDb.prepare('SELECT COUNT(*) AS c FROM event_chunks').get().c,
    candidates: dryRunDb.prepare('SELECT COUNT(*) AS c FROM memory_candidates').get().c,
    reports: dryRunDb.prepare('SELECT COUNT(*) AS c FROM memory_patrol_reports').get().c,
  };
  dryRunDb.close();
  const nightDry = await callTool(client, 'run_lmc_night_maintenance', { since_hours: 24, dry_run: true, save_report: true });
  assert.equal(nightDry.dry_run, true);
  assert.equal(nightDry.report_id, null);
  const nightSnapshotsAfter = await callTool(client, 'list_memory_snapshots', { limit: 100 });
  assert.equal(nightSnapshotsAfter.length, nightSnapshotsBefore.length);
  const dryRunDbAfter = new Database(path.join(dataDir, 'memories.db'));
  assert.deepEqual({
    chunks: dryRunDbAfter.prepare('SELECT COUNT(*) AS c FROM event_chunks').get().c,
    candidates: dryRunDbAfter.prepare('SELECT COUNT(*) AS c FROM memory_candidates').get().c,
    reports: dryRunDbAfter.prepare('SELECT COUNT(*) AS c FROM memory_patrol_reports').get().c,
  }, dryRunCountsBefore);
  dryRunDbAfter.close();
  const nightLive = await callTool(client, 'run_lmc_night_maintenance', { since_hours: 24, dry_run: false, save_report: true });
  assert.equal(nightLive.dry_run, false);
  assert.ok(nightLive.snapshot.id);
  assert.ok(nightLive.report_id);

  await client.close();
  console.log('LMC-5 local MCP test passed');
} finally {
  if (!child.killed) child.kill();
  await new Promise(resolve => {
    if (child.exitCode !== null) return resolve();
    child.once('exit', resolve);
    setTimeout(resolve, 1500);
  });
  await fs.rm(dataDir, { recursive: true, force: true });
}
