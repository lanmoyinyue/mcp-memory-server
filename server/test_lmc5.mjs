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
    'run_memory_patrol',
    'list_memory_patrol_reports',
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
    category: 'work',
    tags: ['测试'],
  });
  const realHeart = await callTool(client, 'write_memory', {
    content: '测试 E 轴：真的心动，项目成功，现在很紧急。',
    category: 'work',
    tags: ['测试'],
  });
  const riskMemory = await callTool(client, 'write_memory', {
    content: '测试 E 轴：这里涉及 token、鉴权、越权和红线，必须谨慎。',
    category: 'work',
    tags: ['测试'],
  });
  const fakeScore = await callTool(client, 'score_e_axis_shadow', { memory_id: fakeHeart.saved.id });
  const realScore = await callTool(client, 'score_e_axis_shadow', { memory_id: realHeart.saved.id });
  const riskScore = await callTool(client, 'score_e_axis_shadow', { memory_id: riskMemory.saved.id });
  assert.ok(fakeScore.scores[0].valence < 0, JSON.stringify(fakeScore, null, 2));
  assert.equal(fakeScore.scores[0].urgency, 0.2, JSON.stringify(fakeScore, null, 2));
  assert.ok(realScore.scores[0].valence > 0, JSON.stringify(realScore, null, 2));
  assert.equal(realScore.scores[0].urgency, 0.75, JSON.stringify(realScore, null, 2));
  assert.equal(realScore.scores[0].scorer_version, 'rules-v2');
  assert.ok(riskScore.scores[0].risk_level > 0.6, JSON.stringify(riskScore, null, 2));

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

  const patrol = await callTool(client, 'run_memory_patrol', { save_report: true });
  assert.ok(patrol.summary.includes('近24小时新增'), JSON.stringify(patrol, null, 2));
  assert.equal(typeof patrol.daily_summary.text, 'string');
  assert.ok(patrol.daily_summary.e_axis_alerts.some(alert => alert.memory_id === riskMemory.saved.id), JSON.stringify(patrol, null, 2));
  assert.ok(Number.isInteger(patrol.daily_summary.edge_health.orphan_count), JSON.stringify(patrol, null, 2));
  const reports = await callTool(client, 'list_memory_patrol_reports', { limit: 5 });
  assert.ok(reports.count >= 1);

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
