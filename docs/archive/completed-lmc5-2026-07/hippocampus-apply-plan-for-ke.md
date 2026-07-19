# 克记忆库 LMC-5 候选晋升层方案

日期：2026-07-02

状态：待克审核。本文只定方案，不改代码，不动真实记忆。

## 先说结论

克现在已经有 raw_events、event_chunks、memory_candidates、批量审核、Y 关系边、Z 审计、E 轴 shadow、patrol 健康报告。

但现在的候选审核只做到：

```text
memory_candidates.pending -> accepted / rejected / stale
```

还没有做到原版 LMC-5 的下一环：

```text
accepted candidate -> review memory -> safe relation edges -> 后续人工/规则发布为 current
```

所以这一步要补的是“海马体 apply 层”：把已经接受的候选，安全地整理成正式记忆库里的 review 记忆，而不是直接变成 current 事实。

## 目标

新增一个保守的候选晋升流程：

```text
accepted memory_candidates
  -> dry-run 预览
  -> 过滤私密/低置信/重复/危险候选
  -> 写入 memories(status='review')
  -> 写 evidence_raw_ids / evidence_chunk_ids
  -> 写安全关系边
  -> 标记候选已晋升
  -> patrol 可检查
```

这一步完成后，候选不再只是“被批过的档案袋”，而是能进入正式记忆生命周期。

## 红线

1. 默认 `dry_run=true`，不显式关闭就不写库。
2. 不把候选直接写成 `current`。
3. 不自动写入亲密原话。
4. `private_candidate` 默认不晋升为正式记忆，只保留 accepted 证据状态。
5. 即使允许私密索引晋升，也只能写元数据摘要，不写原文。
6. 不碰 protected 旧记忆，不 merge protected，不 supersede protected。
7. 不自动处理 `contradicts / supports / cause_effect`，这些只进 review 队列。
8. apply 前要有备份；真实 VPS 上第一轮小批量跑，不全量一口气推。

## 新增或扩展字段

### memory_candidates

建议增加：

```sql
ALTER TABLE memory_candidates ADD COLUMN promoted_memory_id TEXT;
ALTER TABLE memory_candidates ADD COLUMN promoted_at TEXT;
ALTER TABLE memory_candidates ADD COLUMN promotion_note TEXT NOT NULL DEFAULT '';
ALTER TABLE memory_candidates ADD COLUMN relation_hints TEXT NOT NULL DEFAULT '[]';
```

用途：

- `promoted_memory_id`：防止同一个候选重复晋升。
- `promoted_at`：记录何时晋升。
- `promotion_note`：记录跳过或晋升原因。
- `relation_hints`：以后接原版 NightDream 时能直接放 safe/review 关系建议。

### memories

建议增加：

```sql
ALTER TABLE memories ADD COLUMN evidence_chunk_ids TEXT NOT NULL DEFAULT '[]';
```

现在已经有 `evidence_raw_ids`，但原版强调 chunk 是证据窗口。加 `evidence_chunk_ids` 后，review memory 能追溯到片段层，不只追溯到零散 raw。

如果暂时不想迁移 memories，也可以第一版只把 chunk id 留在 candidate 里。但这会让正式记忆的证据链不完整，不推荐。

## 新 MCP 工具

建议工具名：

```text
promote_memory_candidates
```

参数：

```json
{
  "ids": [],
  "match_status": "accepted",
  "suggested_category": "",
  "source": "",
  "channel": "all",
  "candidate_type": "",
  "limit": 50,
  "dry_run": true
}
```

说明：

- `ids` 有值时，只处理指定候选。
- `ids` 为空时，按 filter 查 accepted 候选。
- 第一版不暴露 `include_private`，私密候选一律跳过。
- 第一版固定写入 `status='review'`。
- 第一版固定不写关系边，等晋升链稳定后再开 `safe_only`。
- `limit` 默认 50，真实服务第一次建议 5 或 10。

## 晋升规则

### 可晋升候选

第一版只允许这些类型自动晋升到 review：

```text
work
daily
fact
relationship
anchor
observation
```

其中：

- `work`：技术、部署、排错、工具规则。
- `daily`：日常相处和普通经历。
- `fact`：明确事实，但需要走 Z 轴检查。
- `relationship`：关系理解，但不能覆盖 protected 锚点。
- `anchor`：重要锚点候选，只能 review，不直接 current。
- `observation`：从 chunk 总结出来的观察。

### 默认跳过候选

默认跳过：

```text
private_candidate
intimate
低 confidence
缺 raw_event_ids 且缺 source_chunk_ids
已经 promoted_memory_id 非空
summary 为空
```

跳过不是失败，要写进 dry-run 返回：

```json
{
  "candidate_id": "...",
  "action": "skip",
  "reason": "private_candidate requires include_private=true"
}
```

### 私密候选处理

第一版建议：

```text
private_candidate accepted = 原始证据确认留档
不等于自动写正式记忆
```

第一版不做 `include_private=true`。后续如果月亮明确要开，也只允许生成“私密索引 review memory”，格式类似：

```text
YYYY-MM-DD HH:mm，月亮在 telegram/private 有一段已确认留档的私密互动。
原文只保存在 raw_events 证据层，不在正式记忆正文展开。
```

分类建议：

```text
private_index
```

并且：

- `content` 不含原话。
- `evidence_raw_ids` 指向 raw_events。
- `evidence_chunk_ids` 指向 event_chunks。
- `protected=true`。
- `status='review'`。

这个口子建议让克确认后再开。

## 写入 memories 的格式

非私密候选晋升时：

```text
content = candidate.summary
category = candidate.suggested_category
tags = candidate.suggested_tags + ["candidate-promoted"]
source = "candidate:" + candidate.source + ":" + candidate.channel
status = "review"
active_fact = 0
protected = resolveProtectedFlag(category)
evidence_raw_ids = candidate.raw_event_ids
evidence_chunk_ids = candidate.source_chunk_ids
```

如果 `suggested_category` 是受保护类别，例如 diary、deep、anchor、私藏、心动、cc-diary，则 `protected=true` 仍然生效。

## Z 轴检查

如果候选有 `fact_key` 或被分类为 `fact`：

1. 查同 fact_key 的 current 记忆。
2. 如果没有冲突，可以写 review memory。
3. 如果冲突对象 protected，不能 supersede，只创建 z_conflict_audits。
4. 第一版不自动把旧事实改 historical。

也就是说，这一步只把事实候选放进 review，不做最终事实替换。

## Y 关系边

候选晋升后，可以写安全关系边，但只能非常保守。

第一版 safe 关系来源：

1. `relation_hints` 里明确给出的 safe 类型。
2. 同一个 chunk 派生出的多条 review memory：`same_event`。
3. 新 review memory 和相同 category/tag 的近邻：`same_topic`，强度低一点，例如 0.5。
4. 由候选生成的记忆和它的来源 chunk 不能直接进 `memory_edges`，因为 chunk 不是 memory；证据关系用 `evidence_chunk_ids` 保存。

允许自动写的 safe 类型：

```text
same_topic
same_event
temporal_sequence
derived_from
same_project
same_tool
emotional_link
in_thread
same_person
in_episode
instance_of
```

必须进入 review、不能默认扩展的类型：

```text
contradicts
supports
cause_effect
```

如果候选 relation_hints 里出现 review 类型，只记录到返回结果或 z_conflict_audits，不写成 safe 边。

## 候选状态更新

成功晋升后，不建议继续只保持 `accepted`，否则下次会重复扫到。

建议：

```text
candidate.status = "merged"
candidate.promoted_memory_id = 新 memory id
candidate.promoted_at = now
candidate.promotion_note = "promoted to review memory"
```

这里的 `merged` 含义不是“合并了正式记忆”，而是“这个候选已经被吸收到正式记忆生命周期”。如果觉得名字容易误会，可以后续新增 `promoted` 状态，但那会牵动前端和工具枚举。第一版用已有 `merged` 更稳。

## 返回结果

dry-run 返回：

```json
{
  "dry_run": true,
  "matched_count": 12,
  "would_promote_count": 5,
  "would_skip_count": 7,
  "plans": [
    {
      "candidate_id": "...",
      "action": "promote",
      "memory_preview": {
        "category": "work",
        "status": "review",
        "protected": false,
        "content": "..."
      },
      "relations": []
    }
  ],
  "skips": [
    {
      "candidate_id": "...",
      "action": "skip",
      "reason": "private_candidate skipped by default"
    }
  ]
}
```

apply 返回：

```json
{
  "dry_run": false,
  "promoted_count": 5,
  "skipped_count": 7,
  "created_memory_ids": [],
  "updated_candidate_ids": [],
  "relations_written": 0,
  "review_relations_queued": 0
}
```

## 测试清单

必须补测试，不能只手测。

1. `dry_run=true` 时，memories 数量不变，candidate 不变。
2. accepted work candidate 能写成 `status='review'` 的 memory。
3. 写入后 candidate 变成 `merged`，并记录 `promoted_memory_id`。
4. 重复跑 apply 不会重复创建 memory。
5. private_candidate 默认跳过。
6. private_candidate 即使 accepted，第一版也不会晋升正式记忆。
7. protected 类别晋升后仍然 protected。
8. fact candidate 不自动 supersede current fact。
9. protected fact 冲突只进 z_conflict_audits。
10. 第一版 `relations_written=0`，不会偷偷写 Y 关系边。
11. 后续打开 safe relation 前，再单独补 review relation 不进入默认 graph recall 的测试。
12. `recall_lmc` 默认不召回 `status='review'` 的新记忆。
13. backup/restore 覆盖新字段。
14. 两个请求同时 promote 同一个 candidate，也只能创建一条 review memory。
15. `evidence_raw_ids` 指向不存在的 raw_event 时跳过，不生成断证据链记忆。
16. daily 候选晋升为 review memory 时不设置 `expires_at`。
17. `read_memories/search_memories/hybrid_search/recall_lmc` 默认都排除 review memory。
18. 未知 `suggested_category` 跳过，不猜分类。

本地命令：

```bash
node --check server.js
npm.cmd run test:lmc5
```

如果测试文件继续放在 `server/test_lmc5.mjs`，这次要新增一组 candidate promotion 专项测试。

## 部署顺序

1. 本地写代码。
2. 本地测试库跑全绿。
3. 本地真实库只 dry-run，不 apply。
4. 给月亮和克看 dry-run 输出。
5. 克确认后 push Git。
6. VPS 拉取。
7. VPS 备份。
8. VPS 先 dry-run。
9. VPS 小批量 apply，例如 limit=5。
10. 跑 patrol。
11. 没问题再扩大批量。

## 不做

这一期不做：

- 不做 LLM 自动总结。
- 不做候选自动 current。
- 不做大规模 relation rebuild。
- 不做 review memory 自动发布 current。
- 不做私密原文正式记忆化。
- 不做 destructive cleanup。

这些都留到候选晋升层稳定以后。

## 克审核后的定稿口径

克已审核，第一版按更保守的范围实现：

1. `private_candidate` 保持“只 accepted 留证据”，第一版不暴露 `include_private`。
2. 成功晋升后，candidate 状态用现有 `merged`，不新增 `promoted`。
3. 第一版不写 Y 关系边，等晋升链跑稳后再开 `safe_only`。
4. review memory 继续被默认 recall/search 排除，等人工发布为 current 后再进入正常行为。
5. `evidence_chunk_ids` 加到 memories 表，补完整证据链。
6. Z 轴事实候选只写 review，不自动 supersede。
7. 测试补并发防重复、缺失 raw evidence、review 不过期、搜索排除 review、未知 category 跳过。

## 我的建议

第一轮最稳的做法：

```text
promote_memory_candidates(
  match_status="accepted",
  limit=10,
  dry_run=true
)
```

先看会晋升哪些，再让克挑几条非私密 work/daily 候选小批量 apply。

等这条链路稳定后，再打开 `relation_mode="safe_only"`，让 Y 轴开始自然长边。
