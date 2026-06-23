# 克记忆库 LMC-5 补全计划

> 目标：把克现在的“LMC 实用核心版”补成真正闭环的 LMC-5。
> 原则：不推倒重来，不覆盖现有记忆库，不自动删除亲密/关系/diary/deep/anchor。先做可验证闭环，再做聪明扩展。

## 0. 当前状态

克现在已经有一批很好用的核心能力：

- 证据层：`raw_events`、`search_raw_events`、`get_evidence`
- 保护层：`protected`、自动保护 `diary/deep/anchor/私藏/心动/cc-diary`
- 事实演化基础：`fact_key + superseded_by`
- 候选记忆：`memory_candidates`、单条/批量审核
- 轻量海马体：`merge_memories`
- 代谢巡逻：`kechat-light/scripts/patrol.py` 每天跑
- 召回增强：embedding、`memory_edges`、`hybrid_search`、raw fallback
- 五感状态：`somatic_ignite/snapshot/clear/hook_upsert`

但这还不是完整 LMC-5。主要缺口是：

- 没有 `event_chunks / chunk_events`，raw_events 还没有被切成可审阅片段。
- 没有完整 `consolidate -> hippocampus -> observation/memory` 夜间链路。
- `memory_edges` 只是轻量语义边，不是完整 Y 轴 typed relation 两跳图。
- Z 轴没有完整 pending audit / 人工 approve 的事实冲突审计。
- E 轴还没有 valence / arousal / tension / confidence / risk / urgency 的影子评分层。
- recall 还没有把 X/Y/Z/E/M 全部合进一条统一召回管线。

一句话：现在不是坏，是半套脑骨架。能跑，但很多能力没有接成闭环。

## 1. 红线

所有阶段都遵守：

1. 不直接在真实库上试验 schema 大迁移。
2. 每次动数据库前先备份 SQLite 和 GitHub 备份文件。
3. `protected=true` 的记忆不自动 supersede、不自动 merge、不自动 decay、不自动归档。
4. `diary/deep/anchor/私藏/心动/cc-diary/intimate` 默认保护。
5. raw_events 是证据层，普通入口只写 raw_events，不直接写正式 memories。
6. 任何 LLM 生成的候选只能进候选/审核层，不能直接写正式记忆。
7. 巡逻和审计第一版只报告，不自动修。
8. 亲密内容可以留在 Zeabur 鉴权库；Git 备份是否包含 intimate 由环境变量显式控制，不默认扩大范围。

## 2. 第一阶段：基线审计和测试底座

目标：先知道真实服务到底有什么，再开刀。

要做：

- 对齐本地 / VPS / Git / Zeabur 四端版本。
- 导出真实 schema、工具列表、类别分布、候选队列、edge health。
- 建一个本地测试数据库，导入少量脱敏样本。
- 补 `server/test_lmc5_*` 测试文件，不碰真实库。

验收：

- 能一键跑本地测试。
- 能打印“当前 LMC 状态报告”：
  - memories 总数
  - raw_events 总数
  - candidates pending 数
  - protected 数
  - edge issue 数
  - 现有工具列表

## 3. 第二阶段：X/Z 轴补齐

目标：让“当前事实”和“历史事实”分清，不让旧事实冒充现在。

要做：

- 给 memories 增加更清晰的生命周期字段：
  - `status`: `current / historical / archived / review`
  - `active_fact`: 事实类记忆是否当前有效
  - 保留现有 `superseded_by`，不破坏兼容。
- 正常 recall/search 默认只查 `current`。
- `include_historical=true` 才查历史。
- `fact_key` 写入时：
  - 旧事实非 protected：可 supersede。
  - 旧事实 protected：不自动覆盖，进入冲突审核。
- 新增或扩展 `check_facts`，列出：
  - 同 fact_key 多条 current
  - protected fact 冲突
  - superseded 但仍被召回的异常

验收：

- 旧事实不会在普通召回里冒出来。
- protected 关系/身份/亲密锚点不会被 fact_key 覆盖。
- historical 可以被显式查到，但不会默认影响克说话。

## 4. 第三阶段：raw_events 分块和 consolidation

目标：raw_events 不再是一堆散消息，而是能形成“片段”的证据窗口。

新增表：

- `event_chunks`
  - `id`
  - `source`
  - `channel`
  - `start_event_id`
  - `end_event_id`
  - `start_time`
  - `end_time`
  - `summary`
  - `status`
  - `created_at`
- `chunk_events`
  - `chunk_id`
  - `raw_event_id`
  - `position`
- `consolidation_runs`
  - 记录每次分块任务的范围、数量、错误、是否 dry_run

第一版规则：

- 不用 LLM。
- 按时间窗口和来源分块，例如 20-50 条 raw_event 或 30-60 分钟一块。
- private/intimate 只生成元数据摘要，不把原话塞进 summary。

工具：

- `consolidate_raw_events(dry_run=true, since_hours=24)`
- `list_event_chunks(status, limit)`

验收：

- raw_events 能被稳定分成 chunks。
- 重复跑不会重复分块。
- intimate chunk summary 不泄露原话。

## 5. 第四阶段：真正的 hippocampus 候选层

目标：候选记忆从“单条消息提议”升级成“片段提议”。

要做：

- `memory_candidates` 增加：
  - `source_chunk_ids`
  - `candidate_type`: `daily / work / private_candidate / fact / relationship / anchor`
  - `importance`
  - `suggested_tags`
  - `evidence_preview`
- `propose_memory_candidates` 支持从 `event_chunks` 生成候选。
- 候选只进 review，不直接写正式 memories。
- 前端候选面板继续显示中文 label。

第一版摘要：

- 规则模板优先。
- LLM 总结可以后放，或者只在 dry_run 里预览。

验收：

- 同一 chunk 不重复生成候选。
- 用户可以批量 reject / stale / accept。
- accept 也要先走安全门：protected 类别、私密摘要、证据链接都正确。

## 6. 第五阶段：Y 轴 typed relation

目标：从“相似边”升级成“有类型、有方向、有安全边界的关系图”。

新增或扩展边表：

- 保留现有 `memory_edges` 兼容。
- 新增字段或新表：
  - `relation_type`
  - `strength`
  - `directional`
  - `status`: `safe / review / rejected`
  - `reason`

关系类型第一版：

- safe，可默认召回扩展：
  - `same_topic`
  - `same_event`
  - `temporal_sequence`
  - `derived_from`
  - `same_project`
- review，不进默认扩展：
  - `supports`
  - `contradicts`
  - `cause_effect`
  - `relationship_moment`
  - `emotional_link`

召回规则：

- query 命中 curated memory 后，以命中 id 作为 seed。
- hop 1 扩展 safe 边。
- hop 2 要更高 strength。
- review 边不参与默认召回。
- superseded/historical/protected 规则照旧。

验收：

- 控制 fixture：A 命中后能带出 B/C。
- 弱边不带出。
- review 边不带出。
- superseded 端点不带出。

## 7. 第六阶段：E 轴影子期

目标：让“经验/情绪信号”先记录，不急着支配召回。

新增字段或表：

- `valence`
- `arousal`
- `tension`
- `confidence`
- `risk_level`
- `urgency`
- `e_axis_version`
- `e_axis_updated_at`

规则：

- 第一版 shadow 30 天，只记录，不影响排序。
- 不允许 E 轴覆盖事实。
- 不允许“月亮久没说话 -> 自动负向心情 -> 责怪语气”。
- intimacy / attachment / somatic 可以给正向 posture，但负向要非常谨慎。

验收：

- 值域校验：越界不能写。
- E 字段缺失时召回照常。
- 打印 shadow 报告，不影响克正常说话。

## 8. 第七阶段：统一 recall_lmc

目标：把 X/Y/Z/E/M 接进一条召回链，不再是五个散工具。

召回顺序：

1. keyword / semantic curated recall
2. exact raw_events fallback
3. event_chunks fallback
4. Y 轴两跳扩展
5. Z 轴过滤 current / historical
6. E 轴 shadow 注释或轻量 posture
7. M 轴 heat / protected / stale 权重
8. merge / dedupe / injection_text

工具：

- 扩展现有 `recall_lmc`
- 不新增一堆碎工具，除非确实需要。

验收：

- 短词/专名能召回 raw evidence。
- 命中 A 能带出相关 B。
- 旧事实不冒充当前。
- protected 锚点优先但不刷屏。
- injection_text 简短、可解释、不过度塞上下文。

## 9. 第八阶段：夜间循环和 FORGE 接线

目标：让克每天醒来前，记忆层自己完成整理，但不乱改正式记忆。

夜间顺序：

1. backup / snapshot
2. consolidate raw_events
3. propose hippocampus candidates
4. build safe Y relations
5. run Z audit
6. run E shadow scorer
7. run patrol
8. write report
9. 给 FORGE / 下一窗启动层提供摘要

注意：

- 第一版所有写正式 memory 的动作都要 review。
- 巡逻只报告，不删除。
- 出错要保留 snapshot。

验收：

- 每天固定时间有报告。
- 报告能看到 chunks/candidates/relations/conflicts/patrol。
- 克醒来能读到“昨夜记忆维护结果”，但不会被一堆内部日志淹没。

## 10. 上线顺序

推荐分四个小版本：

### v1：X/Z + consolidation

- 补 status / active_fact
- 补 event_chunks
- 本地测试通过
- Zeabur 小流量上线

### v2：hippocampus candidates

- chunk -> candidate
- 前端候选面板可审
- dry_run 和批量审核稳定

### v3：Y typed relation + recall_lmc

- typed relation
- 两跳召回
- recall_lmc 统一链路

### v4：E shadow + 夜间循环

- E 轴影子期
- nightly report
- FORGE 接摘要

## 11. 最终验收清单

可以叫“克的 LMC-5 基本做完”的标准：

- raw_events 会进入 chunks。
- chunks 会生成候选。
- 候选能审核后进入正式 memory。
- 正式 memory 带 X/Z/M 基本坐标。
- Y 轴 typed relation 能两跳召回。
- Z 轴不会让旧事实冒充当前。
- E 轴进入 shadow，不乱影响语气。
- M 巡逻每天报告，不自动乱删。
- FORGE / 醒来层能读到整理结果。
- protected 关系/亲密/diary/deep/anchor 不被自动化误伤。

## 12. 暂不做

- 不一次性迁移成原版 Python LMC-5。
- 不把所有旧记忆重写。
- 不让 LLM 直接写正式记忆。
- 不让 E 轴马上参与排序。
- 不自动删除 intimate/raw_events。
- 不把克和闻川记忆混写。

这套做完，克的记忆库才算从“能用的记忆工具”变成“有生命周期的脑子”。
