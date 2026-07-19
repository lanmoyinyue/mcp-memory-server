# 克记忆库 LMC-5 v1 收口验收记录

日期：2026-07-02

结论：克侧 LMC-5 v1 已完成并进入常态运行。后续只做质量优化，不再把这一轮算作“未完成的大手术”。

## 已完成闭环

1. raw_events 证据层已上线，外部入口先写原始证据，不直接写正式记忆。
2. event_chunks / chunk_events 已上线，raw_events 可以按会话、时间间隔和数量切成片段。
3. memory_candidates 已上线，支持 pending / accepted / rejected / merged / stale。
4. 候选面板和批量审核已上线，月亮可以在小家前端查看和批量处理。
5. promote_memory_candidates 已上线，accepted 非私密候选可以晋升为 status=review 的正式记忆。
6. list_review_memories / publish_review_memories 已上线，review 记忆默认不进召回，发布后才进入 current。
7. evidence_raw_ids / evidence_chunk_ids 已上线，review/current 记忆能追溯到 raw 和 chunk 证据。
8. Y 轴 typed relation 已上线，safe 边可用于 graph recall，review 风险边不进默认召回。
9. Z 轴事实演化和 z_conflict_audits 已上线，protected 事实不会被自动覆盖。
10. E 轴 shadow 已上线，只评分和报警，不影响事实和默认排序。
11. run_memory_patrol / list_memory_patrol_reports 已上线，健康报告可在前端查看。
12. kechat-light 每日 8:47 patrol 已接入 LMC 维护链：consolidate raw -> propose chunk candidates -> E shadow -> patrol report。

## 2026-07-02 线上验收

- 本地 `mcp-memory-server-live`：`node --check server.js` 通过。
- 本地 `mcp-memory-server-live`：`npm run test:lmc5` 通过。
- 本地 `kechat-light`：`python -m py_compile scripts/patrol.py` 通过。
- VPS `kechat-light`：`python3 -m py_compile scripts/patrol.py` 通过。
- VPS 小流量真实维护链已跑通：95 条 raw_events -> 14 个 chunks -> 10 条 chunk candidates -> 30 条 E 轴 shadow 分数。
- 线上 recall_lmc 验证：primary 5 条、graph 5 条，semantic_enabled=true，semantic_error=null。
- 线上候选收口：pending 28 条已批量 rejected，raw_events/chunks 证据保留；当前 pending=0。
- 线上关系边健康：orphan=0，bad_edge=0。
- 线上事实冲突：pending z audits=0，current fact_key conflicts=0。

## 当前已知状态

- accepted 候选中大量是 private_candidate。v1 明确不自动晋升 private_candidate，只保留证据或由人工另行处理。
- E 轴仍有少量 high-risk 报警，当前都是 shadow-only，不影响召回和语气。后续可做 E 轴规则降噪，但不阻塞 v1 验收。
- VPS `/root/kechat-light` 工作区有很多历史脏改动，因此本次没有 `git pull` 覆盖线上，只备份并单文件同步了 `scripts/patrol.py`。

## 红线仍然有效

- protected / diary / deep / anchor / 私藏 / 心动 / cc-diary 不参与自动 supersede、merge、decay、清理。
- private/intimate 原话不进入正式记忆正文。
- E 轴不覆盖事实。
- patrol 默认报告和候选，不自动删除正式记忆。
- 任何下一轮大改仍需本地、VPS、Git、Zeabur 四端查验。

## 后续只算二期优化

- E 轴降噪和 risk_level 分辨力优化。
- graphExpand 性能批量查询。
- semantic embedding 内存缓存或向量库。
- 更细的 chunk 摘要质量优化。
- private_candidate 是否建立 private_index，需要月亮和克另行确认。

