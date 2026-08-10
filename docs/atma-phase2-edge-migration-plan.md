# A-TMA 第二阶段方案：事实替换时迁移关联边

## 背景

事实演化会把旧记忆标为 `historical`，并用 `superseded_by` 指向新版本；但旧记忆上的关系边仍停在旧 ID。结果不是召回错误，而是原本有效的联想会随着事实替换静默丢失。

当前真实人工演化链很少，因此这不是紧急修复。它仍值得现在做：范围小、可回滚，并且补上这条写入链路后，后续才能放心扩大 `fact_key` 的使用。

本阶段只修关系边连续性，不增加人格、提示词、自动状态路由或付费调用。

## 一、自动迁移的准确触发条件

新事实写入后，只有旧事实的 supersede SQL 返回 `changes === 1`，才允许把该旧 ID 交给迁移 helper。

- `protected = 1` 等原因导致更新 0 行时，不得把旧 ID 当作已替换事实。
- 未成功 supersede 的旧记忆不得迁边。
- 这类跳过单独记为 `supersede_not_applied`，进入巡逻报告。
- helper 不信任调用方提供的“计划替换列表”，只处理数据库确认已完成替换的旧 ID。

## 二、第一期允许迁移的关系

第一期只迁移以下四类无方向语义关系：

- `semantic`
- `same_topic`
- `same_event`
- `same_project`

以下关系虽然属于现有安全类型，但不得迁移：

- `derived_from`：派生关系属于当时的具体版本，迁到新版本会篡改来源。
- `temporal_sequence`：时序属于当时的节点，迁到新版本会改写历史先后。

这两类边保留在旧节点，并分别以 `derived_from_not_migrated`、`temporal_sequence_not_migrated` 计入不可迁移统计。

## 三、迁移规则

对每个确认已 supersede 的旧事实：

1. 找出所有以旧记忆为 source 或 target 的关系边。
2. 只处理第二节列出的四种关系，且边的 `status` 必须安全。
3. 另一端必须存在、未删除、未过期且为 `current`。
4. `old -> X` 迁为 `new -> X`；`X -> old` 迁为 `X -> new`。
5. 替换后若形成 self-loop，则跳过并保留旧边。
6. 若新边不存在，写入新边；若新边已存在，执行迁移专用去重。
7. 新边成功写入或确认等价边已存在后，才删除对应旧边。
8. 失败、跳过或无法证明等价的旧边全部保留。

另一端本身已经 historical 时，本期明确跳过，不追着 `superseded_by` 猜 successor；单独计为 `other_endpoint_historical`。

## 四、迁移专用去重

不得复用现有 `insertMemoryRelationPlan` 的覆盖式 upsert。迁移 helper 使用独立 SQL：

- 以 `strength` 为比较依据，保留已有边与迁入边中的较高值。
- `weight` 始终同步写成最终 `strength`，两列不得分叉。
- 冲突时保留已有边的安全 `relation_type`，不被迁入边覆盖。
- 已有边不是本期允许迁移的四种类型时，不把它视作等价去重成功，旧边不删。
- `status` 不得从更严格状态被迁入数据放宽。

## 五、事务边界

事实 INSERT、旧事实 supersede 和本次自动边迁移必须原子完成。

- embedding 等网络调用在事务开始前完成。
- 迁移 helper 使用 `SAVEPOINT / RELEASE / ROLLBACK TO`，允许被写入路径或存量工具安全嵌套调用；每次调用的 savepoint 名由旧记忆 ID 与单调计数器组成，不复用固定名称。
- 任一步失败，回滚新记忆、旧事实状态和本次边变更。
- MCP 与 REST 两条写入路径共用同一个 helper，不复制迁移逻辑。

## 六、存量修复工具

新增 `migrate_superseded_edges`：

- 默认 `dry_run=true`。
- 只操作 `memory_edges`，不改记忆正文、status、fact_key、protected 或 superseded_by。
- successor 只能来自旧记忆已有且指向 current 记忆的 `superseded_by`，不得猜测。
- 只迁移第二节列出的四种关系。
- 返回 `planned / migrated / deduped / skipped / failed` 明细和原因。
- dry-run 返回本次计划边集合的稳定 hash，作为 `plan_fingerprint`。
- `dry_run=false` 必须携带刚审核过的 `plan_fingerprint`；数据变化导致 hash 不一致时拒绝执行。
- apply 只删除本次确认已成功迁移或等价去重的旧边。
- 一次 apply 对指纹对应的全部计划边使用一个外层事务；中途任一步失败则整批回滚，`migrated / deduped` 计数只在提交成功后返回。

## 七、巡逻统计

新增只读统计：

- `superseded_edge_migration_candidate_count`
- `superseded_edge_unmigratable_count`
- `supersede_not_applied_count`
- 各类跳过原因与少量样本

这些属于关系完整性指标，不并入第一阶段的 `temporalIssueCount`，也不自动触发修复。

## 八、明确不做

- 不改 `graphExpand` 排序、跳数或评分。
- 不迁移 `derived_from`、`temporal_sequence`、待审核、拒绝或风险关系边。
- 不追踪另一端 historical 记忆的 successor。
- 不处理无 `fact_key` 的 historical 记忆。
- 不修改正式记忆内容。
- 不增加人格、语气、安全、亲密或回复方式提示。
- 不调用付费 API，不增加 embedding 次数，不增加后台定时任务。
- 不启用 transition 或 auto active。

## 九、测试与验收

1. `A -> B` 与 `B -> A` 在 B 被 B' 替换后分别正确迁移。
2. supersede SQL 为 0 changes 时不迁边，旧边完整保留。
3. 已有新边时保留更高 strength，weight 与 strength 一致，安全 relation_type 不被覆盖。
4. `derived_from` 与 `temporal_sequence` 不迁移且旧边保留。
5. self-loop、另一端非 live、historical、风险边均跳过且旧边不删。
6. 故意制造失败，验证新记忆、旧状态和边全部回滚。
7. helper 在外层事务中调用时不出现嵌套事务错误。
8. MCP 与 REST 行为一致。
9. dry-run 零写入，且返回稳定 `plan_fingerprint`。
10. 指纹不一致的 apply 必须拒绝；一致时只改预演过的边集合。
11. current、historical 与原有图谱召回无回归。
12. 本地与 VPS 临时库测试通过；Zeabur 上线后先只读巡逻，不直接 apply 存量。

## 十、上线顺序

1. 本地实现 helper、SAVEPOINT 事务、迁移专用 upsert、dry-run 指纹和测试。
2. 本地端到端测试。
3. 克只读审核代码。
4. 修复审核问题后提交 Git。
5. VPS 快进同步并用临时库测试。
6. Zeabur 部署，只读巡逻与 dry-run。
7. 克审核 dry-run 明细与计划指纹，月亮确认执行后，才允许一次存量 apply。
