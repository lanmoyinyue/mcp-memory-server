# A-TMA 后续方案：显式事实演变与影子统计（暂缓）

> 2026-08-10 审核结论：当前非 swap 的有效多版本事实过少，且 supersede 后关联边尚未迁移。本方案保留，等关联边迁移完成且真实演变链自然积累后再启用。

## 目标

在第一阶段 `current / historical / auto-shadow` 的基础上，增加显式 `transition` 视图，让调用方能查询“某件事以前怎样、后来怎样、现在是什么”。本阶段只处理有 `fact_key + superseded_by` 的确定性事实演变，不把普通日记、关系记忆或语义相似关系误当成状态变化。

## 一、`state_view=transition`

### 输入

`recall_lmc` 的 `state_view` 扩展为：

- `current`：默认值，行为保持不变。
- `historical`：第一阶段已完成。
- `transition`：本阶段新增，显式查看事实演变。
- `auto`：继续只做 shadow，不改变实际召回。

### 检索流程

1. 对 query 分别在 current 与 historical 事实中做关键词/语义检索。
2. 只保留有合法 `fact_key` 的命中，并排除大小写任意的 `swap-*`。
3. 按 `fact_key` 合并去重，通过 `superseded_by` 直接组装版本链。
4. 每条链明确返回：
   - `fact_key`
   - `versions`
   - `links`（predecessor -> successor）
   - `current_memory_id`
   - `matched_version_ids`
   - `chain_complete`
5. 不使用 memory graph 连接版本，不让相似边冒充事实继任关系。

## 二、返回与注入

新增结构化字段 `state_transitions`，每个事实链只返回有限版本，默认每链最多 8 条。

`injection_text` 可以加入极短的事实数据行，例如：

```text
- 事实演变 project:x：旧版摘要 -> 当前摘要
```

限制：

- 只陈述数据，不加入“应该如何回答”、人格、语气、安全或关系行为提示。
- `injection_text` 总上限仍为 5000 字符。
- 为 transition 单独预留最多 1600 字符；超出时截断 transition，不挤掉 current 主召回。
- 原始证据、完整私密内容不因 transition 被额外展开。

## 三、`auto` 继续 shadow，但开始可观察

本阶段不启用自动切换。只记录低成本、无付费调用的统计：

- 请求的 `state_view`
- shadow 建议的视图
- 命中的触发词
- 实际 effective view
- transition 可用链数量

优先复用 `recall_traces`，只加结构化字段；不新建一套重复日志，不调用 LLM，不调用额外 embedding，不增加后台定时任务。

统计目标：观察“以前、当时、后来、变化、历史”等词在真实查询中的假阳性。样本不足前，`auto` 永远保持 `effective=current`。

## 四、巡逻与人工处置说明

保持巡逻只读，补充两项：

- `transition_chain_incomplete_count`：链指向存在但无法抵达 current 的事实数。
- `historical_without_fact_key` 的人工处置说明：先核对证据，再选择补 fact_key、恢复 current 或归档；不得自动修复。

不增加自动删除、自动补 key、自动改 status 的功能。

## 五、明确不做

- 不启用 auto 自动切换。
- 不把普通日记按时间强行拼成演变链。
- 不使用 graphExpand 构造事实版本。
- 不接付费 API，不增加 embedding 调用次数。
- 不修改人格、语气、亲密、安全或回复方式提示。
- 不改正式记忆内容，不批量迁移旧数据。

## 六、测试与验收

1. 默认不传 `state_view` 时，返回结果与第一阶段 current 完全一致。
2. transition 查询一条三版本事实，版本顺序和 `superseded_by` 链正确。
3. query 命中旧版或当前版，都能定位同一条事实链。
4. 多条 fact_key 同时命中时不串链。
5. `swap-*`、无 fact_key、断链事实不进入正常 transition 结果。
6. 断链进入只读巡逻报告，不被自动修复。
7. transition 注入不超过 1600 字符，总注入不超过 5000 字符。
8. auto 的 effective view 始终为 current，只产生 shadow 统计。
9. 本地临时库端到端测试、VPS 临时库测试、Zeabur 只读验收全部通过。
10. 验收 diff 中不得出现人格、语气、安全或回复方式提示。

## 七、上线顺序

1. 本地实现与测试。
2. 克只读代码审核。
3. 修复审核问题后再提交 Git。
4. VPS 快进同步并运行临时库测试。
5. Zeabur 部署。
6. 只读验证 transition 结构与巡逻字段，不用付费语义探针。
