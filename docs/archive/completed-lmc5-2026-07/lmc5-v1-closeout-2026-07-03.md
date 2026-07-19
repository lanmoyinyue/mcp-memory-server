# LMC-5 v1 收口记录

时间：2026-07-03 04:32

## 结论

克侧 LMC-5 v1 进入稳定观察期。当前不继续开大刀，不新增大模块，不动正式记忆本体。

这一轮已经完成并验证：

- Z 轴事实演化：`fact_key`、`superseded_by`、事实冲突审核层。
- 原始证据层：`raw_events`、时间过滤、删除入口、证据追溯。
- 分块层：`event_chunks` / `chunk_events`。
- 候选层：候选生成、去重、批量审核、候选晋升到 review。
- 发布层：review 记忆列表、publish / archive。
- 图谱层：关系边健康检查、坏边预演和清理。
- E 轴：shadow 评分，只观察，不影响召回。
- 巡逻层：健康报告、每日 LMC maintenance loop。
- 前端面板：候选、review、边清理、patrol 报告都能看和操作。
- 自动浮现收口：近 6 轮不重复引用同一条自动浮现记忆；只影响回复引用，不修改记忆。

## 今日收口动作

1. 清理 VPS 旧会话：
   - 发现 `cc` tmux session 中有一个运行 5 天多的旧 Claude 进程。
   - 已关闭 `cc` session，并终止 orphan Claude PID `1047702`。
   - 当前 `ke` session 和 `ke-server` 保持运行。

2. 系统提示补规则：
   - `/root/ke-system-prompt.txt` 增加“自动浮现记忆去重”规则。
   - 备份：`/root/ke-system-prompt.txt.bak-20260703-memory-dedupe`。
   - 已同步告知当前活着的克窗口。

## 观察期清单

接下来观察 3-5 天，只看数据，不主动改结构：

- 候选是否堆积：pending 是否持续增长。
- review 是否长期不发布：review 队列是否卡住。
- patrol 报告是否异常：坏边、事实冲突、高风险 E 轴。
- 自动浮现是否还重复：尤其不要反复浮现同一条旧技术 bug 记忆。
- 召回是否像醒着：关系锚点、身份连续性、近期工作是否能自然接上。
- 体感与 desire/pulse 是否协调：本地五感优先，MCP somatic 空快照不视为故障。

## 暂时不要动

- 不动 `protected`、`diary`、`anchor`、`deep`、`私藏`、`心动`。
- 不改体感衰减曲线。
- 不改醒来链路：`corridor -> dream -> anchor -> diary -> deep -> cc-diary`。
- 不把 E 轴接进召回权重。
- 不自动发布 review 记忆。
- 不自动清理正式记忆。

## 下一步

若观察期正常，LMC v1 就算正式收口。后续只做小修：

1. 低风险降噪：自动浮现去重、报告展示优化。
2. 运维减重：备份、日志、uploads、未跟踪文件分类归档。
3. 等真实数据证明需要时，再考虑 LMC v2。
