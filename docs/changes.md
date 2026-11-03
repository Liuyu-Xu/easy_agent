# 变更与验收记录

## 2026-11-03：参数收敛与运行诊断整改

按“公开接口少、内部自动规划、关键参数可调试”的原则完成第一批整改：

- 纯 Python `Node.process()` 在没有显式 Graph/Node 账户作用域时不读取模型账户，不因无关的环境变量冲突而失败；显式 `env_file`、Graph 模型配置或 API key 引用仍保持原有解析和审计。
- `Graph.logs(category=...)` 增加统一类别查询，支持 `graph`、`node`、`model`、`audit` 和 `None`；Node.logs 继续自动查询 node 类别，未增加第二套日志接口。
- `max_steps` 计数移动到节点尝试的已记录阶段，超限运行写入 `NodeFailed`，并带 `reason=max_steps`；不再出现任务无事件失败。
- 没有新增 ToolNode、公开 ChannelSpec、InfoParser 或第二个节点执行入口；Info 解析继续由普通 Node.process 完成。

本批修改新增 2 项回归覆盖纯 Python 账户隔离和 max_steps 失败事件，并增加模型日志类别查询断言。

本批验证证据：[common 确定性与真实联合回归](../tests/evidence/next-improvement-2026-11-03/api-round.txt)、[Python 3.13 确定性回归](../tests/evidence/next-improvement-2026-11-03/python313-tests.txt)。两套 Python 各 249 项确定性测试通过，真实 API 21/21 次请求通过。

## 2026-11-03：节点执行接口统一为 process

按当前接口收敛方案，将 Node、Tool、编译计划、运行时调度、测试和活动文档中的节点执行入口从 `forward(info)` 统一改为 `process(info)`。

- `Node.process(info)` 是唯一节点业务入口；没有保留 `forward` 别名。
- `Tool.process(info)` 继续支持旧式 `invoke(info)` 兼容桥，但不再提供 `forward` 执行入口。
- `_NodePlan`、Hub 内置节点检测、模型调用上下文和同步线程调度全部使用 `process`。
- 定义旧 `forward` 子类时立即抛出 `TypeError`，避免旧实现被静默当成模型节点。
- 没有新增 `ToolNode`、公开 `ChannelSpec` 或 Info 解析专用节点；Info 解析继续由普通 Node 的 `process` 完成。
- HubNode 继续继承 Node 配置，可在构造时指定 `env_file`、model、provider、base_url、api_key_env、timeout 和输出预算；prompt、role 以及输入输出结构仍由 Hub 契约管理。

本轮验证：common / Python 3.10.20 确定性测试 247 项通过，真实 API 21/21 次通过；Python 3.13.9 确定性测试 247 项通过。真实 API 仅在本地测试进程读取用户授权的外部密钥文件，未修改该文件。

证据：[common 真实联合回归](../tests/evidence/process-rename-2026-11-03/api-round.txt)、[common JSON 结果](../tests/evidence/process-rename-2026-11-03/api-round.json)、[Python 3.13.9 回归](../tests/evidence/process-rename-2026-11-03/python313-tests.txt)、[改名回归日志](../tests/evidence/process-rename-precheck.txt)、[接口定向检查](../tests/evidence/process-rename-targeted.txt)。

## 2026-10-19：审查整改与文档收敛

依据 [当前修改审查快照](archive/2026-10-19/v0.3-current-review-2026-10-19.md)，处理 OI-048～055 的 8 组代码问题及 OI-056 的文档问题。公开使用方式继续采用当前 Graph/Node/Hub/Info 契约，OI-057 的工具步数分歧保留在问题清单。

### 实现

- 调度器等待任务完成时同时考虑最近重试到期，不占业务并发名额；移除只适用于空闲调度器的旧等待函数。
- 工具续轮响应使用当轮剩余预算检查；拒绝超额及无法继续完成的工具轮。工具/文本消息必须与正常结束原因匹配，事件记录真实通过校验的结束原因。
- Hub 和工具复用严格 JSON 对象解码；工具参数契约在编译期冻结，模型声明与本地执行不再各自解析签名和类型。
- number 接受整数/浮点数并排除 bool；Optional 在模型声明和本地验证中一致允许 null；位置专用和可变参数在编译时拒绝，不隐式转换值。
- 内部 Tool 冻结输入/输出 Schema 与执行方法，复用现有验证；输出须为 Info，不兼容输入 Schema 在 HTTP 前拒绝。
- 顶层与内部阻塞工作统一按 execution/attempt 登记，使用同一锁保护；超时事件覆盖内部工具线程，清理租约持续到实际退出。
- 补齐运行状态 Any 导入；错误文案移除对旧仓库 MD 路径的依赖；包描述改用 README。

### 测试变更

- 新增 `tests/test_review_fixes.py`：调度唤醒/取消、预算超额/耗尽、异常响应、严格 JSON、冻结类型契约、内部 Tool Schema、线程租约、类型标注。
- 旧 Optional 声明断言同步为包含 null 的类型；异常子进程测试明确采用 UTF-8，避免父进程使用 UTF-8 而 Windows 子进程使用本地编码时的解码失败。
- 联合真实回归新增一个最多两次 HTTP 调用的纯工具场景，总预算 21；完整性断言包含一次 add(2,3)、工具回填和最终文本。
- 模型证据按 execution/attempt/round 配对，避免将最终一轮用量和结束状态套到全部轮次。工具测试另记录 HTTP 适配器实际调用次数，失败和超限均不重试。

### 文档归档

43 份历史 Markdown 移至 `docs/archive/2026-10-19/`，保留正文，新增历史提示并重定位链接；原位置、归档位置及 SHA256 见 [清单](archive/2026-10-19/manifest.json)。未删除旧设计决策、失败记录或原测试证据。

当前维护五份主体：根 README、docs/design、docs/issues、本文和根 API 测试规则；AGENTS 约束与 test/README 的短导航继续保留。历史报告不再被当作当前实施指导，也不再为同一问题建立平行总表。

### 验证进度与证据

最终验收已完成：

| 检查 | 实际结果 | 证据 |
| --- | --- | --- |
| common / Python 3.10.20 | 247 项通过，10.637 秒 | [最终联合日志](../tests/evidence/rectification-2026-10-19/api-round-final.txt) |
| Python 3.13.9 | 247 项通过，8.664 秒 | [最终跨版本日志](../tests/evidence/rectification-2026-10-19/python313-tests-final.txt) |
| 最终真实 API | 8 场景、21/21 次请求通过，所有场景凭据保护与资源退出检查通过 | [最终 JSON](../tests/evidence/rectification-2026-10-19/api-round-final.json) |
| 工具真实场景 | 两次 HTTP 调用，一次 add(2,3)，结果 5；tool_calls/stop 分轮记录 | 同上 |
| 文档与源码核对 | 43 份归档，7 份当前维护/导航文档；本地链接、归档文件哈希及最终源码指纹核对 | [归档检查](../tests/evidence/rectification-2026-10-19/documentation-check.json) |

确定性测试由原 228 项增至 247 项。整改过程中共执行两次有界真实联合回归，各 21 次且均通过，合计 42 次；首轮后补强非有限浮点参数断言，按最终测试版本重新执行收尾。没有叠加独立工具真实探针，也没有后台调用。

- [整改前受控测试](../tests/evidence/rectification-2026-10-19/before-fixes.txt)：15 项测试，18 个失败断言、5 个错误，重现当前审查问题。
- [首轮定向回归](../tests/evidence/rectification-2026-10-19/after-fixes-targeted.txt)：15 项通过。
- [首次完整验证](../tests/evidence/rectification-2026-10-19/after-fixes-suite.txt)：243 项，1 项子进程解码错误；该次失败保留，未执行真实 API。
- [第二次完整验证](../tests/evidence/rectification-2026-10-19/after-fixes-suite-2.txt)：244 项通过，随后补充工具调用限额、模型 Schema 和非有限浮点参数断言，最终结果见上表。
- [首轮联合结果](../tests/evidence/rectification-2026-10-19/api-round.json)：246 项确定性测试及 21 次真实请求通过；保留作为中间记录，最终版本以上表为准。

历史重构、223/228 项验收、19 次正常 API 和旧工具探针等结果见归档，均保留原日期与范围，不能与本轮结果混加。
