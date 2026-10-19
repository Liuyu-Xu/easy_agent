# easy-agent

以 `Info` 传递数据的轻量工作流库。用户实例化节点、声明连接，再通过 `Graph.run(inputs)` 得到按结束节点顺序排列的结果。当前版本为 0.3.0。

## 开始使用

Python 3.10 及以上，在项目根目录安装：

```powershell
python -m pip install -e .
```

模型账户由各节点的 `env_file` 指定；文件须提供 `LLM_MODEL_ID`、`LLM_BASE_URL`、`LLM_API_KEY`。不要将实际账户文件提交到仓库。

```python
from easy_agent import Graph, Info, Node

account = "D:/code/Python/agent/hello_agent/test.env"
draft = Node(env_file=account, prompt="根据输入给出一个简短方案。")
review = Node(env_file=account, prompt="检查输入方案，给出精简后的最终方案。")

graph = Graph()
graph.involve(
    nodes=[draft, review],
    relations=[(draft, review)],
    start=[draft],
    end=[review],
)
outputs = graph.run([Info("为两个人安排周末读书活动。")])
print(outputs[0].text)
```

上述路径是本项目用户指定的外部账户文件，不是库默认配置。不同节点可指定不同文件及参数。`run` 返回本次运行的 `list[Info]`；异步调用为 `await graph.arun(inputs)`，没有公开 `result()`。

不调用模型的节点可以直接覆盖 `forward`：

```python
class Double(Node):
    def forward(self, info):
        return Info(info.data * 2)

node = Double()
graph = Graph()
graph.involve(nodes=[node], relations=[], start=[node], end=[node])
assert graph.run([Info(3)])[0].data == 6
```

## 当前能力

| 能力 | 范围 |
| --- | --- |
| 普通工作流 | 固定无条件 DAG；按 start 分发输入，按 end 收集输出 |
| 多输入、多输出 | 普通节点自动汇总全部直接上游，再向下游广播相同内容；高级整理使用显式 HubNode |
| HubNode | 固定输入字段、输出端口及规则列表，提示词与输出解析由库生成 |
| 模型调用 | 非流式 DeepSeek Chat Completions；普通文本输出、固定 Hub 输出 |
| 内部工具 | 有类型标注的函数、Tool；仅允许无副作用工具，显式授权 |
| 执行与重试 | 同步/异步共用内核；同步业务移至跟踪线程；瞬态失败按 max_attempts 重试 |
| 尚未提供 | 可强制终止的 Worker、持久恢复、流式、时序会话、副作用审批、任意嵌套汇合 |

线程超时表示放弃本次结果，线程可能仍运行；定义租约保留到实际退出。`input_token_budget` 当前接收但不实施，不能视为输入成本上限。更多约束见当前设计。

## 文档与测试入口

- [当前设计与接口契约](docs/design.md)：节点配置、图边界、Hub、工具和执行规则。
- [当前问题清单](docs/issues.md)：唯一维护的问题状态，包含未决公开行为。
- [API 测试规则](v0.3-api-minimal.md)：环境、实际请求上限、联合入口和凭据边界。
- [变更及验收记录](docs/changes.md)：本轮修改、回归结果和证据位置。
- [示例运行](test/README.md)：在 IDE 中无参数运行 test1 系列程序。
- [历史文档归档](docs/archive/README.md)：旧设计、问题和验收记录，保留追溯，不作为当前实施指令。

维护当前文档时更新上述入口，不再为同一问题重复生成独立指导文件。历史结论与当前实现冲突时，以当前设计、问题状态及最新验收记录为准；新增公开行为须先定稿。
