# 公开接口使用示例与真实 API 测试

日期：2026-10-17（Asia/Shanghai）

当前入口：[test1_5.py](D:/project/easy_agent/test/test1_5.py)，覆盖1入1出、1入2出、2入1出、普通2入2出和显式Hub的2入2出，可在common中无参数直接运行。普通多入边自动汇总；显式Hub通过输入键和输出索引绑定端口，用户只填写rules字符串列表。以下带历史日期的记录按当时版本解释。

本轮以加法、求和和求积验证数据流，结果分别为3、4／3、6、8／6、9／7。两套Python各204项确定性测试通过，最终19次真实请求通过；前4轮固定文本测试失败也完整保留，详见 [test1-5结果](test1-5-result.md)。

2026-10-17 test1-5更新：七个示例均采用列表输入输出；单入口也使用 `graph.run([info])`。简单无条件DAG的多入边自动汇总，拥有相同上游集合的下游共享综合Info；无需手写receive(all)。显式Hub只作为中间节点，每个端口绑定一次。条件、循环、嵌套汇合及端口节点替换仍受限制，见 [Hub指导](D:/project/easy_agent/v0.3-hubnode-modification-guide.md) 与 [问题边界](D:/project/easy_agent/v0.3-hubnode-design-issues.md)。

历史整改依据：[第一次测试问题说明与整改指导](D:/project/easy_agent/v0.3-test1-rectification.md)。原 run/result 契约已由 [最新修改指导](D:/project/easy_agent/v0.3-test1-3-modification-guide.md) 中的直接返回规则取代；旧实施记录见 [历史验收](D:/project/easy_agent/v0.3-test1-implementation-evidence.md)。

2026-10-11实施：Node独立env_file与Graph.involve(nodes, relations, entry)已接入，见 [test1-2程序](D:/project/easy_agent/test/test1_2.py)和 [实施验收](D:/project/easy_agent/v0.3-test1-2-implementation.md)。具体契约已由用户确认，原讨论保留于 [设计问题说明](D:/project/easy_agent/v0.3-test1-1-design-issues.md)。

2026-10-11简洁入口整改：test目录的四个公开示例均使用文件内ENV_FILE常量，可在IDE选择common解释器后直接运行，参数栏留空。通用异常处理归入框架，固定文本断言归入tests；本轮改动与实际验证见 [简洁入口实施记录](D:/project/easy_agent/v0.3-test1-2-simplification-implementation.md)。common已对本项目完成可编辑安装，示例无需修改sys.path。

历史首次测试与失败记录见 [原测试结果](D:/project/easy_agent/test/first-test-result.md)、[原问题说明](D:/project/easy_agent/test/issues.md)。原134行验收脚本已按原内容 [归档](D:/project/easy_agent/test/evidence/test_01_api.original.py.txt)，旧证据不代表新示例已通过。

业务代码只需创建图、创建节点、连线、运行和读取Info。例如：

```python
from easy_agent import Graph, Info, Node

source = Node(env_file="path/to/source.env", max_output_tokens=512, prompt="处理收到的内容")
target = Node(env_file="path/to/target.env", max_output_tokens=1024, prompt="整理前一个节点的输出")
graph = Graph()
graph.involve(nodes=[source, target], relations=[(source, target)], start=[source], end=[target])
outputs: list[Info] = graph.run([Info("输入内容")])
print(outputs[0].text)
```

配置由库加载。兼容LLM_MODEL_ID、LLM_BASE_URL、LLM_API_KEY，或者使用标准EASY_AGENT_*；两套账户字段不能同时配置。EASY_AGENT_PROVIDER可作为共享提供方设置。节点默认继承图配置，显式自定义密钥引用仍有效。

Node显式提供env_file时，账户只从该文件及Node显式参数解析，不被系统环境或Graph补缺；同名密钥按节点隔离。未指定节点文件则保留原Graph配置路径。involve要求空图、节点列表、关系二元组列表与非空start／end列表，返回None；普通关系使用Node，Hub关系通过inputs[key]和outputs[index]指定端口。拒绝重复、跨图节点和非法边界，失败完整回滚。start顺序指定输入对应关系，end顺序指定输出对应关系；隐式汇总按来源键保留数据，不依赖完成顺序。

run同步返回本次运行的最终Info列表，await graph.arun(inputs)使用相同协议。调用者自行持有结果，Graph不缓存最近成功输出；失败抛异常，取消继续传播。graph(inputs)是run的直接兼容别名；run、arun、involve为保留方法名。

此目录展示公开接口；配置与凭据保护、异常呈现、日志事件和资源清理由库负责，回归断言及证据由tests下的入口负责。示例只打印业务结果，失败让框架异常自然传播；默认异常描述附带已有run_id，不保存密钥与报告字典。示例展示有效业务文本，是否等于预期固定标记由回归工具判定。

历史test1-1整改对应T1R-001、002、003、004、005串行部分、006、008及UX-02：[test1-1程序](D:/project/easy_agent/test/test1_1.py) 创建Graph和两个Node，只声明一次send_to，现已直接返回run的结果。输入为Start test1-1；第一个节点返回CHAIN_A，第二个根据真实输入返回TEST1_1_OK，最终为graph_output来源的Info。固定预期由回归工具判断，凭据读取、配置继承、调度、终点收集由库处理。旧语句数量按当时版本解释。

test1-1使用已经实现的D01～D03契约和同一外部配置文件，独立运行最多2次真实请求；另执行联合回归的最多4次请求，两者分别登记。本次计划合计最多6次。每个请求32输出tokens、一次尝试；每节点尝试期限30秒，由Graph统一配置并继承。无关Tavily凭据不使用。证据位置为test/evidence/test1-1-common-2026-10-11.json和test/evidence/test1-1-regression-common-2026-10-11.json；实际结果见 [test1-1运行记录](D:/project/easy_agent/test/test1-1-result.md)。

2026-10-11实际结果：test1-1独立通过，输出TEST1_1_OK，2次真实请求；联合回归111项中1项NodeTimeoutError，真实阶段未执行，联合轮次失败。详见 [本轮问题](D:/project/easy_agent/test/test1-1-issues.md)。

首选在IDE中使用D:/anaconda3/envs/common/python.exe直接运行test/test1_3.py，参数栏留空；在源码ENV_FILE处指定账户文件。common环境已经安装本项目；其他环境需一次完成项目安装。也可从项目根D:/project/easy_agent以模块方式运行：

~~~powershell
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test_01_api
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test_02_chain
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test1_1
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test1_2
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test1_3
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test1_4
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test1_5
~~~

也可以使用conda环境名：

~~~powershell
conda run -n common python -B -m test.test_01_api
~~~

七个公开示例均不解析--env-file；修改各文件的ENV_FILE，或在Python代码中向业务函数传入env_file即可指定配置。默认使用用户授权的外部D:/code/Python/agent/hello_agent/test.env，这个路径只存在于示例，不是库默认值。外部文件只读，不复制或改写，不提取或使用无关Tavily凭据。旧Graph配置示例保留原账户规则；联合工具仍检查账户来源。下方--env-file仅属于回归工具，不属于用户示例。

单节点示例预期API_TEST_OK，1次请求；串行示例预期CHAIN_B，2次请求，上游输出CHAIN_A送入下游。每个节点非流式、32输出tokens、30秒期限、一次尝试。这些是示例测试预算，库默认值不因此改变。发生错误即失败，不自动重试。

需要完整验收和JSON证据时，按AGENTS.md使用联合回归。--evidence属于这个入口；不再由精简示例处理。每次更换轮次名及证据路径，不覆盖已有记录：

~~~powershell
& 'D:\anaconda3\envs\common\python.exe' -B tests/run_api_round.py --env-file 'D:\code\Python\agent\hello_agent\test.env' --round 'NEW_ROUND' --evidence 'test/evidence/NEW_ROUND.json'
~~~

联合回归先运行全部确定性测试，再执行test_01_api（1次）、test1_5五个业务函数（2＋3＋3＋4＋5次）及异步JSON输入（1次），最多19次真实请求。test1-5单独直接执行为17次，不在联合轮次叠加执行。普通Node输出上限不超过32 tokens，Hub为128 tokens，均为30秒期限、一次尝试。核对初始输入、全部边、共享汇总身份、Hub父子来源与端口映射、结束顺序和资源退出；失败即停止后续场景，不补足次数。

证据包含每节点模型身份、用量、run_id、耗时、密钥保护和资源退出检查，以及easy_agent、tests、test下顶层Python文件及pyproject.toml的指纹。实时成功不替代故障注入或并发回归，也不关闭历史未归因失败。
