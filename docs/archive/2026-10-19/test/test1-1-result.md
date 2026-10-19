> 历史归档，归档日期 2026-10-19。正文保留当时结论，源码行号可能已变化；当前行为与状态请读 [项目说明](../../../../README.md) 和 [问题清单](../../../issues.md)。本文件不再作为当前修改指令。

# test1-1 编写与运行记录

日期：2026-10-11（Asia/Shanghai）

依据：[第一次测试整改指导](../v0.3-test1-rectification.md)及用户已确认的D01～D03契约。本轮只新增公开接口示例并补充使用和结果文档，框架代码保持现状；D04汇合简化与D05安全诊断仍见 [未决问题](../v0.3-test1-implementation-issues.md)。

## 场景与验收

[程序](../../../../test/test1_1.py) 使用Graph、Node、Info，两个节点通过send_to串行连接。输入Start test1-1，上游固定输出CHAIN_A，下游检查收到的内容后输出TEST1_1_OK。普通终点自动收集，graph.result()读取本次运行的最终Info。业务函数6条可执行语句，占7行；示例不解析凭据、不读取私有属性、不把报告字典作为图输出。

从D:/project/easy_agent根目录使用conda common环境运行：

```powershell
& 'D:\anaconda3\envs\common\python.exe' -B -m test.test1_1
```

外部配置为D:/code/Python/agent/hello_agent/test.env，只读，仅使用模型配置。Graph统一配置每节点尝试期限30秒，每次请求最多32输出tokens、一次尝试，不自动重试。独立示例最多2次真实请求，联合回归最多4次，计划合计最多6次。固定输出不匹配或请求失败均记失败。

独立示例以模块入口执行；测试进程捕获Graph实例与安全标准输出，使用tests/api_support.py集中检查最终Info、result身份、上下游事件关联、模型、凭据来源和资源退出。用户示例只使用公开接口。联合回归继续通过tests/run_api_round.py执行全部确定性测试和既有4次真实请求，不更改其场景与预算。

## 实际结果

独立test1-1通过；联合回归失败。两项分别保留，不将示例成功写成整轮通过。本轮实际只进行了2次真实请求，没有补跑或重试。

| 项目 | 实际结果 |
| --- | --- |
| 环境 | conda common，Python 3.10.20，D:/anaconda3/envs/common/python.exe |
| 独立示例 | 模块入口退出码0；标准输出TEST1_1_OK |
| 最终结果 | Info，origin_kind=graph_output，graph.result()返回同一对象 |
| 串行链路 | 上游输出内容哈希与CHAIN_A匹配；下游输入Info ID对应上游产出，检查通过 |
| 耗时 | 1.562秒，含配置、模块入口、两次调用与验收 |
| 真实请求 | 2次，每节点一次，无重试 |
| 请求／报告模型 | 两节点均为deepseek-flash／deepseek-flash，finish_reason=stop |
| 用量 | first：输入21、输出3、合计24 tokens；second：输入39、输出5、合计44 tokens；总计68 tokens |
| 凭据与资源 | 来源env_file，与指定文件匹配；日志和导出无模型密钥；运行及清理已退出 |
| run_id | 1e0051fe97c14673a44d82f81ce1d78a |
| 独立记录时间 | 2026-10-11 10:16:32（Asia/Shanghai） |
| 联合确定性回归 | 111项，3.846秒；110项通过、1项错误，退出码1 |
| 错误用例 | test_step1.Step1RuntimeTests.test_downstream_wait_does_not_extend_upstream_attempt，NodeTimeoutError |
| 联合真实API阶段 | 因确定性回归失败未执行，0次请求；该联合轮次记failed |

证据：[独立示例JSON](../../../../test/evidence/test1-1-common-2026-10-11.json)、[联合失败记录JSON](../../../../test/evidence/test1-1-regression-common-2026-10-11.json)、[联合原始执行输出](../../../../test/evidence/test1-1-regression-common-2026-10-11.txt)。联合入口在确定性失败时不会生成live JSON，因此联合JSON由测试进程根据保留输出补记，含原始输出指纹，不冒充实际调用的API报告。

新问题与建议见 [test1-1问题说明](test1-1-issues.md)。没有放宽原测试期限、删除断言或擅自修改运行逻辑。33行示例中，建图至返回Info的业务函数为6条可执行语句，占7行；其余是导入、命令行和安全结果检查。

历史未归因的真实失败仍保留，不因本轮结果关闭；不将JSON输入通过解释为结构化输出通过。
