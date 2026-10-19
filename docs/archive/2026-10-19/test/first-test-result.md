> 历史归档，归档日期 2026-10-19。正文保留当时结论，源码行号可能已变化；当前行为与状态请读 [项目说明](../../../../README.md) 和 [问题清单](../../../issues.md)。本文件不再作为当前修改指令。

# 首个 API 测试结果

日期：2026-10-10（Asia/Shanghai）  
最终结果：通过。  
当时测试代码：[原脚本归档](../../../../test/evidence/test_01_api.original.py.txt)。

后续整改已精简当前test/test_01_api.py并增加两节点示例。本文的数值、指纹和命令为首次测试历史记录；新代码及运行方式见 [当前README](README.md)，新一轮结果单独登记。

## 最终实际运行

| 项目 | 结果 |
| --- | --- |
| Python 环境 | conda common |
| 解释器 | D:/anaconda3/envs/common/python.exe |
| Python 版本 | 3.10.20 |
| 请求模型 / 返回模型 | deepseek-flash / deepseek-flash |
| 输入 | First API connectivity test |
| 预期 / 实际返回 | API_TEST_OK / API_TEST_OK |
| 本次调用数 | 1，无自动重试 |
| 实际耗时 | 0.609 秒 |
| 结束原因 | stop |
| Token 用量 | 输入 23，输出 3，总计 26 |
| 密钥来源 | 指定外部 test.env |
| 日志及导出密钥检查 | 通过 |
| 运行资源退出 | 通过 |
| run_id | 289d746481584f9fa859114ed455c530 |

成功记录：[最终结果 JSON](../../../../test/evidence/first-api-common-final-2026-10-10.json)。

运行命令：

~~~powershell
D:\anaconda3\envs\common\python.exe -B test/test_01_api.py --evidence test/evidence/first-api-common-final-2026-10-10.json
~~~

## 联合回归

使用同一个 common 环境执行项目要求的 tests/run_api_round.py：

- 确定性回归：86 项通过。
- 真实同步请求：返回 OK，0.797 秒。
- 真实异步 JSON 请求：返回 7，0.547 秒。
- 两次真实请求均正常结束，密钥来源为 env_file，资源退出且日志/导出密钥检查通过。

证据：[联合回归 JSON](../../../../test/evidence/regression-common-2026-10-10.json)、[原始执行输出](../../../../test/evidence/regression-common-2026-10-10.txt)。

test 目录中的手动真实测试独立运行，不计入原有 86 项确定性用例。原有 tests 目录继续保留。

## 运行过程与失败记录

本轮手动脚本共运行三次，每次配置一次尝试，没有在脚本内部增加自动重试：

1. 初次模型请求记录 GraphRunSucceeded，但我写的检查代码使用 Graph.logs()，只取得图事件，遗漏模型与审计事件，导致检查失败。已改成完整事件查询；见 [初次失败](../../../../test/evidence/first-api-common-2026-10-10.json)。
2. 修改后的第一次运行出现 NodeExecutionError，未生成 ModelRequestFinished；该次具体原因尚未确认，保留为失败，未改算成功或跳过。见 [调用失败](../../../../test/evidence/first-api-common-fixed-2026-10-10.json)。
3. 同一修正脚本再次独立运行成功，返回 API_TEST_OK。最终成功文件与第二次失败文件包含相同脚本 SHA256；期间没有改变模型、密钥、超时或添加重试。

未知调用失败登记在 [问题说明](issues.md)。单次成功不用于证明提供商调用永不失败，也不关闭项目其余审计问题。
