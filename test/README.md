# 示例运行

使用 conda common 的 `D:/anaconda3/envs/common/python.exe`，先在项目根目录执行 `python -m pip install -e .`。

在 IDE 中直接运行 `test1_5.py`，参数栏留空。各程序的 ENV_FILE 指向用户授权的外部账户文件；修改这个常量即可切换文件，不需要命令行参数，也不在示例里编写凭据、日志或清理逻辑。

| 程序 | 用途 |
| --- | --- |
| test_01_api.py | 单节点 |
| test_02_chain.py、test1_1.py～test1_3.py | 链式、装配和接口演进示例 |
| test1_4.py | 一入一出、多入一出、一入多出 |
| test1_5.py | 四种普通图形状与显式 Hub |

直接运行 test1_5 最多 17 次模型请求；正式联合回归已经包含这些场景，不要再叠加运行。正常结果由示例打印，回归断言和证据由 tests 中的入口负责。

接口规则见 [项目说明](../README.md) 和 [当前设计](../docs/design.md)。每轮修改须执行 [API 测试规则](../v0.3-api-minimal.md) 中的联合入口；旧测试结果与问题已归入 [历史文档](../docs/archive/README.md)。
