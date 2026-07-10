
# 简介

Hadoop自带的hadoop-sls只能用于压测调度器，可在实际中影响ResourceManager性能的因素比较多，不能只看调度器。
当前项目可构造海量的Fake NM节点，用于模拟线上RM的巨大压力场景，进行优化。

[![zread](https://img.shields.io/badge/Ask_Zread-_.svg?style=flat&color=00b0aa&labelColor=000000&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyB3aWR0aD0iMTYiIGhlaWdodD0iMTYiIHZpZXdCb3g9IjAgMCAxNiAxNiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTQuOTYxNTYgMS42MDAxSDIuMjQxNTZDMS44ODgxIDEuNjAwMSAxLjYwMTU2IDEuODg2NjQgMS42MDE1NiAyLjI0MDFWNC45NjAxQzEuNjAxNTYgNS4zMTM1NiAxLjg4ODEgNS42MDAxIDIuMjQxNTYgNS42MDAxSDQuOTYxNTZDNS4zMTUwMiA1LjYwMDEgNS42MDE1NiA1LjMxMzU2IDUuNjAxNTYgNC45NjAxVjIuMjQwMUM1LjYwMTU2IDEuODg2NjQgNS4zMTUwMiAxLjYwMDEgNC45NjE1NiAxLjYwMDFaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00Ljk2MTU2IDEwLjM5OTlIMi4yNDE1NkMxLjg4ODEgMTAuMzk5OSAxLjYwMTU2IDEwLjY4NjQgMS42MDE1NiAxMS4wMzk5VjEzLjc1OTlDMS42MDE1NiAxNC4xMTM0IDEuODg4MSAxNC4zOTk5IDIuMjQxNTYgMTQuMzk5OUg0Ljk2MTU2QzUuMzE1MDIgMTQuMzk5OSA1LjYwMTU2IDE0LjExMzQgNS42MDE1NiAxMy43NTk5VjExLjAzOTlDNS42MDE1NiAxMC42ODY0IDUuMzE1MDIgMTAuMzk5OSA0Ljk2MTU2IDEwLjM5OTlaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik0xMy43NTg0IDEuNjAwMUgxMS4wMzg0QzEwLjY4NSAxLjYwMDEgMTAuMzk4NCAxLjg4NjY0IDEwLjM5ODQgMi4yNDAxVjQuOTYwMUMxMC4zOTg0IDUuMzEzNTYgMTAuNjg1IDUuNjAwMSAxMS4wMzg0IDUuNjAwMUgxMy43NTg0QzE0LjExMTkgNS42MDAxIDE0LjM5ODQgNS4zMTM1NiAxNC4zOTg0IDQuOTYwMVYyLjI0MDFDMTQuMzk4NCAxLjg4NjY0IDE0LjExMTkgMS42MDAxIDEzLjc1ODQgMS42MDAxWiIgZmlsbD0iI2ZmZiIvPgo8cGF0aCBkPSJNNCAxMkwxMiA0TDQgMTJaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00IDEyTDEyIDQiIHN0cm9rZT0iI2ZmZiIgc3Ryb2tlLXdpZHRoPSIxLjUiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIvPgo8L3N2Zz4K&logoColor=ffffff)](https://zread.ai/zeekling/yarnRmTester)
<a title="Hits" target="_blank" href="https://github.com/zeekling/hits"><img src="https://hits.b3log.org/zeekling/yarnRmTester.svg"></a>


# 架构

![pic](https://pan.zeekling.cn/zeekling/hadoop/fake/fake_01.png)

核心思想：
- Fake NM：构造大量的Fake NM。在Fake NM里面主要做Container的管理，不会真正的启动。防止占用大量资源。
- Fake AM: 构造的AM。只是一个对象，所有的AM由线程池管理，用于申请新的Container、控制整个作业的运行时长。
- SLSRunner: 压测模块，由于NM是Fake的，作业也是Fake的，只用于控制提交作业的数量。
- SLSMetrics：独立运行的监控服务，通过 MetricsServer HTTP API 和 YARN RM RPC 周期性采集指标，存储到 SQLite 并生成 PNG 趋势图。

# 运行

主要包含三个模块：Fake NM 构造大量模拟 NM 节点、SLSRunner 执行压测任务、SLSMetrics 提供独立监控服务。

## Fake NM 运行

当前模块主要是为RM构造大量的NM，建议在运行之前将集群内正常的NM停止掉。
当前模块的入口是SLSNodeManager，需要修改配置文件的路径，修改configPath为具体的路径即可。

配置文件主要包含：
- core-site.xml：从RM对应的集群里面获取
- hdfs-site.xml：从RM对应的集群里面获取
- yarn-site.xml：从RM对应的集群里面获取。但是下面参数需要按照模拟的实际情况修改：
  - yarn.scheduler.maximum-allocation-mb：模拟节点的内存。
  - yarn.scheduler.maximum-allocation-vcores：模拟节点的vcore。
- fake.properites：Fake NM的主要配置，含义如下：
  - yarn.fake.nodemanager.hostname：模拟节点的主机名，按照实际情况填写。
  - yarn.fake.nodemanager.rack： 模拟节点的Rack。
  - yarn.fake.nodemanger.rpc.port.begin：模拟NM节点的rpc端口范围的开始值。具体范围是起始值+模拟NM的id。
  - yarn.fake.nodemanger.http.port.begin： 模拟NM节点的http端口范围的开始值。具体范围是起始值+模拟NM的id。
  - yarn.fake.nodemanger.count： 模拟NM的数量。
  - yarn.fake.threadpool.size：处理心跳等的线程数目。
  - yarn.fake.job.token-servers： 获取hdfs token的详细信息。
  - yarn.fake.job.duration： Fake作业运行的时长，超过当前时长会将状态变为已完成。
  - yarn.fake.job.container.nums： 一个作业的普通Container数量。
  - yarn.fake.job.container.vcore：普通Container占用的vcore。
  - yarn.fake.job.container.memory-mb： 普通Container占用的内存大小。
  - yarn.fake.job.update.threadpool.size：作业状态更新的线程数目。

接下来就直接运行SLSNodeManager即可。例如：
```bash
java -cp .:lib/*:config/* org.apache.hadoop.sls.SLSNodeManager /home/hadoop01/fakeNM/config/
```

## SLSRunner

当前模块主要是运行压测任务。

当前模块的入口是SLSRunner，需要修改配置文件的路径，修改configPath为具体的路径即可。

配置文件主要包含：

- core-site.xml：从RM对应的集群里面获取
- hdfs-site.xml：从RM对应的集群里面获取
- yarn-site.xml：从RM对应的集群里面获取。
- fake.properites：压测作业相关的主要配置，含义如下：
   - yarn.fake.job.parallel： 提交作业的并行度。
   - yarn.fake.job.cycle.times： 循环次数。
   yarn.fake.job.queue： 作业提交的队列。

接下来就直接运行SLSRunner即可。例如：
```bash
java -cp .:lib/* org.apache.hadoop.sls.SLSRunner /home/hadoop01/fakeNM/config/
```

## SLSMetrics 监控服务

SLSMetrics 是一个独立运行的监控服务，用于对压测过程进行可视化监控和数据持久化。它不依赖 Fake NM 或 SLSRunner，可单独启动。

### 架构

```
MetricsCollector (定时采集)
    |
    ├─→ MetricsStore (内存环形缓冲区，容量由 yarn.metrics.store.size 控制)
    ├─→ MetricsDatabase (SQLite 持久化)
    |
    ChartGenerator (定时生成 PNG 趋势图，间隔由 yarn.metrics.chart.interval 控制)
```

- **MetricsCollector**：周期性从 MetricsServer HTTP API（默认 28080 端口）和 YARN RM RPC 采集指标数据。
- **MetricsStore**：内存中的环形缓冲区，暂存最近的指标数据，供 ChartGenerator 使用。
- **MetricsDatabase**：将指标数据写入 SQLite 数据库，支持按天数保留自动清理。
- **ChartGenerator**：基于 JFreeChart 生成四种 PNG 趋势图：
  - Container 趋势图
  - 资源利用率趋势图
  - 应用状态趋势图
  - 心跳延迟趋势图

### 前置条件

- YARN RM 必须正常运行。
- MetricsServer（内嵌在 SLSNodeManager 中，默认端口 28080）必须可访问。
- 如需从 RM RPC 采集指标，需配置 core-site.xml / yarn-site.xml。

### 配置项

SLSMetrics 的配置项以 `yarn.metrics.*` 为前缀，写在 `fake.properites` 中：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `yarn.metrics.collect.interval` | 5000 | 指标采集间隔（毫秒） |
| `yarn.metrics.chart.interval` | 30000 | 图表生成间隔（毫秒） |
| `yarn.metrics.store.size` | 3600 | 内存环形缓冲区容量 |
| `yarn.metrics.output.dir` | target/metrics | 图表输出目录 |
| `yarn.metrics.server.url` | http://localhost:28080/metrics | MetricsServer HTTP 端点 |
| `yarn.metrics.db.path` | target/metrics/metrics.db | SQLite 数据库路径 |
| `yarn.metrics.db.batch.size` | 10 | SQLite 批量写入条数 |
| `yarn.metrics.db.retention.days` | 7 | 数据保留天数 |
| `yarn.metrics.db.cleanup.interval` | 3600000 | 数据清理间隔（毫秒） |

### 运行方式

**使用启动脚本：**

Windows：
```bash
start-metrics.bat [config_dir]
```

Linux / Mac：
```bash
chmod +x start-metrics.sh
./start-metrics.sh [config_dir]
```

**直接运行：**
```bash
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.metrics.SLSMetrics [config_dir]
```

`config_dir` 为可选参数，默认为 `src/main/resources`，目录内需包含 `fake.properites` 及必要的 XML 配置文件。

### 输出

- **SQLite 数据库**：`target/metrics/metrics.db`，包含历史指标数据。
- **PNG 趋势图**：`target/metrics/` 目录下定时生成四种趋势图。
- 指标分为五类：集群资源、Container 调度、应用状态、心跳统计、队列统计。


