
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

## SLSMetrics Web Dashboard 监控服务

SLSMetrics 是一个独立运行的 Web 监控服务，提供 ECharts 交互式仪表盘。它独立于 Fake NM 和 SLSRunner 进程，可单独启动。

### 架构

```
用户浏览器                           SLSMetrics 进程(28081)           MetricsServer(28080)
    │                                    │                              │
    │--- GET / (ECharts Dashboard) ----→│                              │
    │←--- index.html + dashboard.js ---│                              │
    │                                    │                              │
    │--- GET /api/metrics/current ------→│                              │
    │                                    │--- GET /metrics -----------→│
    │                                    │←--- JSON metrics ----------│
    │                                    │(存入 RingBuffer + SQLite)   │
    │←--- JSON current data -----------│                              │
    │                                    │                              │
    │--- GET /api/metrics/history ------→│                              │
    │                                    │(从 RingBuffer/SQLite 查询)  │
    │←--- JSON history data ----------│                              │
```

- **MetricsCollector**：周期性从 MetricsServer HTTP API（默认 28080 端口）拉取 JSON 指标数据。
- **MetricsStore**：内存 RingBuffer，暂存最近 N 条时序数据（默认 3600 条 ≈ 5 小时）。
- **MetricsDatabase**：SQLite 持久化，每小时清理 7 天前的过期数据。
- **MetricsApiHandler**：REST API 处理器 + 静态文件服务（ECharts 前端资源）。
- **前端 Dashboard**：基于 ECharts 5.5.0 的 6 个交互式图表（含可用节点、可用资源、队列 Pending 容器条形图等） + 8 个 KPI 卡片（含 Pending 容器、Reserved 容器、已提交应用等） + 节点健康状态条（Active/Lost/Unhealthy/Decommissioned）。

### 依赖关系

| 组件 | 前置条件 |
|------|---------|
| SLSMetrics（Web Dashboard） | **必须**：SLSNodeManager（MetricsServer 28080 端口）正在运行 |
| SLSNodeManager（Fake NM） | 必须连接到运行的 YARN RM |
| SLSRunner（压测） | 可选，可在 Fake NM 运行后随时启动 |

### 配置项

SLSMetrics 的配置项以 `yarn.metrics.*` 为前缀，写在 `fake.properites` 中：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `yarn.metrics.collect.interval` | 5000 | 指标采集间隔（毫秒） |
| `yarn.metrics.store.size` | 3600 | 内存环形缓冲区容量（条数） |
| `yarn.metrics.server.url` | http://localhost:28080 | MetricsServer HTTP 端点（不带 /metrics 后缀） |
| `yarn.metrics.nm.collect.enabled` | true | 是否启用从本地 NM MetricsServer 采集指标 |
| `yarn.metrics.nm.url` | http://localhost:28080 | NM MetricsServer 采集地址 |
| `yarn.metrics.web.port` | 28081 | Web Dashboard 监听端口 |
| `yarn.metrics.db.path` | target/metrics/metrics.db | SQLite 数据库路径 |
| `yarn.metrics.db.batch.size` | 10 | SQLite 批量写入条数 |
| `yarn.metrics.db.retention.days` | 7 | 数据保留天数 |
| `yarn.metrics.db.cleanup.interval` | 3600000 | 数据清理间隔（毫秒） |

### 运行方式

**先启动 SLSNodeManager：**
```bash
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.SLSNodeManager <config_dir>
```

**再启动 SLSMetrics Web Dashboard（新终端）：**

使用启动脚本（推荐）：
```bash
start-metrics.bat [config_dir]      # Windows
./start-metrics.sh [config_dir]     # Linux / Mac
```

直接运行：
```bash
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.metrics.SLSMetrics [config_dir]
```

`config_dir` 默认为 `src/main/resources`，需包含 `fake.properites` 及 XML 配置文件。

### 访问

在浏览器打开 `http://localhost:28081` 即可查看 ECharts 仪表盘。

### 输出

| 输出 | 说明 |
|------|------|
| **Web UI** | 浏览器访问 `http://localhost:28081`，自动刷新（10s 间隔） |
| **SQLite 数据库** | `target/metrics/metrics.db`，5 张表持续写入 |
| **API 端点** | `GET /api/metrics/current`、`/history`、`/nodes`、`/queue` |

### ⚠️ 已知限制

- 目前监控系统已通过 RM JMX 解决了集群资源利用率和队列状态的采集问题，KPI 卡片和图表已正常显示数据。


