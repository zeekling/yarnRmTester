# SLSMetrics 监控服务设计文档（v2 — Web Dashboard 版）

## 概述

实现 `org.apache.hadoop.sls.metrics.SLSMetrics` 独立进程，采集 YARN 集群的调度核心指标，持久化到 SQLite 数据库，并通过 ECharts Web Dashboard 提供交互式监控界面。

**与 v1 的区别**：抛弃 PNG 静态图表，改为 ECharts 交互式 Web 仪表盘，保留 SQLite 持久化和内存 RingBuffer。

## 数据来源

| 来源 | 方式 | 说明 |
|------|------|------|
| MetricsServer `/metrics` | HTTP GET | 节点心跳指标、容器分配/释放数据 |
| YarnClient → RM | RPC | 集群资源、应用状态、队列指标 |

## 核心指标（5 大类）

### 1. 集群资源指标
- `totalNodes` / `totalMemory` / `totalVCores`
- `allocatedMemory` / `allocatedVCores`
- `availableMemory` / `availableVCores`
- `clusterUtilization`（派生）

### 2. 容器调度指标
- `totalContainersAllocated` / `totalContainersReleased`
- `activeContainers`（派生）
- `pendingContainers` / `reservedContainers`
- `containerAllocateRate` / `containerReleaseRate`（派生）

### 3. 应用调度指标
- `activeApplications` / `completedApplications`
- `failedApplications` / `submittedApplications`

### 4. 节点心跳指标
- `successfulHeartbeats` / `failedHeartbeats`
- `heartbeatSuccessRate`（派生）
- `avgHeartbeatLatency` / `maxHeartbeatLatency`
- `heartbeatThroughput`（派生）

### 5. 调度队列指标
- `queueName` / `usedCapacity` / `absoluteCapacity`
- `pendingApps` / `activeApps`

## 类结构

```
org.apache.hadoop.sls.metrics
├── SLSMetrics.java              ← main() 入口（HTTP Server + 采集启动）
├── MetricsCollector.java        ← 定时采集线程（轮询 MetricsServer）
├── MetricsSnapshot.java         ← 单次采集快照 POJO
├── MetricsStore.java            ← 内存环形缓冲区（近期时序数据）
├── MetricsDatabase.java         ← SQLite 持久化层（历史数据存储）
└── MetricsApiHandler.java       ← REST API 处理器 + 静态文件服务
```

前端（内嵌在 classpath 中）：
```
src/main/resources/frontend/
├── index.html                   ← ECharts 仪表盘主页面（已有）
├── css/style.css                ← 深色主题样式（已有）
└── js/dashboard.js              ← ECharts 图表逻辑（需新建）
```

## 数据库（SQLite）

### 表结构
- `cluster_resource_snapshots` — 集群资源快照
- `container_scheduling_snapshots` — 容器调度快照
- `application_snapshots` — 应用调度快照
- `heartbeat_snapshots` — 节点心跳快照
- `queue_snapshots` — 队列调度快照

每 5s 一条记录，索引按 `timestamp` 优化。

### 数据保留
- 配置项 `yarn.metrics.db.retention.days`（默认 7 天）
- 定时清理任务每小时执行一次，删除过期数据后回收空间

## REST API 设计

| 端点 | 方法 | 说明 |
|------|------|------|
| `GET /api/metrics/current` | JSON | 最新采集快照（KPI 卡片数据） |
| `GET /api/metrics/history?range=1h` | JSON | 时序历史数据（4 个图表的原始数据） |
| `GET /api/metrics/nodes` | JSON | 所有节点的心跳和容器详情 |
| `GET /api/metrics/queue` | JSON | 队列调度状态 |
| `GET /` | HTML | 返回 index.html 静态页面 |
| `GET /css/style.css` | CSS | 返回样式文件 |
| `GET /js/dashboard.js` | JS | 返回前端逻辑 |

## 前端仪表盘

基于 ECharts 5.5.0 的交互式 Web Dashboard，包含：
1. **KPI 卡片行**：总节点数、总内存、总 vCore、活跃容器、活跃应用
2. **容器调度趋势图**（折线图）：容器分配/释放趋势
3. **资源利用率图**（面积图）：集群资源使用率
4. **应用状态图**（柱状图）：活跃/完成/失败应用数
5. **心跳延迟图**（折线图）：心跳延迟和吞吐量
6. **队列状态表**：各队列容量和活跃应用数
7. **时间范围选择器**：30m / 1h / 6h / 1d / 7d
8. **自动刷新**：每 10s 拉取当前数据

数据流：前端每 10s 调用 `GET /api/metrics/current` 刷新 KPI 卡片；切换时间范围时调用 `GET /api/metrics/history?range=xxx` 获取图表数据。

## 存储架构

```
MetricsCollector (每5s)
    │
    ├──→ MetricsSnapshot
    │       ├──→ MetricsStore (内存 RingBuffer, 最多 3600 条 ≈ 5h)
    │       │       └──→ REST API 提供近期时序数据 ← 前端拉取
    │       └──→ MetricsDatabase (异步批量写入)
    │               └──→ SQLite .db 文件
    │
DataCleanupTask (每1h) → 删除过期数据 + VACUUM
```

## 类间关系

| 类 | 依赖 | 说明 |
|----|------|------|
| `MetricsSnapshot` | 无 | 纯 POJO，所有字段为 `long`/`int`/`Map` |
| `MetricsStore` | `MetricsSnapshot` | 环形缓冲区，`add()` / `query(range)` |
| `MetricsDatabase` | `MetricsSnapshot` | SQLite 建表/批量写入/查询/清理 |
| `MetricsCollector` | `MetricsStore`, `MetricsDatabase` | 定时调度，从 `/metrics` 拉数据 |
| `MetricsApiHandler` | `MetricsStore`, `MetricsDatabase` | 处理 HTTP 请求，构建 JSON 响应 |
| `SLSMetrics` | `MetricsCollector`, `MetricsApiHandler` | main 入口，组装并启动 |

## 配置项

```properties
# 采集与 Web Dashboard
yarn.metrics.collect.interval=5000
yarn.metrics.server.url=http://localhost:28080
yarn.metrics.web.port=28081
yarn.metrics.store.size=3600

# 数据库
yarn.metrics.db.path=target/metrics/metrics.db
yarn.metrics.db.batch.size=10
yarn.metrics.db.retention.days=7
yarn.metrics.db.cleanup.interval=3600000
```

## 数据流向

```
用户浏览器                     SLSMetrics 进程(28081)           MetricsServer(28080)
    │                              │                              │
    │--- GET / (index.html) -----→│                              │
    │←--- HTML + CSS + JS -------│                              │
    │                              │                              │
    │--- GET /api/metrics/current-→│                              │
    │                              │--- GET /metrics -----------→│
    │                              │←--- JSON metrics ----------│
    │                              │(存入 RingBuffer + SQLite)   │
    │←--- JSON current data -----│                              │
    │                              │                              │
    │--- GET /api/metrics/history-→│                              │
    │                              │(从 RingBuffer/SQLite 查询)  │
    │←--- JSON history data ----│                              │
```

## ⚠️ 已知数据限制

当前 MVP 阶段，MetricsCollector 仅从 MetricsServer HTTP API 采集数据。MetricsServer 仅输出以下字段：
- `totalNodes`、`totalContainersAllocated/released`、`activeContainers`
- `applications.active/completed/failed`
- `nodeHeartbeatMetrics`（心跳延迟统计）

**以下字段当前恒为 0，需后续通过 YarnClient RPC 补充：**
- 集群资源：`totalMemoryMB`、`totalVCores`、`allocatedMemoryMB/VCores`、`clusterUtilizationPercent`
- 调度队列：`queueMetrics`（含队列容量、待处理应用等）
- 高级调度：`pendingContainers`、`reservedContainers`、`submittedApplications`

**未来扩展**：在 `MetricsCollector` 中添加 `ResourceManagerMetricsCollector` 作为第二数据源，通过 YarnClient 获取 `YarnClusterMetrics`（集群资源）和 `QueueInfo`（队列调度状态），合并到快照中。

## 启动方式

```bash
# 先启动 SLSNodeManager（带 MetricsServer）
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.SLSNodeManager <config_dir>

# 再启动 SLSMetrics Web Dashboard（新终端）
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.metrics.SLSMetrics <config_dir>
```

或使用 `start-metrics.bat` / `start-metrics.sh` 脚本。
