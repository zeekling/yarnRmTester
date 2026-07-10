# SLSMetrics 监控服务设计文档

## 概述

实现 `org.apache.hadoop.sls.SLSMetrics` 独立进程，采集 YARN 集群的调度核心指标，持久化到 SQLite 数据库，并生成 PNG 监控图表。

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
├── SLSMetrics.java              ← main() 入口
├── MetricsCollector.java        ← 定时采集线程
├── MetricsSnapshot.java         ← 单次采集快照 POJO
├── MetricsStore.java            ← 内存环形缓冲区
├── MetricsDatabase.java         ← SQLite 持久化层
├── ChartGenerator.java          ← 图表生成器
└── charts/
    ├── ContainerTrendChart.java
    ├── ResourceUtilizationChart.java
    ├── ApplicationStatusChart.java
    └── HeartbeatLatencyChart.java
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

## 图表输出

| 文件 | 类型 | 内容 |
|------|------|------|
| `container-trend.png` | 折线图 | 容器分配/释放/活跃数 |
| `resource-util.png` | 面积图 | 内存和 vCore 利用率 |
| `app-status.png` | 柱状图 | 应用状态分布 |
| `heartbeat-latency.png` | 折线图 | 心跳延迟和吞吐量 |

## 配置项

```properties
# 采集与图表
yarn.metrics.collect.interval=5000
yarn.metrics.chart.interval=30000
yarn.metrics.store.size=3600
yarn.metrics.output.dir=target/metrics
yarn.metrics.server.url=http://localhost:28080

# 数据库
yarn.metrics.db.path=target/metrics/metrics.db
yarn.metrics.db.batch.size=10
yarn.metrics.db.retention.days=7
yarn.metrics.db.cleanup.interval=3600000
```

## 新增 Maven 依赖

- `org.jfree:jfreechart:1.5.5` — 图表生成
- `org.xerial:sqlite-jdbc:3.46.1.3` — SQLite 持久化

## 数据流向

```
MetricsCollector (每5s)
    │
    ├──→ MetricsSnapshot
    │       ├──→ MetricsStore (内存 RingBuffer)
    │       │       └──→ ChartGenerator (每30s) → PNG
    │       └──→ MetricsDatabase
    │               └──→ SQLite .db 文件
    │
DataCleanupTask (每1h) → 删除过期数据
```

## 启动方式

```bash
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.SLSMetrics <config_dir>
```
