# Spec: slsmetrics-design

Scope: repo

# SLSMetrics 监控服务设计

SLSMetrics 是一个独立进程，用于采集 YARN 集群调度核心指标，持久化到 SQLite 数据库，并生成 PNG 监控图表。

## 类结构
- `org.apache.hadoop.sls.metrics.SLSMetrics` — main() 入口
- `MetricsSnapshot` — 单次采集快照 POJO
- `MetricsStore` — 内存环形缓冲区
- `MetricsDatabase` — SQLite 持久化层（建表、批量写入、定时清理）
- `MetricsCollector` — 定时采集线程（轮询 MetricsServer + RM）
- `ChartGenerator` — 图表生成器
- `charts/ContainerTrendChart` — 容器趋势折线图
- `charts/ResourceUtilizationChart` — 资源利用率面积图
- `charts/ApplicationStatusChart` — 应用状态柱状图
- `charts/HeartbeatLatencyChart` — 心跳延迟折线图

## 核心指标（5大类）
1. 集群资源：totalNodes, totalMemory, totalVCores, allocatedMemory/VCores, utilization
2. 容器调度：allocated, released, active, pending, reserved, rate
3. 应用调度：active, completed, failed, submitted
4. 节点心跳：success/failed counts, success rate, avg/max latency, throughput
5. 队列调度：queueName, usedCapacity, absCapacity, pendingApps, activeApps

## 数据库（SQLite）
- 5张表：cluster_resource_snapshots, container_scheduling_snapshots, application_snapshots, heartbeat_snapshots, queue_snapshots
- 采集间隔 5s
- 数据保留默认 7 天，可配置，每小时清理一次

## 图表输出（PNG）
- container-trend.png, resource-util.png, app-status.png, heartbeat-latency.png
- 每 30s 生成一次

## 配置项
- yarn.metrics.collect.interval, chart.interval, store.size, output.dir, server.url
- yarn.metrics.db.path, db.batch.size, db.retention.days, db.cleanup.interval

## 新增依赖
- org.jfree:jfreechart:1.5.5
- org.xerial:sqlite-jdbc:3.46.1.3

详细设计见 docs/plans/2026-07-08-slsmetrics-design.md