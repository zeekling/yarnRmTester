# Spec: slsmetrics-design

Scope: repo

# SLSMetrics 监控服务设计（v2 — Web Dashboard）

SLSMetrics 是一个独立进程，用于采集 YARN 集群调度核心指标，持久化到 SQLite 数据库，并提供 ECharts Web Dashboard 交互式监控界面。

## 类结构
- `org.apache.hadoop.sls.metrics.SLSMetrics` — main() 入口（HTTP Server + 采集启动）
- `MetricsSnapshot` — 单次采集快照 POJO
- `MetricsStore` — 内存环形缓冲区（近期时序数据）
- `MetricsDatabase` — SQLite 持久化层（建表、批量写入、定时清理）
- `MetricsCollector` — 定时采集线程（轮询 MetricsServer HTTP）
- `MetricsApiHandler` — REST API 处理器 + 静态文件服务

前端（内嵌在 classpath）：
- `frontend/index.html` — ECharts 仪表盘（已有）
- `frontend/css/style.css` — 深色主题（已有）
- `frontend/js/dashboard.js` — ECharts 前端逻辑（需新建）

## REST API
| 端点 | 说明 |
|------|------|
| `GET /api/metrics/current` | 最新采集快照（KPI 卡片数据） |
| `GET /api/metrics/history?range=1h` | 时序历史数据（图表数据） |
| `GET /api/metrics/nodes` | 所有节点的心跳和容器详情 |
| `GET /api/metrics/queue` | 队列调度状态 |
| `GET /` | 静态文件服务 |

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

## 配置项
- yarn.metrics.collect.interval, store.size, server.url, web.port
- yarn.metrics.db.path, db.batch.size, db.retention.days, db.cleanup.interval
- (不需要 JFreeChart 相关配置，用 ECharts Web 前端替代 PNG 图表)

## 依赖
- org.xerial:sqlite-jdbc:3.46.1.3（已在 pom.xml 中）

详细设计见 docs/plans/2026-07-08-slsmetrics-design.md