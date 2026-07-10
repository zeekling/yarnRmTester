---
plan name: slsmetrics-impl
plan description: SLSMetrics implementation tasks
plan status: active
---

## Idea
实现 SLSMetrics 独立监控服务。包含：1) 新增 Maven 依赖 (JFreeChart + SQLite)；2) MetricsSnapshot 数据快照 POJO；3) MetricsStore 内存环形缓冲区；4) MetricsDatabase SQLite 持久化层（建表/写入/定时清理）；5) MetricsCollector 定时采集（HTTP轮询 MetricsServer + RM RPC）；6) 4个图表绘制类（容器趋势/资源利用率/应用状态/心跳延迟）；7) ChartGenerator 图表生成器；8) SLSMetrics 主入口 main 类；9) 单元测试；10) 编译验证

## Implementation
- Task 1: 更新 pom.xml 添加 JFreeChart + SQLite 依赖并编译验证
- Task 2: 创建 MetricsSnapshot POJO 数据快照类
- Task 3: 创建 MetricsStore 环形缓冲区(内存时序存储)
- Task 4: 创建 MetricsDatabase SQLite 持久化层
- Task 5: 创建 MetricsCollector 定时采集器
- Task 6: 创建 4个图表绘制类 (ContainerTrend/ResourceUtilization/ApplicationStatus/HeartbeatLatency)
- Task 7: 创建 ChartGenerator 图表生成器
- Task 8: 创建 SLSMetrics 主入口 main 类
- Task 9: 编写单元测试
- Task 10: 全面编译验证 + 扩展 fake.properites

## Required Specs
<!-- SPECS_START -->
- slsmetrics-design
<!-- SPECS_END -->