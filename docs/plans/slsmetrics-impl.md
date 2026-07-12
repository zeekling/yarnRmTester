---
plan name: slsmetrics-impl
plan description: SLSMetrics implementation tasks
plan status: done
---

## Idea
实现 SLSMetrics 独立监控服务（Web Dashboard 版）。包含：1) MetricsSnapshot 数据快照 POJO；2) MetricsStore 内存环形缓冲区；3) MetricsDatabase SQLite 持久化层（建表/写入/定时清理）；4) MetricsCollector 定时采集（HTTP轮询 MetricsServer）；5) MetricsApiHandler REST API + 静态文件服务；6) SLSMetrics 主入口（HTTP Server + 采集启动）；7) dashboard.js ECharts 前端逻辑；8) 单元测试；9) 编译验证

## Web Dashboard 架构（v2，替代 PNG JFreeChart）
采用 ECharts 交互式 Web Dashboard 替代原有的 JFreeChart PNG 图表方案。前端 HTML/CSS 已就绪，需新建 dashboard.js 前端逻辑 + MetricsApiHandler REST API 后端。

## Implementation
- Task 1: 创建 MetricsSnapshot POJO 数据快照类
- Task 2: 创建 MetricsStore 环形缓冲区(内存时序存储)
- Task 3: 创建 MetricsDatabase SQLite 持久化层
- Task 4: 创建 MetricsCollector 定时采集器
- Task 5: 创建 MetricsApiHandler REST API + 静态文件服务
- Task 6: 创建 SLSMetrics 主入口 main 类（HTTP Server + 组装启动）
- Task 7: 创建 dashboard.js ECharts 前端图表逻辑
- Task 8: 编写单元测试
- Task 9: 全面编译验证 + 编译通过

## Required Specs
<!-- SPECS_START -->
- slsmetrics-design
<!-- SPECS_END -->