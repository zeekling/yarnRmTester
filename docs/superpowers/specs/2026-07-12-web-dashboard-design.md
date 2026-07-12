# Web Dashboard Design for SLSMetrics

**设计日期**：2026-07-12
**状态**：✅ 已批准
**技术方案**：独立静态页面 + REST API

---

## 1. 项目概述

### 1.1 背景

SLSMetrics 项目提供了一个 YARN RM 压力测试的监控服务，现有功能包括：
- 实时指标采集（节点数、内存、vCore、容器数等）
- 历史数据持久化（SQLite 存储）
- REST API 接口（`/api/metrics/latest`, `/api/metrics/history`）

当前用户需要通过 REST API 直接查询数据，缺乏直观的可视化界面。

### 1.2 目标

提供一个 Web Dashboard，用于：
- 实时展示集群指标（节点、内存、vCore、容器、应用）
- 可视化历史趋势（内存利用率、容器分配趋势）
- 支持多种图表类型切换（饼图、环形图、折线图）

---

## 2. 技术方案：独立静态页面 + REST API

### 2.1 架构设计

```
┌─────────────────────────────────────────────────┐
│  Web Browser (Chrome/Firefox/Edge)              │
│  ┌───────────────────────────────────────────┐  │
│  │  index.html (主页面)                      │  │
│  │  - 模块 1: 实时集群指标 (饼图)            │  │
│  │  - 模块 2: 历史趋势图 (折线图)            │  │
│  │  - 定时刷新机制 (5s/30s)                  │  │
│  └───────────────────────────────────────────┘  │
│         ↓ Fetch API (REST)                      │
│  ┌───────────────────────────────────────────┐  │
│  │  SLSMetrics Server (端口 28081)            │  │
│  │  - /api/metrics/latest                    │  │
│  │  - /api/metrics/history                    │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

### 2.2 为什么选择方案 A？

| 对比项 | 方案 A（推荐） | 方案 B | 方案 C |
|-------|--------------|--------|--------|
| 开发成本 | ⭐⭐ 低 | ⭐⭐⭐ 中 | ⭐⭐⭐⭐ 高 |
| 实时性 | ⭐⭐⭐ 可接受 | ⭐⭐⭐⭐ 好 | ⭐⭐⭐⭐⭐ 最好 |
| 部署复杂度 | ⭐⭐ 低 | ⭐⭐⭐ 中 | ⭐⭐⭐⭐ 高 |
| 维护性 | ⭐⭐⭐⭐ 好 | ⭐⭐⭐ 中 | ⭐⭐⭐ 中 |
| 适用场景 | ⭐ 快速部署 | 内网统一部署 | 高频实时监控 |

**选择理由**：
1. 现有 REST API 已完善，无需后端修改
2. 前端独立，易于迭代和测试
3. 支持后续扩展（WebSocket 实时推送、多用户认证等）

---

## 3. 目录结构

```
yarnRmTester/
├── src/
│   ├── main/
│   │   ├── java/org/apache/hadoop/sls/metrics/
│   │   │   ├── SLSMetrics.java
│   │   │   ├── MetricsServer.java
│   │   │   ├── MetricsCollector.java
│   │   │   ├── MetricsDatabase.java
│   │   │   ├── MetricsStore.java
│   │   │   ├── MetricsSnapshot.java
│   │   │   ├── MetricsApiHandler.java
│   │   │   ├── MetricsHttpHandler.java
│   │   │   ├── ResourceManagerMetricsCollector.java
│   │   │   └── ...
│   │   │
│   │   ├── resources/
│   │   │   ├── static/                          # ⭐ 新增静态资源目录
│   │   │   │   ├── index.html                   # 主页面
│   │   │   │   ├── realtime.html                # 实时指标页面
│   │   │   │   └── history.html                 # 历史趋势页面
│   │   │   ├── fake.properites
│   │   │   ├── core-site.xml
│   │   │   ├── hdfs-site.xml
│   │   │   └── yarn-site.xml
│   │   │
│   │   └── resources/                           # （现有测试配置）
│   │       └── sls-test.properties
│   │
│   └── test/
│       └── ...
│
├── target/
│   ├── classes/                                  # Maven 编译输出
│   │   └── static/                              # ⭐ 静态文件会被复制到此处
│   │       ├── index.html
│   │       ├── realtime.html
│   │       └── history.html
│   └── lib/
│
└── pom.xml
```

**说明**：
- Maven 默认会自动复制 `src/main/resources/static/` 到 `target/classes/static/`
- `target/classes/` 会被包含在 classpath 中，可以通过 HTTP 服务器访问
- 无需额外配置，Maven Build 会自动处理

---

## 4. HTML 页面设计

### 4.1 主页面（index.html）

**布局结构**：

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <title>SLSMetrics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
  <style>
    /* CSS 样式 */
  </style>
</head>
<body>
  <header>
    <h1>SLSMetrics Dashboard</h1>
    <nav>
      <a href="/index.html">Dashboard</a>
      <a href="/realtime.html">Realtime</a>
      <a href="/history.html">History</a>
    </nav>
  </header>

  <main>
    <!-- 模块 1: 实时集群指标 -->
    <section id="realtime-module">
      <h2>实时集群指标</h2>
      <div id="realtime-chart"></div>
    </section>

    <!-- 模块 2: 历史趋势图 -->
    <section id="history-module">
      <h2>历史趋势图</h2>
      <div id="history-chart"></div>
    </section>
  </main>

  <footer>
    <p>Last update: <span id="last-update"></span></p>
  </footer>

  <script>
    // JavaScript 逻辑
  </script>
</body>
</html>
```

**样式设计要点**：
- 响应式布局（Flexbox + Grid）
- 现代化配色（蓝色主题）
- 图表自适应窗口大小
- 加载状态指示器

---

### 4.2 实时指标页面（realtime.html）

**功能**：
- 专注展示实时集群指标（饼图、环形图）
- 支持图表类型切换（饼图 ↔ 环形图）
- 节点级别详细指标（可选扩展）

**关键组件**：
```html
<div id="chart-controls">
  <button onclick="switchChartType('pie')">饼图</button>
  <button onclick="switchChartType('doughnut')">环形图</button>
</div>

<div id="metrics-container">
  <!-- 实时指标卡片列表 -->
  <div class="metric-card">
    <h3>节点数</h3>
    <p class="value" id="val-nodes">-</p>
  </div>
  <!-- ... 其他指标卡片 ... -->
</div>

<div id="chart-container">
  <div id="realtime-chart"></div>
</div>
```

---

### 4.3 历史趋势页面（history.html）

**功能**：
- 专注展示历史趋势（折线图）
- 支持时间范围选择（1h / 6h / 24h / 7d）
- 支持数据类型切换（内存利用率 / 容器分配 / 心跳成功率）
- 支持数据量调整（limit 参数）

**关键组件**：
```html
<div id="chart-controls">
  <select id="time-range">
    <option value="1h">最近 1 小时</option>
    <option value="6h">最近 6 小时</option>
    <option value="24h">最近 24 小时</option>
    <option value="7d">最近 7 天</option>
  </select>

  <select id="data-type">
    <option value="memory">内存利用率</option>
    <option value="container">容器分配</option>
    <option value="heartbeat">心跳成功率</option>
  </select>
</div>

<div id="history-chart"></div>
```

---

## 5. ECharts 配置

### 5.1 实时指标图表

**配置参数**：

```javascript
const realtimeOption = {
  tooltip: {
    trigger: 'item',
    formatter: '{b}: {c} ({d}%)'
  },
  legend: {
    bottom: 10,
    orient: 'horizontal'
  },
  series: [{
    type: 'pie',
    radius: ['40%', '70%'],
    avoidLabelOverlap: true,
    label: {
      show: true,
      formatter: '{b}\n{c} ({d}%)'
    },
    labelLine: {
      show: true
    },
    data: [
      { value: cluster.totalNodes, name: '节点数' },
      { value: cluster.totalMemoryMB, name: '总内存 (MB)' },
      { value: cluster.allocatedMemoryMB, name: '已分配内存 (MB)' },
      { value: cluster.availableMemoryMB, name: '可用内存 (MB)' },
      { value: cluster.totalVCores, name: '总 vCores' },
      { value: cluster.allocatedVCores, name: '已用 vCores' },
      { value: cluster.activeContainers, name: '活跃容器' },
      { value: cluster.activeApps, name: '活跃应用' },
      { value: cluster.completedApps, name: '已完成应用' },
      { value: cluster.failedApps, name: '失败应用' }
    ]
  }]
};
```

**图表类型切换逻辑**：

```javascript
let chartType = 'pie'; // 默认饼图

function switchChartType(type) {
  chartType = type;
  realtimeChart.setOption({
    series: [{
      type: type, // 'pie' 或 'doughnut'
      radius: type === 'pie' ? ['40%', '70%'] : ['60%', '80%']
    }]
  });
}
```

---

### 5.2 历史趋势图

**配置参数**：

```javascript
const historyOption = {
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross'
    },
    formatter: (params) => {
      let result = params[0].axisValue + '<br/>';
      params.forEach(p => {
        result += `${p.marker} ${p.seriesName}: ${p.value}<br/>`;
      });
      return result;
    }
  },
  grid: {
    left: '3%',
    right: '4%',
    bottom: '10%',
    containLabel: true
  },
  xAxis: {
    type: 'category',
    boundaryGap: false,
    data: timestamps,
    axisLabel: {
      rotate: 45,
      interval: Math.max(0, Math.floor(timestamps.length / 10))
    },
    axisPointer: {
      type: 'shadow'
    }
  },
  yAxis: {
    type: 'value',
    name: '数量 / 百分比',
    axisLabel: {
      formatter: '{value}'
    }
  },
  series: [
    {
      name: '内存利用率 (%)',
      type: 'line',
      smooth: true,
      data: memoryUtilization,
      itemStyle: { color: '#5470C6' },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(84, 112, 198, 0.3)' },
          { offset: 1, color: 'rgba(84, 112, 198, 0.1)' }
        ])
      }
    },
    {
      name: '容器分配数',
      type: 'line',
      smooth: true,
      data: containerAllocation,
      itemStyle: { color: '#91CC75' }
    }
  ]
};
```

---

## 6. 数据刷新机制

### 6.1 定时刷新策略

| 数据类型 | 刷新频率 | 刷新条件 | 说明 |
|---------|---------|---------|------|
| 实时指标 | 5 秒 | 每隔 5 秒 | 适合展示最新状态 |
| 历史趋势 | 30 秒 | 用户未切换时 | 减少服务器负载 |
| 手动刷新 | 立即 | 用户点击按钮 | 立即获取最新数据 |

**实现代码**：

```javascript
// 实时指标定时器（5 秒）
setInterval(() => {
  fetchRealtimeMetrics();
}, 5000);

// 历史趋势定时器（30 秒）
setInterval(() => {
  fetchHistoryMetrics();
}, 30000);

// 手动刷新按钮
document.getElementById('refresh-btn').addEventListener('click', () => {
  showLoading(true);
  fetchRealtimeMetrics()
    .then(() => fetchHistoryMetrics())
    .finally(() => showLoading(false));
});

// 时间范围切换时立即刷新
document.getElementById('time-range').addEventListener('change', (e) => {
  const range = e.target.value;
  showLoading(true);
  fetchHistoryMetrics(range)
    .finally(() => showLoading(false));
});
```

---

### 6.2 数据获取函数

```javascript
async function fetchRealtimeMetrics() {
  const url = '/api/metrics/latest';
  const loading = document.getElementById('realtime-loading');

  try {
    loading.style.display = 'block';
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    updateRealtimeChart(data);
    updateRealtimeText(data);

  } catch (error) {
    console.error('Failed to fetch realtime metrics:', error);
    showError('无法加载实时指标，请稍后再试');
  } finally {
    loading.style.display = 'none';
  }
}

async function fetchHistoryMetrics(range = '1h') {
  const url = `/api/metrics/history?type=memory&range=${range}&limit=100`;
  const loading = document.getElementById('history-loading');

  try {
    loading.style.display = 'block';
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    updateHistoryChart(data);

  } catch (error) {
    console.error('Failed to fetch history metrics:', error);
    showError('无法加载历史趋势，请稍后再试');
  } finally {
    loading.style.display = 'none';
  }
}
```

---

## 7. REST API 调用方式

### 7.1 API 端点详细说明

| 端点 | 用途 | 请求方法 | 请求参数 | 响应格式 | 页面使用 |
|------|------|---------|---------|---------|---------|
| `/api/metrics/latest` | 获取最新快照 | GET | 无 | JSON | 实时指标、主页面 |
| `/api/metrics/history` | 获取历史时序数据 | GET | type=memory<br>range=1h<br>limit=100 | JSON | 历史趋势页面 |
| `/metrics` | 获取节点级别指标 | GET | 无 | JSON | 节点详情页面（可选） |

### 7.2 响应数据结构

#### 7.2.1 `/api/metrics/latest` 响应

```json
{
  "timestamp": 1718110000000,
  "time": "10:00:00",
  "cluster": {
    "totalNodes": 10,
    "totalMemoryMB": 160000,
    "totalVCores": 160,
    "allocatedMemoryMB": 80000,
    "allocatedVCores": 80,
    "availableMemoryMB": 80000,
    "availableVCores": 80,
    "memoryUtilization": 50.0,
    "vcoreUtilization": 50.0
  },
  "container": {
    "activeContainers": 100,
    "pendingContainers": 20,
    "reservedContainers": 5
  },
  "application": {
    "activeApps": 15,
    "completedApps": 50,
    "failedApps": 2,
    "submittedApps": 67
  },
  "queue": {
    "queueName": "default",
    "usedCapacity": 0.5,
    "absoluteCapacity": 1.0,
    "pendingApps": 10,
    "activeApps": 15
  }
}
```

#### 7.2.2 `/api/metrics/history` 响应

```json
{
  "type": "memory",
  "range": "1h",
  "timestamps": ["10:00:00", "10:01:00", "10:02:00", ...],
  "series": {
    "memoryUtilization": [45.2, 46.1, 47.5, ...],
    "containerAllocation": [95, 98, 102, ...]
  }
}
```

### 7.3 前端数据映射

```javascript
// 实时指标图表数据映射
function updateRealtimeChart(data) {
  const seriesData = [
    { value: data.cluster.totalNodes, name: '节点数' },
    { value: data.cluster.totalMemoryMB, name: '总内存 (MB)' },
    { value: data.cluster.allocatedMemoryMB, name: '已分配内存 (MB)' },
    { value: data.cluster.availableMemoryMB, name: '可用内存 (MB)' },
    { value: data.cluster.totalVCores, name: '总 vCores' },
    { value: data.cluster.allocatedVCores, name: '已用 vCores' },
    { value: data.container.activeContainers, name: '活跃容器' },
    { value: data.application.activeApps, name: '活跃应用' },
    { value: data.application.completedApps, name: '已完成应用' },
    { value: data.application.failedApps, name: '失败应用' }
  ];

  realtimeChart.setOption({
    series: [{ data: seriesData }]
  });
}

// 历史趋势图数据映射
function updateHistoryChart(data) {
  const timestamps = data.timestamps;
  const series = [
    {
      name: '内存利用率 (%)',
      type: 'line',
      data: data.series.memoryUtilization,
      itemStyle: { color: '#5470C6' }
    },
    {
      name: '容器分配数',
      type: 'line',
      data: data.series.containerAllocation,
      itemStyle: { color: '#91CC75' }
    }
  ];

  historyChart.setOption({
    xAxis: { data: timestamps },
    series: series
  });
}
```

---

## 8. 错误处理

### 8.1 网络错误处理

```javascript
async function fetchWithRetry(url, maxRetries = 3, delay = 1000) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      return await response.json();
    } catch (error) {
      console.error(`Attempt ${i + 1} failed:`, error);

      if (i === maxRetries - 1) {
        throw error;
      }

      // 指数退避
      const waitTime = delay * Math.pow(2, i);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
}
```

### 8.2 加载状态指示器

```javascript
// 显示/隐藏加载指示器
function showLoading(show, containerId = 'realtime-loading') {
  const el = document.getElementById(containerId);
  if (el) {
    el.style.display = show ? 'block' : 'none';
  }
}

// 错误提示
function showError(message) {
  const toast = document.getElementById('error-toast');
  toast.textContent = message;
  toast.style.display = 'block';

  setTimeout(() => {
    toast.style.display = 'none';
  }, 5000);
}
```

### 8.3 空数据处理

```javascript
function handleEmptyData(data, type) {
  if (!data || !data.cluster) {
    return { hasData: false };
  }

  if (type === 'realtime' && !data.cluster.totalNodes) {
    return { hasData: false, message: '暂无数据' };
  }

  if (type === 'history' && (!data.timestamps || data.timestamps.length === 0)) {
    return { hasData: false, message: '暂无历史数据' };
  }

  return { hasData: true };
}
```

---

## 9. 部署和访问

### 9.1 启动步骤

```bash
# 1. 编译打包项目
mvn clean package

# 2. 启动 SLSMetrics 服务
java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.metrics.SLSMetrics

# 3. 访问 Web Dashboard
# 打开浏览器访问：
#   - http://localhost:28081/index.html
#   - http://localhost:28081/realtime.html
#   - http://localhost:28081/history.html
```

### 9.2 访问路径说明

- **主页面**：`http://localhost:28081/index.html`
  - 包含实时指标 + 历史趋势两个模块
- **实时指标页面**：`http://localhost:28081/realtime.html`
  - 专注展示实时集群指标
  - 支持图表类型切换
- **历史趋势页面**：`http://localhost:28081/history.html`
  - 专注展示历史趋势
  - 支持时间范围和数据类型选择

### 9.3 生产环境部署

**注意事项**：
1. **HTTPS**：生产环境建议启用 HTTPS（自签名证书或 CA 证书）
2. **防火墙**：开放端口 28081
3. **反向代理**：建议使用 Nginx/Apache 作为反向代理
4. **静态资源优化**：
   - 压缩 HTML/CSS/JS 文件
   - 使用 CDN 加载 ECharts
   - 启用浏览器缓存
5. **监控和日志**：记录 API 请求日志和错误日志

---

## 10. 实现步骤

### 10.1 阶段 1：基础框架（高优先级）

1. ✅ 创建 `src/main/resources/static/` 目录
2. ✅ 创建 `index.html` 主页面框架
3. ✅ 创建 `realtime.html` 实时指标页面
4. ✅ 创建 `history.html` 历史趋势页面
5. ✅ 引入 ECharts CDN 脚本

### 10.2 阶段 2：核心功能（高优先级）

1. ✅ 实现实时指标图表（ECharts 饼图）
2. ✅ 实现历史趋势图表（ECharts 折线图）
3. ✅ 实现定时刷新机制（5s/30s）
4. ✅ 实现 REST API 调用逻辑
5. ✅ 实现图表数据映射

### 10.3 阶段 3：交互优化（中优先级）

1. ✅ 实现图表类型切换（饼图 ↔ 环形图）
2. ✅ 实现时间范围选择（1h / 6h / 24h / 7d）
3. ✅ 实现数据类型切换（内存 / 容器 / 心跳）
4. ✅ 实现加载状态指示器
5. ✅ 实现错误提示和重试机制
6. ✅ 实现手动刷新按钮
7. ✅ 实现响应式布局和样式优化

### 10.4 阶段 4：测试和验证（中优先级）

1. ✅ 手动测试页面功能
2. ✅ 测试 API 调用和错误处理
3. ✅ 测试不同浏览器兼容性
4. ✅ 测试静态资源路径配置
5. ✅ 性能测试（大量数据时）

### 10.5 阶段 5：文档和部署（低优先级）

1. ✅ 编写使用文档（README.md）
2. ✅ 创建部署脚本
3. ✅ 编写单元测试（可选）
4. ✅ 编写集成测试（可选）

---

## 11. 后续扩展

### 11.1 短期优化（可选）

- [ ] 节点级别详细指标页面
- [ ] WebSocket 实时推送（替代定时刷新）
- [ ] 数据导出功能（CSV/PDF）
- [ ] 用户认证和权限控制
- [ ] 多集群支持

### 11.2 长期优化（可选）

- [ ] 移动端适配
- [ ] 暗色主题切换
- [ ] 自定义图表配置（用户可配置数据源）
- [ ] 数据聚合和统计功能
- [ ] 告警和通知功能

---

## 12. 附录

### 12.1 依赖清单

| 依赖 | 版本 | 用途 | 来源 |
|------|------|------|------|
| ECharts | 5.4.3 | 图表渲染 | CDN (jsdelivr) |
| Fetch API | 浏览器内置 | HTTP 请求 | - |
| CSS3 | 浏览器内置 | 样式渲染 | - |

### 12.2 浏览器兼容性

| 浏览器 | 版本要求 | 支持状态 |
|--------|---------|---------|
| Chrome | 90+ | ✅ 完全支持 |
| Firefox | 88+ | ✅ 完全支持 |
| Safari | 14+ | ✅ 完全支持 |
| Edge | 90+ | ✅ 完全支持 |

### 12.3 常见问题（FAQ）

**Q1: 页面显示空白？**
- A: 检查是否启动了 SLSMetrics 服务
- A: 检查浏览器控制台是否有 JavaScript 错误
- A: 确认 `/api/metrics/latest` 接口是否正常返回数据

**Q2: 图表不刷新？**
- A: 检查网络请求是否成功（F12 Network 面板）
- A: 检查定时器是否正常运行
- A: 清除浏览器缓存后重试

**Q3: 静态资源 404？**
- A: 确认 Maven Build 是否成功
- A: 检查 `target/classes/static/` 目录是否存在
- A: 重新执行 `mvn clean package`

---

**文档版本**：v1.0
**最后更新**：2026-07-12
**作者**：PuaSE
