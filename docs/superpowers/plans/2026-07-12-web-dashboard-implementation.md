# Web Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a web dashboard for SLSMetrics with real-time metrics visualization and historical trend charts using ECharts.

**Architecture:** Independent static HTML pages served by existing MetricsServer (port 28081), consuming REST APIs at `/api/metrics/latest` and `/api/metrics/history`.

**Tech Stack:** HTML5 + ECharts 5.4.3 + Vanilla JavaScript + Fetch API + CSS3

---

## File Structure Overview

```
src/main/resources/static/
├── index.html              # Main dashboard page (realtime + history)
├── realtime.html           # Real-time metrics only page
├── history.html            # Historical trend charts page
└── assets/                 # (Optional) CSS, JS files
    └── dashboard.css       # Shared styles
    └── dashboard.js        # Shared JavaScript utilities
```

---

## Tasks

### Task 1: Create static resources directory and base CSS

**Files:**
- Create: `src/main/resources/static/`
- Create: `src/main/resources/static/assets/dashboard.css`

- [ ] **Step 1: Create static directory**

Create directory: `src/main/resources/static/`

- [ ] **Step 2: Create shared CSS file**

Create `src/main/resources/static/assets/dashboard.css`:

```css
/* Global styles */
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
  background-color: #f5f7fa;
  color: #333;
  line-height: 1.6;
}

header {
  background-color: #2c3e50;
  color: white;
  padding: 1rem 2rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
}

header h1 {
  font-size: 1.5rem;
}

nav a {
  color: #ecf0f1;
  text-decoration: none;
  margin-left: 1.5rem;
  padding: 0.5rem 1rem;
  border-radius: 4px;
  transition: background-color 0.3s;
}

nav a:hover {
  background-color: #34495e;
}

nav a.active {
  background-color: #3498db;
}

main {
  padding: 2rem;
  max-width: 1400px;
  margin: 0 auto;
}

section {
  margin-bottom: 2rem;
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
  padding: 1.5rem;
}

section h2 {
  color: #2c3e50;
  margin-bottom: 1rem;
  border-bottom: 2px solid #ecf0f1;
  padding-bottom: 0.5rem;
}

#chart-container {
  width: 100%;
  height: 500px;
  margin-top: 1rem;
}

/* Metric cards */
.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 1.5rem;
  margin-bottom: 1.5rem;
}

.metric-card {
  background-color: #f8f9fa;
  border-radius: 8px;
  padding: 1.5rem;
  text-align: center;
  border: 1px solid #e9ecef;
}

.metric-card h3 {
  font-size: 0.9rem;
  color: #7f8c8d;
  margin-bottom: 0.5rem;
  text-transform: uppercase;
}

.metric-card .value {
  font-size: 2rem;
  font-weight: bold;
  color: #2c3e50;
}

.metric-card .unit {
  font-size: 0.8rem;
  color: #95a5a6;
}

/* Controls */
.controls {
  display: flex;
  gap: 1rem;
  margin-bottom: 1rem;
  flex-wrap: wrap;
}

.controls select,
.controls button {
  padding: 0.5rem 1rem;
  border: 1px solid #ddd;
  border-radius: 4px;
  background-color: white;
  font-size: 0.9rem;
}

.controls button {
  background-color: #3498db;
  color: white;
  border: none;
  cursor: pointer;
  transition: background-color 0.3s;
}

.controls button:hover {
  background-color: #2980b9;
}

.controls button:disabled {
  background-color: #bdc3c7;
  cursor: not-allowed;
}

/* Loading indicator */
#loading {
  display: none;
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background-color: rgba(0,0,0,0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

#loading .spinner {
  border: 4px solid #f3f3f3;
  border-top: 4px solid #3498db;
  border-radius: 50%;
  width: 50px;
  height: 50px;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

/* Error toast */
#error-toast {
  position: fixed;
  top: 20px;
  right: 20px;
  background-color: #e74c3c;
  color: white;
  padding: 1rem 1.5rem;
  border-radius: 4px;
  box-shadow: 0 4px 6px rgba(0,0,0,0.1);
  display: none;
  z-index: 1000;
}

footer {
  background-color: #ecf0f1;
  padding: 1rem 2rem;
  text-align: center;
  margin-top: 2rem;
}

footer p {
  color: #7f8c8d;
  font-size: 0.9rem;
}

/* Responsive */
@media (max-width: 768px) {
  header {
    flex-direction: column;
    gap: 1rem;
  }

  nav a {
    margin: 0.25rem 0;
  }

  .metrics-grid {
    grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
  }

  #chart-container {
    height: 400px;
  }
}
```

- [ ] **Step 3: Verify directory structure**

Run: `ls src/main/resources/static/`

Expected: `assets/` directory

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/static/assets/dashboard.css
git commit -m "feat(web): add dashboard CSS styles"
```

---

### Task 2: Create main dashboard page (index.html)

**Files:**
- Create: `src/main/resources/static/index.html`

- [ ] **Step 1: Create main dashboard HTML structure**

Create `src/main/resources/static/index.html`:

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SLSMetrics Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
  <link rel="stylesheet" href="assets/dashboard.css">
</head>
<body>
  <header>
    <h1>SLSMetrics Dashboard</h1>
    <nav>
      <a href="/index.html" class="active">Dashboard</a>
      <a href="/realtime.html">Realtime</a>
      <a href="/history.html">History</a>
    </nav>
  </header>

  <main>
    <!-- Module 1: Real-time Cluster Metrics -->
    <section id="realtime-module">
      <h2>实时集群指标</h2>
      <div class="controls">
        <button id="refresh-btn" title="手动刷新">🔄 刷新</button>
      </div>
      <div id="chart-container">
        <div id="realtime-chart"></div>
      </div>
      <div id="loading"></div>
    </section>

    <!-- Module 2: Historical Trend Charts -->
    <section id="history-module">
      <h2>历史趋势图</h2>
      <div class="controls">
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
        <button id="refresh-history-btn">🔄 刷新</button>
      </div>
      <div id="chart-container">
        <div id="history-chart"></div>
      </div>
      <div id="loading"></div>
    </section>
  </main>

  <footer>
    <p>Last update: <span id="last-update">-</span></p>
  </footer>

  <div id="error-toast"></div>

  <script src="assets/dashboard.js"></script>
</body>
</html>
```

- [ ] **Step 2: Verify file creation**

Run: `ls src/main/resources/static/index.html`

Expected: File exists

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/static/index.html
git commit -m "feat(web): add main dashboard HTML page"
```

---

### Task 3: Create real-time metrics page (realtime.html)

**Files:**
- Create: `src/main/resources/static/realtime.html`

- [ ] **Step 1: Create real-time metrics HTML structure**

Create `src/main/resources/static/realtime.html`:

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Real-time Metrics</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
  <link rel="stylesheet" href="assets/dashboard.css">
</head>
<body>
  <header>
    <h1>Real-time Cluster Metrics</h1>
    <nav>
      <a href="/index.html">Dashboard</a>
      <a href="/realtime.html" class="active">Realtime</a>
      <a href="/history.html">History</a>
    </nav>
  </header>

  <main>
    <!-- Chart Controls -->
    <section id="chart-controls-section">
      <h2>图表类型</h2>
      <div class="controls">
        <button id="pie-btn">饼图</button>
        <button id="doughnut-btn">环形图</button>
      </div>
    </section>

    <!-- Metric Cards -->
    <section id="metrics-section">
      <h2>实时指标卡片</h2>
      <div class="metrics-grid">
        <div class="metric-card">
          <h3>节点数</h3>
          <p class="value" id="val-nodes">-</p>
        </div>
        <div class="metric-card">
          <h3>总内存 (MB)</h3>
          <p class="value" id="val-total-memory">-</p>
        </div>
        <div class="metric-card">
          <h3>已分配内存 (MB)</h3>
          <p class="value" id="val-allocated-memory">-</p>
        </div>
        <div class="metric-card">
          <h3>可用内存 (MB)</h3>
          <p class="value" id="val-available-memory">-</p>
        </div>
        <div class="metric-card">
          <h3>已用内存率</h3>
          <p class="value" id="val-memory-utilization">-</p>
        </div>
        <div class="metric-card">
          <h3>总 vCores</h3>
          <p class="value" id="val-total-vcores">-</p>
        </div>
        <div class="metric-card">
          <h3>已用 vCores</h3>
          <p class="value" id="val-allocated-vcores">-</p>
        </div>
        <div class="metric-card">
          <h3>已用 vCores 率</h3>
          <p class="value" id="val-vcore-utilization">-</p>
        </div>
        <div class="metric-card">
          <h3>活跃容器</h3>
          <p class="value" id="val-active-containers">-</p>
        </div>
        <div class="metric-card">
          <h3>活跃应用</h3>
          <p class="value" id="val-active-apps">-</p>
        </div>
        <div class="metric-card">
          <h3>已完成应用</h3>
          <p class="value" id="val-completed-apps">-</p>
        </div>
        <div class="metric-card">
          <h3>失败应用</h3>
          <p class="value" id="val-failed-apps">-</p>
        </div>
      </div>
    </section>

    <!-- Chart Section -->
    <section id="chart-section">
      <h2>集群资源分配</h2>
      <div id="chart-container">
        <div id="realtime-chart"></div>
      </div>
      <div id="loading"></div>
    </section>
  </main>

  <footer>
    <p>Last update: <span id="last-update">-</span></p>
  </footer>

  <div id="error-toast"></div>

  <script src="assets/dashboard.js"></script>
</body>
</html>
```

- [ ] **Step 2: Verify file creation**

Run: `ls src/main/resources/static/realtime.html`

Expected: File exists

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/static/realtime.html
git commit -m "feat(web): add real-time metrics page"
```

---

### Task 4: Create historical trend page (history.html)

**Files:**
- Create: `src/main/resources/static/history.html`

- [ ] **Step 1: Create historical trend HTML structure**

Create `src/main/resources/static/history.html`:

```html
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Historical Trend</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>
  <link rel="stylesheet" href="assets/dashboard.css">
</head>
<body>
  <header>
    <h1>Historical Trend Charts</h1>
    <nav>
      <a href="/index.html">Dashboard</a>
      <a href="/realtime.html">Realtime</a>
      <a href="/history.html" class="active">History</a>
    </nav>
  </header>

  <main>
    <!-- Chart Controls -->
    <section id="chart-controls-section">
      <h2>图表控制</h2>
      <div class="controls">
        <label>
          <span>时间范围:</span>
          <select id="time-range">
            <option value="1h">最近 1 小时</option>
            <option value="6h">最近 6 小时</option>
            <option value="24h">最近 24 小时</option>
            <option value="7d">最近 7 天</option>
          </select>
        </label>
        <label>
          <span>数据类型:</span>
          <select id="data-type">
            <option value="memory">内存利用率</option>
            <option value="container">容器分配</option>
            <option value="heartbeat">心跳成功率</option>
          </select>
        </label>
        <button id="refresh-btn">🔄 刷新</button>
      </div>
    </section>

    <!-- Chart Section -->
    <section id="chart-section">
      <h2>趋势图表</h2>
      <div id="chart-container">
        <div id="history-chart"></div>
      </div>
      <div id="loading"></div>
    </section>
  </main>

  <footer>
    <p>Last update: <span id="last-update">-</span></p>
  </footer>

  <div id="error-toast"></div>

  <script src="assets/dashboard.js"></script>
</body>
</html>
```

- [ ] **Step 2: Verify file creation**

Run: `ls src/main/resources/static/history.html`

Expected: File exists

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/static/history.html
git commit -m "feat(web): add historical trend page"
```

---

### Task 5: Create shared JavaScript utilities (dashboard.js)

**Files:**
- Create: `src/main/resources/static/assets/dashboard.js`

- [ ] **Step 1: Create shared JavaScript file**

Create `src/main/resources/static/assets/dashboard.js`:

```javascript
// Global variables
let realtimeChart = null;
let historyChart = null;
let realtimeRefreshInterval = null;
let historyRefreshInterval = null;

// Chart configuration constants
const ECHARTS_VERSION = '5.4.3';

// Fetch with retry
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

      const waitTime = delay * Math.pow(2, i);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
}

// Show/hide loading indicator
function showLoading(show, containerId = 'loading') {
  const el = document.getElementById(containerId);
  if (el) {
    el.style.display = show ? 'block' : 'none';
  }
}

// Show error toast
function showError(message) {
  const toast = document.getElementById('error-toast');
  toast.textContent = message;
  toast.style.display = 'block';

  setTimeout(() => {
    toast.style.display = 'none';
  }, 5000);
}

// Format timestamp to HH:mm:ss
function formatTimestamp(ts) {
  const date = new Date(ts);
  return date.toLocaleTimeString('zh-CN', { hour12: false });
}

// Update last update timestamp
function updateLastUpdate(timestamp) {
  const el = document.getElementById('last-update');
  if (el) {
    el.textContent = formatTimestamp(timestamp);
  }
}

// Initialize charts
function initCharts() {
  // Initialize realtime chart
  const realtimeChartEl = document.getElementById('realtime-chart');
  if (realtimeChartEl) {
    realtimeChart = echarts.init(realtimeChartEl);
  }

  // Initialize history chart
  const historyChartEl = document.getElementById('history-chart');
  if (historyChartEl) {
    historyChart = echarts.init(historyChartEl);
  }

  // Resize handler
  window.addEventListener('resize', () => {
    if (realtimeChart) {
      realtimeChart.resize();
    }
    if (historyChart) {
      historyChart.resize();
    }
  });
}

// Realtime chart configuration
function getRealtimeChartOption(data) {
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

  return {
    tooltip: {
      trigger: 'item',
      formatter: '{b}: {c} ({d}%)'
    },
    legend: {
      bottom: 10,
      orient: 'horizontal',
      type: 'scroll',
      pageTextStyle: {
        color: '#666'
      }
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
      data: seriesData
    }]
  };
}

// History chart configuration
function getHistoryChartOption(timestamps, memoryUtilization, containerAllocation) {
  return {
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
      top: '5%',
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
}

// Fetch and update realtime metrics
async function fetchRealtimeMetrics() {
  const url = '/api/metrics/latest';

  try {
    showLoading(true);
    const data = await fetchWithRetry(url);

    if (!data || !data.cluster) {
      showError('无法获取实时指标数据');
      return;
    }

    // Update realtime chart
    if (realtimeChart) {
      realtimeChart.setOption(getRealtimeChartOption(data));
    }

    // Update metric cards (for realtime.html)
    updateRealtimeCards(data);

    // Update last update timestamp
    updateLastUpdate(data.timestamp);

  } catch (error) {
    console.error('Failed to fetch realtime metrics:', error);
    showError('无法加载实时指标，请稍后再试');
  } finally {
    showLoading(false);
  }
}

// Update metric cards (for realtime.html page)
function updateRealtimeCards(data) {
  const cards = {
    'val-nodes': data.cluster.totalNodes,
    'val-total-memory': data.cluster.totalMemoryMB,
    'val-allocated-memory': data.cluster.allocatedMemoryMB,
    'val-available-memory': data.cluster.availableMemoryMB,
    'val-memory-utilization': data.cluster.memoryUtilization ? data.cluster.memoryUtilization.toFixed(2) + '%' : '-',
    'val-total-vcores': data.cluster.totalVCores,
    'val-allocated-vcores': data.cluster.allocatedVCores,
    'val-vcore-utilization': data.cluster.vcoreUtilization ? data.cluster.vcoreUtilization.toFixed(2) + '%' : '-',
    'val-active-containers': data.container.activeContainers,
    'val-active-apps': data.application.activeApps,
    'val-completed-apps': data.application.completedApps,
    'val-failed-apps': data.application.failedApps
  };

  for (const [id, value] of Object.entries(cards)) {
    const el = document.getElementById(id);
    if (el) {
      el.textContent = value;
    }
  }
}

// Fetch and update history metrics
async function fetchHistoryMetrics(range = '1h', dataType = 'memory') {
  const url = `/api/metrics/history?type=${dataType}&range=${range}&limit=100`;

  try {
    showLoading(true);
    const data = await fetchWithRetry(url);

    if (!data || !data.timestamps || data.timestamps.length === 0) {
      showError('无法获取历史趋势数据');
      return;
    }

    // Update history chart
    if (historyChart) {
      const memoryUtilization = data.series.memoryUtilization || [];
      const containerAllocation = data.series.containerAllocation || [];
      historyChart.setOption(getHistoryChartOption(data.timestamps, memoryUtilization, containerAllocation));
    }

    // Update last update timestamp
    updateLastUpdate(Date.now());

  } catch (error) {
    console.error('Failed to fetch history metrics:', error);
    showError('无法加载历史趋势，请稍后再试');
  } finally {
    showLoading(false);
  }
}

// Setup realtime refresh interval (for index.html)
function setupRealtimeRefresh() {
  if (realtimeRefreshInterval) {
    clearInterval(realtimeRefreshInterval);
  }

  realtimeRefreshInterval = setInterval(() => {
    fetchRealtimeMetrics();
  }, 5000);
}

// Setup history refresh interval (for index.html)
function setupHistoryRefresh() {
  if (historyRefreshInterval) {
    clearInterval(historyRefreshInterval);
  }

  historyRefreshInterval = setInterval(() => {
    const range = document.getElementById('time-range')?.value || '1h';
    const dataType = document.getElementById('data-type')?.value || 'memory';
    fetchHistoryMetrics(range, dataType);
  }, 30000);
}

// Setup event listeners for realtime.html
function setupRealtimePage() {
  const pieBtn = document.getElementById('pie-btn');
  const doughnutBtn = document.getElementById('doughnut-btn');

  if (pieBtn) {
    pieBtn.addEventListener('click', () => {
      realtimeChart.setOption({
        series: [{
          type: 'pie',
          radius: ['40%', '70%']
        }]
      });
    });
  }

  if (doughnutBtn) {
    doughnutBtn.addEventListener('click', () => {
      realtimeChart.setOption({
        series: [{
          type: 'pie',
          radius: ['60%', '80%']
        }]
      });
    });
  }

  // Manual refresh button
  const refreshBtn = document.getElementById('refresh-btn');
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      fetchRealtimeMetrics();
    });
  }
}

// Setup event listeners for history.html
function setupHistoryPage() {
  const timeRangeSelect = document.getElementById('time-range');
  const dataTypeSelect = document.getElementById('data-type');
  const refreshBtn = document.getElementById('refresh-btn');

  // Time range change
  if (timeRangeSelect) {
    timeRangeSelect.addEventListener('change', (e) => {
      const dataType = dataTypeSelect?.value || 'memory';
      fetchHistoryMetrics(e.target.value, dataType);
    });
  }

  // Data type change
  if (dataTypeSelect) {
    dataTypeSelect.addEventListener('change', (e) => {
      const range = timeRangeSelect?.value || '1h';
      fetchHistoryMetrics(range, e.target.value);
    });
  }

  // Manual refresh button
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      const range = timeRangeSelect?.value || '1h';
      const dataType = dataTypeSelect?.value || 'memory';
      fetchHistoryMetrics(range, dataType);
    });
  }
}

// Setup event listeners for index.html
function setupIndexPage() {
  const refreshBtn = document.getElementById('refresh-btn');
  const refreshHistoryBtn = document.getElementById('refresh-history-btn');
  const timeRangeSelect = document.getElementById('time-range');
  const dataTypeSelect = document.getElementById('data-type');

  // Manual refresh for realtime
  if (refreshBtn) {
    refreshBtn.addEventListener('click', () => {
      fetchRealtimeMetrics();
    });
  }

  // Manual refresh for history
  if (refreshHistoryBtn) {
    refreshHistoryBtn.addEventListener('click', () => {
      const range = timeRangeSelect?.value || '1h';
      const dataType = dataTypeSelect?.value || 'memory';
      fetchHistoryMetrics(range, dataType);
    });
  }

  // Automatic refresh setup
  setupRealtimeRefresh();
  setupHistoryRefresh();
}

// Initialize everything when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  initCharts();

  // Determine which page to load based on URL
  const path = window.location.pathname;

  if (path === '/realtime.html' || path.includes('/realtime.html')) {
    fetchRealtimeMetrics();
    setupRealtimePage();
  } else if (path === '/history.html' || path.includes('/history.html')) {
    const timeRangeSelect = document.getElementById('time-range');
    const range = timeRangeSelect?.value || '1h';
    const dataTypeSelect = document.getElementById('data-type');
    const dataType = dataTypeSelect?.value || 'memory';
    fetchHistoryMetrics(range, dataType);
    setupHistoryPage();
  } else {
    // Default: index.html
    fetchRealtimeMetrics();
    fetchHistoryMetrics();
    setupIndexPage();
  }
});
```

- [ ] **Step 2: Verify file creation**

Run: `ls src/main/resources/static/assets/dashboard.js`

Expected: File exists

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/static/assets/dashboard.js
git commit -m "feat(web): add shared JavaScript utilities"
```

---

### Task 6: Build and verify static resources

**Files:**
- Modify: `pom.xml` (verify Maven configuration)
- Build: Project

- [ ] **Step 1: Verify Maven build configuration**

Read `pom.xml` to ensure static resources are properly configured.

Expected: Maven will automatically copy `src/main/resources/static/` to `target/classes/static/`

- [ ] **Step 2: Build project**

Run: `mvn clean package`

Expected: Build succeeds without errors

- [ ] **Step 3: Verify static files in target**

Run: `ls target/classes/static/`

Expected: `index.html`, `realtime.html`, `history.html`, `assets/` directory with `dashboard.css` and `dashboard.js`

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/static/
git commit -m "feat(web): complete static resources"
```

---

## Self-Review Checklist

### Spec Coverage

1. **Real-time cluster metrics** ✅
   - Task 1: CSS styles
   - Task 2: Main dashboard HTML
   - Task 3: Real-time metrics page
   - Task 5: ECharts configuration

2. **Historical trend charts** ✅
   - Task 1: CSS styles
   - Task 2: Main dashboard HTML
   - Task 4: Historical trend page
   - Task 5: ECharts configuration

3. **REST API integration** ✅
   - Task 5: `fetchRealtimeMetrics()` calls `/api/metrics/latest`
   - Task 5: `fetchHistoryMetrics()` calls `/api/metrics/history`

4. **Automatic refresh** ✅
   - Task 5: `setupRealtimeRefresh()` (5s interval)
   - Task 5: `setupHistoryRefresh()` (30s interval)

5. **Manual refresh** ✅
   - Task 5: Refresh buttons for all pages

6. **Error handling** ✅
   - Task 5: `fetchWithRetry()` with exponential backoff
   - Task 5: `showError()` toast notification
   - Task 5: Loading indicator

7. **Responsive layout** ✅
   - Task 1: CSS with media queries

8. **ECharts integration** ✅
   - Task 5: `getRealtimeChartOption()`
   - Task 5: `getHistoryChartOption()`

### Placeholder Scan

- [x] No "TBD", "TODO", "implement later"
- [x] No "fill in details"
- [x] No "Add error handling" (error handling implemented)
- [x] No "Write tests" (manual verification steps included)

### Type Consistency

- [x] Variable names consistent (`realtimeChart`, `historyChart`)
- [x] Function names consistent (`fetchRealtimeMetrics()`, `fetchHistoryMetrics()`)
- [x] CSS class names consistent (all lowercase with hyphens)
- [x] HTML IDs consistent (lowercase with hyphens)

### Design Adherence

- [x] Directory structure matches design document
- [x] File names match design document
- [x] API endpoints match design document (`/api/metrics/latest`, `/api/metrics/history`)
- [x] Refresh intervals match design document (5s / 30s)
- [x] ECharts configuration matches design document

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-12-web-dashboard-implementation.md`.

**Two execution options:**

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**
