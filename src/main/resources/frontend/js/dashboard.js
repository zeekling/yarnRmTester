/**
 * SLSMetrics 监控仪表盘 - Dashboard 主逻辑
 * 依赖：ECharts 5.5.0（通过 CDN 加载）
 *
 * 功能模块：
 * - 4 个 ECharts 图表（容器调度趋势、资源利用率、应用状态、心跳延迟）
 * - 5 个 KPI 卡片（节点数、内存、vCore、活跃容器、活跃应用）
 * - 自动刷新机制（10秒间隔，可暂停/继续）
 * - 时间范围选择（30m / 1h / 6h / 1d / 7d）
 * - 队列状态表格
 * - 窗口自适应
 */

// ============================================
// 全局状态
// ============================================
const REFRESH_INTERVAL = 10000; // 自动刷新间隔（毫秒）
const COUNTDOWN_STEP = 1000;    // 倒计时更新步进（毫秒）
let refreshTimer = null;        // 自动刷新定时器
let countdownTimer = null;      // 倒计时的定时器
let isPaused = false;          // 是否暂停自动刷新
let countdown = 10;            // 倒计时秒数
let currentRange = '1h';       // 当前选中的时间范围
const charts = [];             // 所有 ECharts 实例，用于批量 resize
var chartQueuePending = null;  // 队列待定容器柱状图（独立变量，非 time-series）

// ============================================
// ECharts 深色主题通用配置工厂函数
// 每次调用返回独立副本，避免多实例间引用共享
// ============================================
function createDarkTheme() {
    return {
        backgroundColor: 'transparent',
        textStyle: { color: '#aaa' },

        grid: {
            left: 60,
            right: 20,
            top: 30,
            bottom: 30,
            containLabel: false
        },

        xAxis: {
            type: 'time',
            axisLine: { lineStyle: { color: '#2a2a4a' } },
            axisTick: { lineStyle: { color: '#2a2a4a' } },
            axisLabel: { color: '#aaa', fontSize: 11 },
            splitLine: { show: false }
        },

        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: { color: '#aaa', fontSize: 11 },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } }
        },

        tooltip: {
            trigger: 'axis',
            backgroundColor: '#1a1a2e',
            borderColor: '#2a2a4a',
            borderWidth: 1,
            textStyle: { color: '#e0e0e0', fontSize: 12 },
            formatter: function (params) {
                if (!params || params.length === 0) return '';
                var first = params[0];
                if (!first || !first.value || first.value.length < 2) return '';
                var date = new Date(first.value[0]);
                var pad = function (n) { return n < 10 ? '0' + n : '' + n; };
                var timeStr = pad(date.getMonth() + 1) + '-' + pad(date.getDate()) + ' '
                    + pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
                var html = '<div style="font-weight:600;margin-bottom:4px;border-bottom:1px solid #2a2a4a;padding-bottom:4px;">'
                    + timeStr + '</div>';
                for (var i = 0; i < params.length; i++) {
                    var p = params[i];
                    if (p.value && p.value.length >= 2) {
                        var val = p.value[1];
                        var displayVal = val != null ? val : '-';
                        html += '<div style="display:flex;align-items:center;gap:6px;padding:2px 0;">'
                            + '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:'
                            + p.color + ';"></span>'
                            + p.seriesName + ': <strong>' + displayVal + '</strong>'
                            + '</div>';
                    }
                }
                return html;
            }
        },

        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 }
        }
    };
}

// ============================================
// 1. 初始化 ECharts 图表（4 个）
// ============================================
function initCharts() {
    initChartContainer();
    initChartResource();
    initChartApplication();
    initChartHeartbeat();
    initChartAvailableNodes();
    initChartAvailableResources();

    // 所有图表显示 loading（首次加载时使用）
    charts.forEach(function (chart) {
        chart.showLoading({
            text: '加载中...',
            textColor: '#aaa',
            maskColor: 'rgba(15, 15, 26, 0.6)',
            lineWidth: 2,
            spinnerColor: '#3498db'
        });
    });

    if (chartQueuePending) {
        chartQueuePending.showLoading({
            text: '加载中...',
            textColor: '#aaa',
            maskColor: 'rgba(15, 15, 26, 0.6)',
            lineWidth: 2,
            spinnerColor: '#3498db'
        });
    }
}

/**
 * 图1：容器调度趋势（折线图）
 */
function initChartContainer() {
    var dom = document.getElementById('chart-container');
    if (!dom) return;
    var chart = echarts.init(dom);
    var theme = createDarkTheme();
    chart.setOption({
        backgroundColor: theme.backgroundColor,
        tooltip: theme.tooltip,
        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 },
            data: ['容器分配数', '容器释放数', '活跃容器数']
        },
        grid: theme.grid,
        xAxis: theme.xAxis,
        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: { color: '#aaa', fontSize: 11 },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } },
            name: '数量',
            nameTextStyle: { color: '#888', fontSize: 11 }
        },
        series: [
            {
                name: '容器分配数',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#3498db' },
                itemStyle: { color: '#3498db' },
                data: []
            },
            {
                name: '容器释放数',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#e74c3c' },
                itemStyle: { color: '#e74c3c' },
                data: []
            },
            {
                name: '活跃容器数',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#2ecc71' },
                itemStyle: { color: '#2ecc71' },
                data: []
            }
        ]
    });
    charts.push(chart);
}

/**
 * 图2：资源利用率（堆叠面积图）
 */
function initChartResource() {
    var dom = document.getElementById('chart-resource');
    if (!dom) return;
    var chart = echarts.init(dom);
    var theme = createDarkTheme();
    chart.setOption({
        backgroundColor: theme.backgroundColor,
        tooltip: theme.tooltip,
        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 },
            data: ['已用内存占比', '已用 vCore 占比']
        },
        grid: theme.grid,
        xAxis: theme.xAxis,
        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: {
                color: '#aaa',
                fontSize: 11,
                formatter: '{value}%'
            },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } },
            name: '利用率 (%)',
            nameTextStyle: { color: '#888', fontSize: 11 },
            max: 100
        },
        series: [
            {
                name: '已用内存占比',
                type: 'line',
                symbol: 'none',
                smooth: true,
                stack: 'resource',
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(52, 152, 219, 0.4)' },
                        { offset: 1, color: 'rgba(52, 152, 219, 0.05)' }
                    ])
                },
                lineStyle: { width: 2, color: '#3498db' },
                itemStyle: { color: '#3498db' },
                data: []
            },
            {
                name: '已用 vCore 占比',
                type: 'line',
                symbol: 'none',
                smooth: true,
                stack: 'resource',
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(230, 126, 34, 0.4)' },
                        { offset: 1, color: 'rgba(230, 126, 34, 0.05)' }
                    ])
                },
                lineStyle: { width: 2, color: '#e67e22' },
                itemStyle: { color: '#e67e22' },
                data: []
            }
        ]
    });
    charts.push(chart);
}

/**
 * 图3：应用状态（柱状图）
 */
function initChartApplication() {
    var dom = document.getElementById('chart-application');
    if (!dom) return;
    var chart = echarts.init(dom);
    var theme = createDarkTheme();
    chart.setOption({
        backgroundColor: theme.backgroundColor,
        tooltip: theme.tooltip,
        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 },
            data: ['活跃应用', '已完成应用', '失败应用']
        },
        grid: theme.grid,
        xAxis: theme.xAxis,
        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: { color: '#aaa', fontSize: 11 },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } },
            name: '数量',
            nameTextStyle: { color: '#888', fontSize: 11 }
        },
        series: [
            {
                name: '活跃应用',
                type: 'bar',
                barWidth: '24%',
                itemStyle: { color: '#2ecc71', borderRadius: [2, 2, 0, 0] },
                data: []
            },
            {
                name: '已完成应用',
                type: 'bar',
                barWidth: '24%',
                itemStyle: { color: '#3498db', borderRadius: [2, 2, 0, 0] },
                data: []
            },
            {
                name: '失败应用',
                type: 'bar',
                barWidth: '24%',
                itemStyle: { color: '#e74c3c', borderRadius: [2, 2, 0, 0] },
                data: []
            }
        ]
    });
    charts.push(chart);
}

/**
 * 图4：心跳延迟（折线图 + 面积）
 */
function initChartHeartbeat() {
    var dom = document.getElementById('chart-heartbeat');
    if (!dom) return;
    var chart = echarts.init(dom);
    var theme = createDarkTheme();
    chart.setOption({
        backgroundColor: theme.backgroundColor,
        tooltip: theme.tooltip,
        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 },
            data: ['平均延迟', '最大延迟']
        },
        grid: theme.grid,
        xAxis: theme.xAxis,
        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: {
                color: '#aaa',
                fontSize: 11,
                formatter: '{value} ms'
            },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } },
            name: '延迟 (ms)',
            nameTextStyle: { color: '#888', fontSize: 11 }
        },
        series: [
            {
                name: '平均延迟',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#f39c12' },
                itemStyle: { color: '#f39c12' },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(243, 156, 18, 0.2)' },
                        { offset: 1, color: 'rgba(243, 156, 18, 0.02)' }
                    ])
                },
                data: []
            },
            {
                name: '最大延迟',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#e74c3c' },
                itemStyle: { color: '#e74c3c' },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(231, 76, 60, 0.2)' },
                        { offset: 1, color: 'rgba(231, 76, 60, 0.02)' }
                    ])
                },
                data: []
            }
        ]
    });
    charts.push(chart);
}

/**
 * 图5：可用节点（折线图 + 面积）
 */
function initChartAvailableNodes() {
    var dom = document.getElementById('chart-nodes');
    if (!dom) return;
    var chart = echarts.init(dom);
    var theme = createDarkTheme();
    chart.setOption({
        backgroundColor: theme.backgroundColor,
        tooltip: theme.tooltip,
        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 },
            data: ['可用节点']
        },
        grid: theme.grid,
        xAxis: theme.xAxis,
        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: { color: '#aaa', fontSize: 11 },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } },
            name: '节点数',
            nameTextStyle: { color: '#888', fontSize: 11 }
        },
        series: [
            {
                name: '可用节点',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#2ecc71' },
                itemStyle: { color: '#2ecc71' },
                areaStyle: {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                        { offset: 0, color: 'rgba(46, 204, 113, 0.3)' },
                        { offset: 1, color: 'rgba(46, 204, 113, 0.02)' }
                    ])
                },
                data: []
            }
        ]
    });
    charts.push(chart);
}

/**
 * 图6：可用资源（双折线图）
 */
function initChartAvailableResources() {
    var dom = document.getElementById('chart-resource-avail');
    if (!dom) return;
    var chart = echarts.init(dom);
    var theme = createDarkTheme();
    chart.setOption({
        backgroundColor: theme.backgroundColor,
        tooltip: theme.tooltip,
        legend: {
            bottom: 0,
            left: 'center',
            textStyle: { color: '#aaa', fontSize: 11 },
            data: ['可用内存 (GB)', '可用 vCore']
        },
        grid: theme.grid,
        xAxis: theme.xAxis,
        yAxis: {
            type: 'value',
            axisLine: { show: false },
            axisTick: { show: false },
            axisLabel: { color: '#aaa', fontSize: 11 },
            splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } },
            name: '资源量',
            nameTextStyle: { color: '#888', fontSize: 11 }
        },
        series: [
            {
                name: '可用内存 (GB)',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#3498db' },
                itemStyle: { color: '#3498db' },
                data: []
            },
            {
                name: '可用 vCore',
                type: 'line',
                symbol: 'none',
                smooth: true,
                lineStyle: { width: 2, color: '#e67e22' },
                itemStyle: { color: '#e67e22' },
                data: []
            }
        ]
    });
    charts.push(chart);
}

/**
 * 图7：各队列待定容器柱状图（独立变量，不在 charts[] 中）
 * 使用 'dark' 主题，与仪表盘整体风格一致
 */
function initChartQueuePending() {
    var dom = document.getElementById('chart-queue-pending');
    if (!dom) return;
    chartQueuePending = echarts.init(dom, 'dark');
    chartQueuePending.setOption({
        title: { text: '各队列待定容器', left: 'center', textStyle: { fontSize: 14, color: '#aaa' } },
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
        xAxis: { type: 'category', data: [], axisLabel: { rotate: 30, color: '#aaa' } },
        yAxis: { type: 'value', minInterval: 1, name: '待定容器数', nameTextStyle: { color: '#888' }, axisLabel: { color: '#aaa' }, splitLine: { lineStyle: { color: '#2a2a4a', type: 'dashed' } } },
        series: [{ type: 'bar', data: [], itemStyle: { color: '#e6a23c' } }]
    });

    // 统一处理 resize
    window.addEventListener('resize', function () {
        if (chartQueuePending) chartQueuePending.resize();
        charts.forEach(function (chart) {
            if (chart) chart.resize();
        });
    });
}

// ============================================
// 2. 更新 KPI 卡片
// ============================================
function updateKPICards(data) {
    var el;
    el = document.querySelector('#kpi-nodes .kpi-value');
    if (el) el.textContent = data.totalNodes != null ? data.totalNodes : '--';

    el = document.querySelector('#kpi-memory .kpi-value');
    if (el) {
        el.textContent = data.totalMemoryMB != null
            ? (data.totalMemoryMB / 1024).toFixed(1)
            : '--';
    }

    el = document.querySelector('#kpi-vcore .kpi-value');
    if (el) el.textContent = data.totalVCores != null ? data.totalVCores : '--';

    el = document.querySelector('#kpi-container .kpi-value');
    if (el) el.textContent = data.activeContainers != null ? data.activeContainers : '--';

    el = document.querySelector('#kpi-app .kpi-value');
    if (el) el.textContent = data.activeApplications != null ? data.activeApplications : '--';

    el = document.querySelector('#kpi-pending .kpi-value');
    if (el) el.textContent = data.pendingContainers != null ? data.pendingContainers : '--';

    el = document.querySelector('#kpi-reserved .kpi-value');
    if (el) el.textContent = data.reservedContainers != null ? data.reservedContainers : '--';

    el = document.querySelector('#kpi-submitted .kpi-value');
    if (el) el.textContent = data.submittedApplications != null ? data.submittedApplications : '--';

    // 节点健康详情
    var healthEl = document.querySelector('.node-health-detail');
    if (healthEl) {
        var parts = [];
        if (data.lostNodes > 0) parts.push('丢失: ' + data.lostNodes);
        if (data.unhealthyNodes > 0) parts.push('不健康: ' + data.unhealthyNodes);
        if (data.decommissionedNodes > 0) parts.push('退役: ' + data.decommissionedNodes);
        if (data.activeApplications != null && data.totalNodes != null) {
            healthEl.textContent = '活跃: ' + (data.totalNodes - (data.lostNodes||0) - (data.unhealthyNodes||0) - (data.decommissionedNodes||0)) 
                + ' / ' + data.totalNodes 
                + (parts.length > 0 ? ' | ' + parts.join(', ') : '');
        }
    }
}

// ============================================
// 3. 自动刷新机制
// ============================================

/** 启动自动刷新 */
function startAutoRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    if (countdownTimer) clearInterval(countdownTimer);

    isPaused = false;
    countdown = REFRESH_INTERVAL / 1000;
    updateRefreshUI();

    refreshTimer = setInterval(function () {
        refreshCurrent();
        refreshHistory();
    }, REFRESH_INTERVAL);

    countdownTimer = setInterval(function () {
        countdown--;
        if (countdown <= 0) {
            countdown = REFRESH_INTERVAL / 1000;
        }
        var label = document.getElementById('refreshLabel');
        if (label) label.textContent = '自动刷新 ' + countdown + 's';
    }, COUNTDOWN_STEP);
}

/** 停止自动刷新 */
function stopAutoRefresh() {
    if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
    }
    if (countdownTimer) {
        clearInterval(countdownTimer);
        countdownTimer = null;
    }
    isPaused = true;
    updateRefreshUI();
}

/** 切换暂停/继续 */
function toggleRefresh() {
    if (isPaused) {
        startAutoRefresh();
    } else {
        stopAutoRefresh();
    }
}

/** 更新刷新控件的 UI 状态 */
function updateRefreshUI() {
    var indicator = document.getElementById('refreshIndicator');
    var toggleBtn = document.getElementById('refreshToggle');
    var label = document.getElementById('refreshLabel');

    if (isPaused) {
        if (indicator) indicator.classList.add('paused');
        if (toggleBtn) toggleBtn.textContent = '继续';
        if (label) label.textContent = '已暂停';
    } else {
        if (indicator) indicator.classList.remove('paused');
        if (toggleBtn) toggleBtn.textContent = '暂停';
        if (label) label.textContent = '自动刷新 ' + countdown + 's';
    }
}

// ============================================
// 4. 时间范围选择
// ============================================
function bindRangeButtons() {
    var btns = document.querySelectorAll('.range-btn');
    for (var i = 0; i < btns.length; i++) {
        btns[i].addEventListener('click', function () {
            var active = document.querySelectorAll('.range-btn');
            for (var j = 0; j < active.length; j++) {
                active[j].classList.remove('active');
            }
            this.classList.add('active');
            currentRange = this.getAttribute('data-range');
            // 重置倒计时，让用户感知刷新
            countdown = REFRESH_INTERVAL / 1000;
            refreshHistory();
        });
    }
}

// ============================================
// 5. 数据拉取函数（4 个 API 端点）
// ============================================

/**
 * GET /api/metrics/current
 * 返回当前指标快照
 */
async function fetchCurrent() {
    var response = await fetch('/api/metrics/current');
    if (!response.ok) {
        throw new Error('请求失败: ' + response.status + ' ' + response.statusText);
    }
    return await response.json();
}

/**
 * GET /api/metrics/history?range={range}
 * 返回时序历史数据
 */
async function fetchHistory(range) {
    var response = await fetch('/api/metrics/history?range=' + encodeURIComponent(range));
    if (!response.ok) {
        throw new Error('请求失败: ' + response.status + ' ' + response.statusText);
    }
    return await response.json();
}

/**
 * GET /api/metrics/nodes
 * 返回节点详情
 */
async function fetchNodes() {
    var response = await fetch('/api/metrics/nodes');
    if (!response.ok) {
        throw new Error('请求失败: ' + response.status + ' ' + response.statusText);
    }
    return await response.json();
}

/**
 * GET /api/metrics/queue
 * 返回队列状态
 */
async function fetchQueue() {
    var response = await fetch('/api/metrics/queue');
    if (!response.ok) {
        throw new Error('请求失败: ' + response.status + ' ' + response.statusText);
    }
    return await response.json();
}

// ============================================
// 6. 图表数据更新函数
// ============================================

/**
 * 将 timestamps 和 values 数组转为 ECharts 时间序列
 * @param {number[]} timestamps - 时间戳数组（毫秒）
 * @param {number[]} values - 对应数值数组
 * @returns {Array<[number, number]>} ECharts 系列数据
 */
function buildTimeSeries(timestamps, values) {
    if (!timestamps || !values || timestamps.length === 0) return [];
    var result = [];
    for (var i = 0; i < timestamps.length; i++) {
        var v = values[i];
        result.push([timestamps[i], v != null ? v : 0]);
    }
    return result;
}

/**
 * 更新图1：容器调度趋势
 */
function updateChartContainer(data) {
    var chart = charts[0];
    if (!chart) return;
    chart.hideLoading();

    if (!data.timestamps || data.timestamps.length === 0) {
        chart.setOption({
            series: [
                { data: [] },
                { data: [] },
                { data: [] }
            ]
        });
        return;
    }

    chart.setOption({
        series: [
            { data: buildTimeSeries(data.timestamps, data.containerAllocated) },
            { data: buildTimeSeries(data.timestamps, data.containerReleased) },
            { data: buildTimeSeries(data.timestamps, data.activeContainers) }
        ]
    });
}

/**
 * 更新图2：资源利用率（堆叠面积图）
 * 优先使用独立的 memoryUtilizationPercent / vcoreUtilizationPercent，
 * 若不存在则使用 clusterUtilizationPercent 作为整体利用率展示。
 */
function updateChartResource(data) {
    var chart = charts[1];
    if (!chart) return;
    chart.hideLoading();

    if (!data.timestamps || data.timestamps.length === 0) {
        chart.setOption({ series: [{ data: [] }, { data: [] }] });
        return;
    }

    var memoryData, vcoreData;

    if (data.memoryUtilizationPercent && data.vcoreUtilizationPercent) {
        // 后端返回了独立的 memory/vcore 时序数据
        memoryData = buildTimeSeries(data.timestamps, data.memoryUtilizationPercent);
        vcoreData = buildTimeSeries(data.timestamps, data.vcoreUtilizationPercent);
    } else if (data.clusterUtilizationPercent) {
        // 仅有整体利用率时，用整体数据做内存利用率，vCore 显示为零
        memoryData = buildTimeSeries(data.timestamps, data.clusterUtilizationPercent);
        vcoreData = data.timestamps.map(function (t) { return [t, 0]; });
    } else {
        memoryData = [];
        vcoreData = [];
    }

    chart.setOption({
        series: [
            { data: memoryData },
            { data: vcoreData }
        ]
    });
}

/**
 * 更新图3：应用状态（柱状图）
 */
function updateChartApplication(data) {
    var chart = charts[2];
    if (!chart) return;
    chart.hideLoading();

    if (!data.timestamps || data.timestamps.length === 0) {
        chart.setOption({
            series: [
                { data: [] },
                { data: [] },
                { data: [] }
            ]
        });
        return;
    }

    chart.setOption({
        series: [
            { data: buildTimeSeries(data.timestamps, data.activeApplications) },
            { data: buildTimeSeries(data.timestamps, data.completedApplications) },
            { data: buildTimeSeries(data.timestamps, data.failedApplications) }
        ]
    });
}

/**
 * 更新图4：心跳延迟（折线图）
 */
function updateChartHeartbeat(data) {
    var chart = charts[3];
    if (!chart) return;
    chart.hideLoading();

    if (!data.timestamps || data.timestamps.length === 0) {
        chart.setOption({ series: [{ data: [] }, { data: [] }] });
        return;
    }

    chart.setOption({
        series: [
            { data: buildTimeSeries(data.timestamps, data.avgHeartbeatLatencyMs) },
            { data: buildTimeSeries(data.timestamps, data.maxHeartbeatLatencyMs) }
        ]
    });
}

/**
 * 更新图5：可用节点
 */
function updateChartAvailableNodes(data) {
    var chart = charts[4];
    if (!chart) return;
    chart.hideLoading();

    if (!data.timestamps || data.timestamps.length === 0 || !data.availableNodes) {
        chart.setOption({ series: [{ data: [] }] });
        return;
    }

    chart.setOption({
        series: [
            { data: buildTimeSeries(data.timestamps, data.availableNodes) }
        ]
    });
}

/**
 * 更新图6：可用资源
 */
function updateChartAvailableResources(data) {
    var chart = charts[5];
    if (!chart) return;
    chart.hideLoading();

    if (!data.timestamps || data.timestamps.length === 0) {
        chart.setOption({ series: [{ data: [] }, { data: [] }] });
        return;
    }

    var memoryGB = data.availableMemoryMB
        ? data.availableMemoryMB.map(function (v) { return v != null ? v / 1024 : null; })
        : [];

    chart.setOption({
        series: [
            { data: buildTimeSeries(data.timestamps, memoryGB) },
            { data: buildTimeSeries(data.timestamps, data.availableVCores) }
        ]
    });
}

// ============================================
// 7. 刷新历史数据（更新 4 个图表）
// ============================================
function refreshHistory() {
    fetchHistory(currentRange)
        .then(function (data) {
            updateChartContainer(data);
            updateChartResource(data);
            updateChartApplication(data);
            updateChartHeartbeat(data);
            updateChartAvailableNodes(data);
            updateChartAvailableResources(data);
        })
        .catch(function (error) {
            console.error('获取历史数据失败:', error);
            // 图表显示错误状态
            charts.forEach(function (chart) {
                if (!chart) return;
                chart.hideLoading();
                chart.setOption({
                    title: {
                        text: '数据加载失败',
                        left: 'center',
                        top: 'center',
                        textStyle: { color: '#e74c3c', fontSize: 14, fontWeight: 'normal' }
                    }
                });
            });
        });
}

// ============================================
// 8. 刷新当前数据（KPI 卡片 + 队列表格）
// ============================================
function refreshCurrent() {
    fetchCurrent()
        .then(function (data) {
            updateKPICards(data);
        })
        .catch(function (error) {
            console.error('获取当前指标失败:', error);
        });

    fetchQueue()
        .then(function (data) {
            updateQueueTable(data);
            updateChartQueuePending(data);
        })
        .catch(function (error) {
            console.error('获取队列数据失败:', error);
        });
}

// ============================================
// 9. 更新队列状态表格
// ============================================
function updateQueueTable(data) {
    var tbody = document.getElementById('queueTableBody');
    if (!tbody) return;

    if (!data.queues || data.queues.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="no-data">等待数据...</td></tr>';
        return;
    }

    var html = '';
    for (var i = 0; i < data.queues.length; i++) {
        var q = data.queues[i];
        html += '<tr>'
            + '<td>' + escapeHtml(q.queueName) + '</td>'
            + '<td>' + (q.absoluteCapacity != null ? q.absoluteCapacity.toFixed(1) : '-') + '</td>'
            + '<td>' + (q.usedCapacity != null ? q.usedCapacity.toFixed(1) : '-') + '</td>'
            + '<td>' + (q.pendingApps != null ? q.pendingApps : '-') + '</td>'
            + '<td>' + (q.activeApps != null ? q.activeApps : '-') + '</td>'
            + '</tr>';
    }
    tbody.innerHTML = html;
}

/**
 * 更新图7：各队列待定容器柱状图
 * 从 fetchQueue 返回的数据中提取所有队列名和 pendingContainers
 */
function updateChartQueuePending(data) {
    if (!chartQueuePending) return;
    if (!data || !data.queues || data.queues.length === 0) {
        chartQueuePending.setOption({
            xAxis: { data: [] },
            series: [{ data: [] }]
        });
        return;
    }

    var names = [];
    var values = [];
    for (var i = 0; i < data.queues.length; i++) {
        var q = data.queues[i];
        names.push(q.queueName);
        values.push(q.pendingContainers != null ? q.pendingContainers : 0);
    }

    chartQueuePending.setOption({
        xAxis: { data: names },
        series: [{ data: values }]
    });
}

/**
 * HTML 转义，防止 XSS 注入
 * @param {*} str - 输入值
 * @returns {string} 转义后的 HTML 安全字符串
 */
function escapeHtml(str) {
    if (str == null) return '';
    if (typeof str !== 'string') str = String(str);
    var div = document.createElement('div');
    div.appendChild(document.createTextNode(str));
    return div.innerHTML;
}

// ============================================
// 10. 绑定交互事件
// ============================================
function bindEvents() {
    // 时间范围按钮
    bindRangeButtons();

    // 刷新控制
    var toggleBtn = document.getElementById('refreshToggle');
    if (toggleBtn) {
        toggleBtn.addEventListener('click', toggleRefresh);
    }

    var nowBtn = document.getElementById('refreshNow');
    if (nowBtn) {
        nowBtn.addEventListener('click', function () {
            countdown = REFRESH_INTERVAL / 1000;
            if (!isPaused) {
                var label = document.getElementById('refreshLabel');
                if (label) label.textContent = '自动刷新 ' + countdown + 's';
            }
            refreshCurrent();
            refreshHistory();
        });
    }

    // 窗口自适应：所有图表跟随容器尺寸变化
    window.addEventListener('resize', function () {
        for (var i = 0; i < charts.length; i++) {
            if (charts[i]) charts[i].resize();
        }
    });
}

// ============================================
// 11. 页面初始化
// ============================================
document.addEventListener('DOMContentLoaded', function () {
    initCharts();       // 初始化 6 个 time-series ECharts 图表
    initChartQueuePending(); // 初始化队列待定容器柱状图
    refreshCurrent();   // 首次加载 KPI 卡片 + 队列表格
    refreshHistory();   // 首次加载图表历史数据
    startAutoRefresh(); // 启动自动刷新
    bindEvents();       // 绑定按钮事件
});
