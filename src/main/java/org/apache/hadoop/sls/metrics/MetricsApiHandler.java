package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * REST API 处理器。
 * 提供 /api/metrics/* 的 JSON API + 静态文件服务。
 */
public class MetricsApiHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsApiHandler.class);

    private static final int STORE_FALLBACK_THRESHOLD = 2;

    private final MetricsStore store;
    private final MetricsDatabase database;
    private final ObjectMapper objectMapper;

    public MetricsApiHandler(MetricsStore store, MetricsDatabase database) {
        this.store = store;
        this.database = database;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();

            if (!"GET".equalsIgnoreCase(method)) {
                sendJson(exchange, Map.of("error", "Method not allowed"), 405);
                return;
            }

            // 路由分发
            if ("/api/metrics/current".equals(path)) {
                handleCurrent(exchange);
            } else if ("/api/metrics/history".equals(path)) {
                handleHistory(exchange);
            } else if ("/api/metrics/nodes".equals(path)) {
                handleNodes(exchange);
            } else if ("/api/metrics/queue".equals(path)) {
                handleQueue(exchange);
            } else if (path.equals("/") || path.equals("/index.html")) {
                serveStatic(exchange, "/frontend/index.html", "text/html; charset=utf-8");
            } else if (path.startsWith("/css/") || path.startsWith("/js/")) {
                String resourcePath = "/frontend" + path;
                // 路径规范化检查，防止路径遍历
                String normalized = new URI(resourcePath).normalize().getPath();
                if (!normalized.startsWith("/frontend/")) {
                    sendJson(exchange, Map.of("error", "Invalid path"), 400);
                    return;
                }
                String contentType = path.endsWith(".css") ? "text/css; charset=utf-8"
                        : "text/javascript; charset=utf-8";
                serveStatic(exchange, normalized, contentType);
            } else {
                sendJson(exchange, Map.of("error", "Not found"), 404);
            }
        } catch (Exception e) {
            LOG.error("Error handling API request: {}", exchange.getRequestURI(), e);
            sendJson(exchange, Map.of("error", "Internal server error: " + e.getMessage()), 500);
        }
    }

    /**
     * 处理 /api/metrics/current：返回最新快照。
     */
    private void handleCurrent(HttpExchange exchange) throws IOException {
        MetricsSnapshot latest = store.getLatest();
        if (latest == null) {
            sendJson(exchange, Map.of("error", "No data collected yet"), 503);
            return;
        }
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("timestamp", latest.getTimestamp());
        response.put("totalNodes", latest.getTotalNodes());
        response.put("lostNodes", latest.getLostNodes());
        response.put("unhealthyNodes", latest.getUnhealthyNodes());
        response.put("decommissionedNodes", latest.getDecommissionedNodes());
        response.put("totalMemoryMB", latest.getTotalMemoryMB());
        response.put("totalVCores", latest.getTotalVCores());
        response.put("allocatedMemoryMB", latest.getAllocatedMemoryMB());
        response.put("allocatedVCores", latest.getAllocatedVCores());
        response.put("clusterUtilizationPercent", latest.getClusterUtilizationPercent());
        response.put("totalContainersAllocated", latest.getTotalContainersAllocated());
        response.put("totalContainersReleased", latest.getTotalContainersReleased());
        response.put("activeContainers", latest.getActiveContainers());
        response.put("pendingContainers", latest.getPendingContainers());
        response.put("reservedContainers", latest.getReservedContainers());
        response.put("activeApplications", latest.getActiveApplications());
        response.put("completedApplications", latest.getCompletedApplications());
        response.put("failedApplications", latest.getFailedApplications());
        response.put("submittedApplications", latest.getSubmittedApplications());
        response.put("heartbeatSuccessRate", latest.getHeartbeatSuccessRate());
        response.put("avgHeartbeatLatencyMs", latest.getAvgHeartbeatLatencyMs());
        response.put("maxHeartbeatLatencyMs", latest.getMaxHeartbeatLatencyMs());
        sendJson(exchange, response, 200);
    }

    /**
     * 处理 /api/metrics/history：返回历史时序数据（ECharts 友好格式）。
     */
    private void handleHistory(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        String range = "1h"; // 默认
        if (query != null) {
            for (String param : query.split("&")) {
                String[] kv = param.split("=");
                if (kv.length == 2 && "range".equals(kv[0])) {
                    range = kv[1];
                    break;
                }
            }
        }

        long endTime = System.currentTimeMillis();
        long startTime = endTime - parseRange(range);

        // 优先从 MetricsStore（环形缓冲区）查询
        List<MetricsSnapshot> snapshots = store.queryByTimeRange(startTime, endTime);

        // 如果 store 数据不足，从 database 补充
        if (snapshots.size() < STORE_FALLBACK_THRESHOLD && database != null) {
            snapshots = database.queryByTimeRange(startTime, endTime);
        }

        // 构建前端 ECharts 友好的格式
        List<Long> timestamps = new ArrayList<>();
        List<Long> containerAllocated = new ArrayList<>();
        List<Long> containerReleased = new ArrayList<>();
        List<Long> activeContainers = new ArrayList<>();
        List<Integer> pendingContainers = new ArrayList<>();
        List<Integer> reservedContainers = new ArrayList<>();
        List<Integer> activeApps = new ArrayList<>();
        List<Integer> completedApps = new ArrayList<>();
        List<Integer> failedApps = new ArrayList<>();
        List<Double> heartbeatLatency = new ArrayList<>();
        List<Double> clusterUtil = new ArrayList<>();
        List<Integer> availableNodes = new ArrayList<>();
        List<Long> availableMemoryMB = new ArrayList<>();
        List<Integer> availableVCores = new ArrayList<>();

        for (MetricsSnapshot snap : snapshots) {
            timestamps.add(snap.getTimestamp());
            containerAllocated.add(snap.getTotalContainersAllocated());
            containerReleased.add(snap.getTotalContainersReleased());
            activeContainers.add(snap.getActiveContainers());
            pendingContainers.add(snap.getPendingContainers());
            reservedContainers.add(snap.getReservedContainers());
            activeApps.add(snap.getActiveApplications());
            completedApps.add(snap.getCompletedApplications());
            failedApps.add(snap.getFailedApplications());
            heartbeatLatency.add(snap.getAvgHeartbeatLatencyMs());
            clusterUtil.add(snap.getClusterUtilizationPercent());
            availableNodes.add(snap.getTotalNodes() - snap.getLostNodes() - snap.getUnhealthyNodes() - snap.getDecommissionedNodes());
            availableMemoryMB.add(snap.getAvailableMemoryMB());
            availableVCores.add(snap.getAvailableVCores());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("timestamps", timestamps);
        response.put("containerAllocated", containerAllocated);
        response.put("containerReleased", containerReleased);
        response.put("activeContainers", activeContainers);
        response.put("pendingContainers", pendingContainers);
        response.put("reservedContainers", reservedContainers);
        response.put("activeApplications", activeApps);
        response.put("completedApplications", completedApps);
        response.put("failedApplications", failedApps);
        response.put("avgHeartbeatLatencyMs", heartbeatLatency);
        response.put("clusterUtilizationPercent", clusterUtil);
        response.put("availableNodes", availableNodes);
        response.put("availableMemoryMB", availableMemoryMB);
        response.put("availableVCores", availableVCores);

        sendJson(exchange, response, 200);
    }

    /**
     * 处理 /api/metrics/nodes：返回所有节点的心跳统计。
     */
    private void handleNodes(HttpExchange exchange) throws IOException {
        MetricsSnapshot latest = store.getLatest();
        if (latest == null || latest.getNodeMetrics() == null) {
            sendJson(exchange, Map.of("nodes", Collections.emptyList()), 200);
            return;
        }
        List<Map<String, Object>> nodeList = new ArrayList<>();
        for (Map.Entry<String, MetricsSnapshot.NodeMetrics> entry : latest.getNodeMetrics().entrySet()) {
            MetricsSnapshot.NodeMetrics nm = entry.getValue();
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("nodeId", nm.getNodeId());
            node.put("totalHeartbeats", nm.getTotalHeartbeats());
            node.put("successfulHeartbeats", nm.getSuccessfulHeartbeats());
            node.put("failedHeartbeats", nm.getFailedHeartbeats());
            node.put("avgLatencyMs", nm.getAvgLatencyMs());
            node.put("maxLatencyMs", nm.getMaxLatencyMs());
            nodeList.add(node);
        }
        sendJson(exchange, Map.of("nodes", nodeList), 200);
    }

    /**
     * 处理 /api/metrics/queue：返回队列调度状态。
     */
    private void handleQueue(HttpExchange exchange) throws IOException {
        MetricsSnapshot latest = store.getLatest();
        if (latest == null || latest.getQueueMetrics() == null) {
            sendJson(exchange, Map.of("queues", Collections.emptyList()), 200);
            return;
        }
        List<Map<String, Object>> queueList = new ArrayList<>();
        for (Map.Entry<String, MetricsSnapshot.QueueMetrics> entry : latest.getQueueMetrics().entrySet()) {
            MetricsSnapshot.QueueMetrics qm = entry.getValue();
            Map<String, Object> queue = new LinkedHashMap<>();
            queue.put("queueName", qm.getQueueName());
            queue.put("absoluteCapacity", qm.getAbsoluteCapacity());
            queue.put("usedCapacity", qm.getUsedCapacity());
            queue.put("pendingApps", qm.getPendingApps());
            queue.put("activeApps", qm.getActiveApps());
            queue.put("pendingContainers", qm.getPendingContainers());
            queueList.add(queue);
        }
        sendJson(exchange, Map.of("queues", queueList), 200);
    }

    /**
     * 提供静态文件服务。
     */
    private void serveStatic(HttpExchange exchange, String resourcePath, String contentType) throws IOException {
        InputStream is = getClass().getResourceAsStream(resourcePath);
        if (is == null) {
            sendJson(exchange, Map.of("error", "File not found: " + resourcePath), 404);
            return;
        }
        byte[] content = is.readAllBytes();
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, content.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(content);
        }
    }

    /**
     * 解析时间范围字符串为毫秒值。
     */
    static long parseRange(String range) {
        if (range == null || range.isEmpty()) return 3600000L;
        char unit = range.charAt(range.length() - 1);
        long value = Long.parseLong(range.substring(0, range.length() - 1));
        switch (unit) {
            case 'm': return value * 60000L;
            case 'h': return value * 3600000L;
            case 'd': return value * 86400000L;
            default: return 3600000L;
        }
    }

    /**
     * 发送 JSON 响应。
     */
    private void sendJson(HttpExchange exchange, Object data, int statusCode) throws IOException {
        String json = objectMapper.writeValueAsString(data);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
