package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MetricsHttpHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsHttpHandler.class);

    private final Map<String, HeartbeatResponseCollector> heartbeatCollectors;
    private final ResourceManagerMetricsCollector rmMetricsCollector;
    private final MetricsServer metricsServer;
    private final ObjectMapper objectMapper;

    public MetricsHttpHandler(Map<String, HeartbeatResponseCollector> heartbeatCollectors,
                             ResourceManagerMetricsCollector rmMetricsCollector,
                             MetricsServer metricsServer) {
        this.heartbeatCollectors = heartbeatCollectors;
        this.rmMetricsCollector = rmMetricsCollector;
        this.metricsServer = metricsServer;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        URI requestURI = exchange.getRequestURI();
        String path = requestURI.getPath();

        if ("/metrics".equals(path)) {
            handleMetrics(exchange);
        } else {
            handleNotFound(exchange);
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        // 收集最新的RM指标
        rmMetricsCollector.collectMetrics();
        
        Map<String, Object> metricsMap = new HashMap<>();

        long totalContainersAllocated = 0;
        long totalContainersReleased = 0;
        int totalNodes = heartbeatCollectors.size();

        for (HeartbeatResponseCollector collector : heartbeatCollectors.values()) {
            MetricsData data = collector.getMetricsData();
            totalContainersAllocated += data.getTotalContainersAllocated();
            totalContainersReleased += data.getTotalContainersReleased();
        }

        metricsMap.put("cluster", Map.of(
            "totalNodes", totalNodes
        ));
        metricsMap.put("scheduling", Map.of(
            "totalContainersAllocated", totalContainersAllocated,
            "totalContainersReleased", totalContainersReleased
        ));
        metricsMap.put("applications", Map.of(
            "active", rmMetricsCollector.getActiveApplications(),
            "completed", rmMetricsCollector.getCompletedApplications(),
            "failed", rmMetricsCollector.getFailedApplications()
        ));
        metricsMap.put("timestamp", System.currentTimeMillis());
        metricsMap.put("nodeHeartbeatMetrics", getNodeHeartbeatMetrics());

        String jsonResponse;
        try {
            jsonResponse = objectMapper.writeValueAsString(metricsMap);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize metrics to JSON", e);
            sendError(exchange, 500, "Internal server error");
            return;
        }

        sendResponse(exchange, jsonResponse, 200, "application/json");
    }

    /**
     * 获取节点心跳统计信息的映射表
     *
     * @return 包含所有节点心跳统计信息的映射表，键为节点ID，值为NodeHeartbeatStats对象
     */
    public Map<String, NodeHeartbeatStats> getNodeHeartbeatStatsMap() {
        return metricsServer.getNodeHeartbeatStatsMap();
    }

    /**
     * 获取所有节点的详细心跳指标
     *
     * @return 包含所有节点心跳指标的映射表，键为节点ID，值为该节点的心跳指标
     *         指标包括：totalHeartbeats（总心跳次数）、successfulHeartbeats（成功心跳次数），
     *         failedHeartbeats（失败心跳次数）、heartbeatCount（记录的心跳次数）、
     *         totalHeartbeatDuration（总心跳持续时间）、maxHeartbeatDuration（最大心跳持续时间）
     *         和 avgHeartbeatDuration（平均心跳持续时间）
     */
    public Map<String, Map<String, Object>> getNodeHeartbeatMetrics() {
        Map<String, Map<String, Object>> nodeMetrics = new HashMap<>();
        for (Map.Entry<String, HeartbeatResponseCollector> entry : heartbeatCollectors.entrySet()) {
            String nodeId = entry.getKey();
            HeartbeatResponseCollector collector = entry.getValue();
            MetricsData metricsData = collector.getMetricsData();

            Map<String, Object> nodeData = new HashMap<>();
            nodeData.put("totalHeartbeats", metricsData.getSuccessfulHeartbeats() + metricsData.getFailedHeartbeats());
            nodeData.put("successfulHeartbeats", metricsData.getSuccessfulHeartbeats());
            nodeData.put("failedHeartbeats", metricsData.getFailedHeartbeats());

            Map<String, NodeHeartbeatStats> nodeStatsMap = getNodeHeartbeatStatsMap();
            if (nodeStatsMap.containsKey(nodeId)) {
                NodeHeartbeatStats stats = nodeStatsMap.get(nodeId);
                nodeData.put("heartbeatCount", stats.getHeartbeatCount());
                nodeData.put("totalHeartbeatDuration", stats.getTotalHeartbeatDuration());
                nodeData.put("maxHeartbeatDuration", stats.getMaxHeartbeatDuration());
                nodeData.put("avgHeartbeatDuration", stats.getAverageHeartbeatDuration());
            }

            nodeMetrics.put(nodeId, nodeData);
        }
        return nodeMetrics;
    }

    private void handleNotFound(HttpExchange exchange) throws IOException {
        sendError(exchange, 404, "Not found");
    }

    private void sendResponse(HttpExchange exchange, String responseBody, int statusCode, String contentType) throws IOException {
        byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        Map<String, String> error = new HashMap<>();
        error.put("error", message);
        String jsonResponse = objectMapper.writeValueAsString(error);
        sendResponse(exchange, jsonResponse, statusCode, "application/json");
    }
}