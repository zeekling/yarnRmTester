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
    private final ObjectMapper objectMapper;

    public MetricsHttpHandler(Map<String, HeartbeatResponseCollector> heartbeatCollectors,
                             ResourceManagerMetricsCollector rmMetricsCollector) {
        this.heartbeatCollectors = heartbeatCollectors;
        this.rmMetricsCollector = rmMetricsCollector;
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

        long totalHeartbeats = 0;
        long totalContainersAllocated = 0;
        long totalContainersReleased = 0;
        int totalNodes = heartbeatCollectors.size();

        for (HeartbeatResponseCollector collector : heartbeatCollectors.values()) {
            MetricsData data = collector.getMetricsData();
            totalHeartbeats += data.getTotalHeartbeats();
            totalContainersAllocated += data.getTotalContainersAllocated();
            totalContainersReleased += data.getTotalContainersReleased();
        }

        metricsMap.put("cluster", Map.of(
            "totalNodes", totalNodes,
            "totalHeartbeats", totalHeartbeats
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