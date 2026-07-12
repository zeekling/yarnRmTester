package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 定时采集器。
 * 按配置间隔轮询 MetricsServer /metrics，将结果转换为 MetricsSnapshot 并存储。
 */
public class MetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

    private final String metricsServerUrl;
    private final long collectIntervalMs;
    private final MetricsStore store;
    private final MetricsDatabase database;
    private final boolean nmEnabled;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private volatile boolean running = false;
    private ScheduledFuture<?> collectFuture;

    /**
     * @param metricsServerUrl  MetricsServer 的 base URL（如 http://localhost:28080）
     * @param collectIntervalMs 采集间隔（毫秒）
     * @param store             内存环形缓冲区
     * @param database          SQLite 持久化层
     */
    public MetricsCollector(String metricsServerUrl, long collectIntervalMs,
                            MetricsStore store, MetricsDatabase database) {
        this(metricsServerUrl, collectIntervalMs, store, database, true);
    }

    /**
     * @param metricsServerUrl  MetricsServer 的 base URL（如 http://localhost:28080）
     * @param collectIntervalMs 采集间隔（毫秒）
     * @param store             内存环形缓冲区
     * @param database          SQLite 持久化层
     * @param nmEnabled         是否启用 NM 指标采集（false 时 start/collectOnce 直接返回）
     */
    public MetricsCollector(String metricsServerUrl, long collectIntervalMs,
                            MetricsStore store, MetricsDatabase database, boolean nmEnabled) {
        this.metricsServerUrl = metricsServerUrl;
        this.collectIntervalMs = collectIntervalMs;
        this.store = store;
        this.database = database;
        this.nmEnabled = nmEnabled;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-collector");
            t.setDaemon(true);
            return t;
        });
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 启动定时采集。
     */
    public void start() {
        if (!nmEnabled) {
            LOG.info("NM metrics collection disabled, MetricsCollector will not schedule");
            return;
        }
        if (running) {
            LOG.warn("MetricsCollector is already running");
            return;
        }
        running = true;
        collectFuture = scheduler.scheduleAtFixedRate(() -> {
            try {
                MetricsSnapshot snapshot = collectOnce();
                if (snapshot != null) {
                    store.add(snapshot);
                    if (database != null) {
                        database.insertBatch(java.util.Collections.singletonList(snapshot));
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error during scheduled collection", e);
            }
        }, 0, collectIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info("MetricsCollector started: interval={}ms, target={}", collectIntervalMs, metricsServerUrl);
    }

    /**
     * 停止采集。
     */
    public void stop() {
        running = false;
        if (collectFuture != null && !collectFuture.isCancelled()) {
            collectFuture.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOG.info("MetricsCollector stopped");
    }

    /**
     * 是否正在运行。
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * 单次采集（public 便于测试）。
     * HTTP GET 请求 {metricsServerUrl}/metrics，解析 JSON 构建 MetricsSnapshot。
     *
     * @return 采集到的快照，或 null（失败时）
     */
    public MetricsSnapshot collectOnce() {
        if (!nmEnabled) {
            LOG.debug("NM metrics collection disabled, collectOnce returns null");
            return null;
        }
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(metricsServerUrl + "/metrics"))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                LOG.warn("MetricsServer returned status {} for /metrics", response.statusCode());
                return null;
            }

            JsonNode root = objectMapper.readTree(response.body());
            return parseMetricsResponse(root);
        } catch (Exception e) {
            LOG.warn("Failed to collect metrics from {}: {}", metricsServerUrl + "/metrics", e.getMessage());
            return null;
        }
    }

    /**
     * 解析 MetricsServer /metrics 的 JSON 响应为 MetricsSnapshot。
     *
     * ⚠️ 已知限制：MetricsServer 当前仅输出 cluster/scheduling/applications/nodeHeartbeatMetrics
     * 四个节点。以下字段 MetricsServer 不提供、永远为 0：
     * - 集群资源（totalMemoryMB/totalVCores 等）
     * - 队列调度数据（queueMetrics）
     * - pendingContainers / reservedContainers / submittedApplications
     * 后续需通过 YarnClient RPC（ResourceManagerMetricsCollector）补充第二数据源。
     */
    MetricsSnapshot parseMetricsResponse(JsonNode root) {
        MetricsSnapshot snapshot = new MetricsSnapshot();

        // 时间戳
        JsonNode timestampNode = root.get("timestamp");
        if (timestampNode != null) {
            snapshot.setTimestamp(timestampNode.asLong());
        }

        // cluster
        JsonNode cluster = root.get("cluster");
        if (cluster != null) {
            JsonNode totalNodes = cluster.get("totalNodes");
            if (totalNodes != null) {
                snapshot.setTotalNodes(totalNodes.asInt());
            }
        }

        // scheduling
        JsonNode scheduling = root.get("scheduling");
        if (scheduling != null) {
            JsonNode allocated = scheduling.get("totalContainersAllocated");
            if (allocated != null) {
                snapshot.setTotalContainersAllocated(allocated.asLong());
            }
            JsonNode released = scheduling.get("totalContainersReleased");
            if (released != null) {
                snapshot.setTotalContainersReleased(released.asLong());
            }
        }

        // 派生字段：activeContainers
        snapshot.setActiveContainers(
                snapshot.getTotalContainersAllocated() - snapshot.getTotalContainersReleased());

        // applications
        JsonNode applications = root.get("applications");
        if (applications != null) {
            JsonNode active = applications.get("active");
            if (active != null) {
                snapshot.setActiveApplications(active.asInt());
            }
            JsonNode completed = applications.get("completed");
            if (completed != null) {
                snapshot.setCompletedApplications(completed.asInt());
            }
            JsonNode failed = applications.get("failed");
            if (failed != null) {
                snapshot.setFailedApplications(failed.asInt());
            }
        }

        // nodeHeartbeatMetrics — 汇总计算心跳指标
        JsonNode nodeHeartbeatMetrics = root.get("nodeHeartbeatMetrics");
        if (nodeHeartbeatMetrics != null && nodeHeartbeatMetrics.isObject()) {
            parseHeartbeatMetrics(snapshot, nodeHeartbeatMetrics);
        }

        return snapshot;
    }

    /**
     * 解析节点心跳指标，汇总计算全局心跳数据。
     */
    private void parseHeartbeatMetrics(MetricsSnapshot snapshot, JsonNode nodeHeartbeatMetrics) {
        long totalSuccessful = 0;
        long totalFailed = 0;
        double sumAvgLatency = 0;
        double maxLatency = 0;
        int nodeCount = 0;
        Map<String, MetricsSnapshot.NodeMetrics> nodeMetricsMap = new LinkedHashMap<>();

        Iterator<Map.Entry<String, JsonNode>> fields = nodeHeartbeatMetrics.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String nodeId = entry.getKey();
            JsonNode nodeData = entry.getValue();

            long successful = nodeData.has("successfulHeartbeats") ? nodeData.get("successfulHeartbeats").asLong() : 0;
            long failed = nodeData.has("failedHeartbeats") ? nodeData.get("failedHeartbeats").asLong() : 0;
            long totalHb = nodeData.has("totalHeartbeats") ? nodeData.get("totalHeartbeats").asLong() : (successful + failed);
            double avgLatency = nodeData.has("avgHeartbeatDuration") ? nodeData.get("avgHeartbeatDuration").asDouble() : 0;
            double maxHbLatency = nodeData.has("maxHeartbeatDuration") ? nodeData.get("maxHeartbeatDuration").asDouble() : 0;

            totalSuccessful += successful;
            totalFailed += failed;
            sumAvgLatency += avgLatency;
            if (maxHbLatency > maxLatency) {
                maxLatency = maxHbLatency;
            }
            nodeCount++;

            // 构建节点指标
            MetricsSnapshot.NodeMetrics nm = new MetricsSnapshot.NodeMetrics();
            nm.setNodeId(nodeId);
            nm.setTotalHeartbeats(totalHb);
            nm.setSuccessfulHeartbeats(successful);
            nm.setFailedHeartbeats(failed);
            nm.setAvgLatencyMs(avgLatency);
            nm.setMaxLatencyMs(maxHbLatency);
            nodeMetricsMap.put(nodeId, nm);
        }

        snapshot.setSuccessfulHeartbeats(totalSuccessful);
        snapshot.setFailedHeartbeats(totalFailed);

        long totalHeartbeats = totalSuccessful + totalFailed;
        snapshot.setHeartbeatSuccessRate(
                totalHeartbeats > 0 ? (double) totalSuccessful / totalHeartbeats * 100.0 : 100.0);

        snapshot.setAvgHeartbeatLatencyMs(nodeCount > 0 ? sumAvgLatency / nodeCount : 0);
        snapshot.setMaxHeartbeatLatencyMs(maxLatency);

        // 吞吐量：总心跳数 / 采集间隔（假设为 5s），近似
        snapshot.setHeartbeatThroughput(totalHeartbeats > 0 ? (double) totalHeartbeats / (collectIntervalMs / 1000.0) : 0);

        snapshot.setNodeMetrics(nodeMetricsMap);
    }
}
