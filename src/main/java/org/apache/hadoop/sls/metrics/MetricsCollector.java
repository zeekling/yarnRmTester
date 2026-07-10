package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 定期指标采集器。通过 HTTP 请求 MetricsServer 和 YARN RPC 客户端
 * 收集集群指标，计算衍生指标，最后存入 MetricsStore 和 MetricsDatabase。
 */
public class MetricsCollector implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

    private final String metricsServerUrl;
    private final YarnClient yarnClient;
    private final MetricsStore store;
    private final MetricsDatabase database;
    private final long collectIntervalMs;
    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private volatile MetricsSnapshot previousSnapshot;

    /**
     * @param metricsServerUrl  MetricsServer HTTP 地址（如 http://localhost:28080/metrics）
     * @param yarnClient        YARN 客户端
     * @param store             内存存储
     * @param database          SQLite 持久化
     * @param collectIntervalMs 采集间隔（毫秒）
     */
    public MetricsCollector(String metricsServerUrl, YarnClient yarnClient,
                            MetricsStore store, MetricsDatabase database,
                            long collectIntervalMs) {
        this.metricsServerUrl = metricsServerUrl;
        this.yarnClient = yarnClient;
        this.store = store;
        this.database = database;
        this.collectIntervalMs = collectIntervalMs;
        this.objectMapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-collector");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(this::collect,
                0, collectIntervalMs, TimeUnit.MILLISECONDS);

        LOG.info("MetricsCollector started: interval={}ms, serverUrl={}", collectIntervalMs, metricsServerUrl);
    }

    /**
     * 执行一次完整采集流程。
     */
    void collect() {
        try {
            MetricsSnapshot snapshot = new MetricsSnapshot();

            boolean metricsOk = pollMetricsServer(snapshot);
            if (!metricsOk) {
                LOG.warn("Metrics server poll failed, skipping this collection cycle");
                return;
            }
            pollRMClient(snapshot);
            computeDerivedMetrics(snapshot);

            store.add(snapshot);
            database.add(snapshot);

            previousSnapshot = snapshot;

            LOG.debug("Metrics collected: ts={}, nodes={}, allocContainers={}, activeApps={}",
                    snapshot.getTimestamp(), snapshot.getTotalNodes(),
                    snapshot.getTotalContainersAllocated(), snapshot.getActiveApplications());
        } catch (Exception e) {
            LOG.error("Error during metrics collection", e);
        }
    }

    /**
     * 从 MetricsServer 的 HTTP /metrics 接口拉取集群基础指标。
     * @return true 如果采集成功，false 如果失败（跳过后续处理）
     */
    boolean pollMetricsServer(MetricsSnapshot snapshot) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(metricsServerUrl))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                LOG.warn("Metrics server returned status: {}", response.statusCode());
                return false;
            }

            JsonNode root = objectMapper.readTree(response.body());

            // cluster
            JsonNode cluster = root.get("cluster");
            if (cluster != null) {
                if (cluster.has("totalNodes")) {
                    snapshot.setTotalNodes(cluster.get("totalNodes").asInt());
                }
            }

            // scheduling
            JsonNode scheduling = root.get("scheduling");
            if (scheduling != null) {
                if (scheduling.has("totalContainersAllocated")) {
                    snapshot.setTotalContainersAllocated(
                            scheduling.get("totalContainersAllocated").asLong());
                }
                if (scheduling.has("totalContainersReleased")) {
                    snapshot.setTotalContainersReleased(
                            scheduling.get("totalContainersReleased").asLong());
                }
            }

            // nodeHeartbeatMetrics
            JsonNode nodeHeartbeatMetrics = root.get("nodeHeartbeatMetrics");
            if (nodeHeartbeatMetrics != null && nodeHeartbeatMetrics.isObject()) {
                long totalSuccess = 0;
                long totalFailed = 0;
                double avgLatencySum = 0;
                long maxLatency = 0;
                int nodeCount = 0;

                Iterator<String> fieldNames = nodeHeartbeatMetrics.fieldNames();
                while (fieldNames.hasNext()) {
                    String nodeId = fieldNames.next();
                    JsonNode nodeData = nodeHeartbeatMetrics.get(nodeId);
                    if (nodeData == null) continue;

                    if (nodeData.has("successfulHeartbeats")) {
                        totalSuccess += nodeData.get("successfulHeartbeats").asLong();
                    }
                    if (nodeData.has("failedHeartbeats")) {
                        totalFailed += nodeData.get("failedHeartbeats").asLong();
                    }
                    if (nodeData.has("avgHeartbeatDuration")) {
                        avgLatencySum += nodeData.get("avgHeartbeatDuration").asDouble();
                    }
                    if (nodeData.has("maxHeartbeatDuration")) {
                        long nodeMax = nodeData.get("maxHeartbeatDuration").asLong();
                        if (nodeMax > maxLatency) {
                            maxLatency = nodeMax;
                        }
                    }
                    nodeCount++;
                }

                snapshot.setSuccessfulHeartbeats(totalSuccess);
                snapshot.setFailedHeartbeats(totalFailed);
                snapshot.setAvgHeartbeatLatency(nodeCount > 0 ? avgLatencySum / nodeCount : 0);
                snapshot.setMaxHeartbeatLatency(maxLatency);
            }

            LOG.debug("Polled metrics server: nodes={}, containers={}/{}",
                    snapshot.getTotalNodes(),
                    snapshot.getTotalContainersAllocated(),
                    snapshot.getTotalContainersReleased());

            return true;

        } catch (IOException e) {
            String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            LOG.warn("Failed to connect to metrics server at {}: {} (hint: verify hostname, check if MetricsServer on port 28080 is running)", metricsServerUrl, detail);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Metrics server poll interrupted");
            return false;
        }
    }

    /**
     * 通过 YARN RPC 客户端获取应用和队列指标。
     */
    void pollRMClient(MetricsSnapshot snapshot) {
        // 获取应用统计
        try {
            List<ApplicationReport> apps = yarnClient.getApplications();
            int active = 0;
            int completed = 0;
            int failed = 0;
            int submitted = 0;

            for (ApplicationReport app : apps) {
                YarnApplicationState state = app.getYarnApplicationState();
                switch (state) {
                    case RUNNING:
                    case ACCEPTED:
                        active++;
                        break;
                    case FINISHED:
                        completed++;
                        break;
                    case FAILED:
                        failed++;
                        break;
                    case SUBMITTED:
                    case NEW:
                    case NEW_SAVING:
                        submitted++;
                        break;
                    default:
                        break;
                }
            }

            snapshot.setActiveApplications(active);
            snapshot.setCompletedApplications(completed);
            snapshot.setFailedApplications(failed);
            snapshot.setSubmittedApplications(submitted);

            LOG.debug("Polled RM applications: active={}, completed={}, failed={}, submitted={}",
                    active, completed, failed, submitted);
        } catch (IOException | YarnException e) {
            LOG.warn("Failed to get applications from RM", e);
        }

        // 获取队列信息
        try {
            List<QueueInfo> queues = yarnClient.getAllQueues();
            if (queues != null && !queues.isEmpty()) {
                // 使用第一个队列（通常为 default）
                QueueInfo firstQueue = queues.get(0);
                snapshot.setQueueName(firstQueue.getQueueName());
                snapshot.setQueueUsedCapacity(firstQueue.getCurrentCapacity());
                snapshot.setQueueAbsoluteCapacity(firstQueue.getCapacity());

                // 队列中的应用数量
                int pending = 0;
                int activeInQueue = 0;
                if (firstQueue.getApplications() != null) {
                    for (ApplicationReport app : firstQueue.getApplications()) {
                        YarnApplicationState state = app.getYarnApplicationState();
                        switch (state) {
                            case SUBMITTED:
                            case NEW:
                            case NEW_SAVING:
                                pending++;
                                break;
                            case RUNNING:
                            case ACCEPTED:
                                activeInQueue++;
                                break;
                            default:
                                break;
                        }
                    }
                }
                snapshot.setQueuePendingApps(pending);
                snapshot.setQueueActiveApps(activeInQueue);
            }
        } catch (IOException | YarnException e) {
            LOG.warn("Failed to get queue info from RM", e);
        }
    }

    /**
     * 基于前后两次快照计算衍生指标：速率、成功率、吞吐量。
     */
    void computeDerivedMetrics(MetricsSnapshot snapshot) {
        MetricsSnapshot prev = previousSnapshot;
        if (prev == null) {
            snapshot.setHeartbeatSuccessRate(1.0);
            snapshot.setContainerAllocateRate(0);
            snapshot.setContainerReleaseRate(0);
            snapshot.setHeartbeatThroughput(0);
            return;
        }

        long timeDiff = snapshot.getTimestamp() - prev.getTimestamp();
        double intervalSeconds = timeDiff / 1000.0;

        if (intervalSeconds <= 0) {
            snapshot.setHeartbeatSuccessRate(1.0);
            snapshot.setContainerAllocateRate(0);
            snapshot.setContainerReleaseRate(0);
            snapshot.setHeartbeatThroughput(0);
            return;
        }

        // 心跳成功率
        long totalHb = snapshot.getSuccessfulHeartbeats() + snapshot.getFailedHeartbeats();
        if (totalHb > 0) {
            snapshot.setHeartbeatSuccessRate(
                    (double) snapshot.getSuccessfulHeartbeats() / totalHb);
        } else {
            snapshot.setHeartbeatSuccessRate(1.0);
        }

        // 容器分配速率 (ops/s) — 使用 Math.max(0, diff) 防止计数器重置产生负值
        long allocDiff = Math.max(0,
                snapshot.getTotalContainersAllocated() - prev.getTotalContainersAllocated());
        snapshot.setContainerAllocateRate(allocDiff / intervalSeconds);

        // 容器释放速率 (ops/s)
        long releaseDiff = Math.max(0,
                snapshot.getTotalContainersReleased() - prev.getTotalContainersReleased());
        snapshot.setContainerReleaseRate(releaseDiff / intervalSeconds);

        // 心跳吞吐量 (ops/s)
        long prevTotalHb = prev.getSuccessfulHeartbeats() + prev.getFailedHeartbeats();
        long currTotalHb = snapshot.getSuccessfulHeartbeats() + snapshot.getFailedHeartbeats();
        long hbDiff = Math.max(0, currTotalHb - prevTotalHb);
        snapshot.setHeartbeatThroughput(hbDiff / intervalSeconds);
    }

    @Override
    public void close() {
        LOG.info("Stopping MetricsCollector");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
