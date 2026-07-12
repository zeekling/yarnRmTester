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

/**
 * RM JMX 指标采集器。
 *
 * 通过 HTTP GET 请求 RM 的 JMX 端点（如 http://hadoop01:8088/jmx?qry=Hadoop:*），
 * 解析 JMX Bean JSON，提取集群资源、队列调度和应用状态指标。
 *
 * 采集的 JMX Bean：
 * - ClusterMetrics → 集群节点数（活跃/失联/不健康）
 * - QueueMetrics(q0=root) → 全局资源总量（内存/vCore/容器）、应用状态
 * - QueueMetrics(q0=root,q1=*) → 子队列详细指标
 */
public class JmxMetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(JmxMetricsCollector.class);

    private final String jmxUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * @param jmxUrl RM JMX 端点 URL，如 http://hadoop01:8088/jmx?qry=Hadoop:*
     */
    public JmxMetricsCollector(String jmxUrl) {
        this.jmxUrl = jmxUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * 执行单次 JMX 采集。
     *
     * @return 填充了 JMX 指标的 MetricsSnapshot，或 null（失败时）
     */
    public MetricsSnapshot collectOnce() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(jmxUrl))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                LOG.warn("RM JMX returned status {} for {}", response.statusCode(), jmxUrl);
                return null;
            }

            JsonNode root = objectMapper.readTree(response.body());
            return parseJmxResponse(root);
        } catch (Exception e) {
            LOG.warn("Failed to collect RM JMX metrics from {}: {}", jmxUrl, e.getMessage());
            return null;
        }
    }

    /**
     * 解析 RM JMX 响应 JSON，提取 Bean 数据并填充 MetricsSnapshot。
     */
    MetricsSnapshot parseJmxResponse(JsonNode root) {
        MetricsSnapshot snapshot = new MetricsSnapshot();

        JsonNode beans = root.get("beans");
        if (beans == null || !beans.isArray()) {
            LOG.warn("RM JMX response has no 'beans' array");
            return snapshot;
        }

        boolean foundClusterMetrics = false;
        boolean foundRootQueue = false;

        for (int i = 0; i < beans.size(); i++) {
            JsonNode bean = beans.get(i);
            String name = bean.has("name") ? bean.get("name").asText("") : "";

            if (name.contains("ClusterMetrics") && name.contains("ResourceManager")) {
                parseClusterMetrics(snapshot, bean);
                foundClusterMetrics = true;
            } else if (name.contains("QueueMetrics") && name.contains("q0=root")) {
                // 判断是 root 还是子队列
                if (name.contains("q1=")) {
                    // 子队列
                    parseQueueMetrics(snapshot, bean, false);
                } else {
                    // root 队列：包含全局资源容量
                    parseClusterResources(snapshot, bean);
                    parseQueueMetrics(snapshot, bean, true);
                    foundRootQueue = true;
                }
            }
        }

        // 计算集群利用率
        if (snapshot.getTotalMemoryMB() > 0) {
            double util = (double) snapshot.getAllocatedMemoryMB()
                    / (double) snapshot.getTotalMemoryMB() * 100.0;
            snapshot.setClusterUtilizationPercent(Math.round(util * 10.0) / 10.0);
        }

        if (!foundClusterMetrics) {
            LOG.debug("No ClusterMetrics bean found in JMX response");
        }
        if (!foundRootQueue) {
            LOG.debug("No root QueueMetrics bean found in JMX response");
        }

        return snapshot;
    }

    /**
     * 解析 ClusterMetrics（集群节点统计）。
     */
    private void parseClusterMetrics(MetricsSnapshot snapshot, JsonNode bean) {
        int activeNMs = getInt(bean, "NumActiveNMs", 0);
        int lostNMs = getInt(bean, "NumLostNMs", 0);
        int unhealthyNMs = getInt(bean, "NumUnhealthyNMs", 0);
        int decommissionedNMs = getInt(bean, "NumDecommissionedNMs", 0);

        // 注意：rebootedNMs 是活跃节点的子集，不计入 totalNodes 以避免重复计算
        int totalNMs = activeNMs + lostNMs + unhealthyNMs + decommissionedNMs;
        if (totalNMs > 0) {
            snapshot.setTotalNodes(totalNMs);
        } else {
            // 如果所有分类都为零但可能有值，至少报告活跃节点
            snapshot.setTotalNodes(activeNMs);
        }

        snapshot.setLostNodes(lostNMs);
        snapshot.setUnhealthyNodes(unhealthyNMs);
        snapshot.setDecommissionedNodes(decommissionedNMs);

        LOG.debug("JMX ClusterMetrics: active={}, lost={}, unhealthy={}, decom={}",
                activeNMs, lostNMs, unhealthyNMs, decommissionedNMs);
    }

    /**
     * 解析 root QueueMetrics 中的全局资源总量。
     * root 队列的 AllocatedMB + AvailableMB = 集群总内存容量。
     */
    private void parseClusterResources(MetricsSnapshot snapshot, JsonNode bean) {
        long allocatedMB = getLong(bean, "AllocatedMB", 0);
        long availableMB = getLong(bean, "AvailableMB", 0);
        int allocatedVCores = getInt(bean, "AllocatedVCores", 0);
        int availableVCores = getInt(bean, "AvailableVCores", 0);

        snapshot.setTotalMemoryMB(allocatedMB + availableMB);
        snapshot.setTotalVCores(allocatedVCores + availableVCores);
        snapshot.setAllocatedMemoryMB(allocatedMB);
        snapshot.setAllocatedVCores(allocatedVCores);
        snapshot.setAvailableMemoryMB(availableMB);
        snapshot.setAvailableVCores(availableVCores);

        // 容器调度（当前值，非累计值）
        snapshot.setActiveContainers(getLong(bean, "AllocatedContainers", 0));
        snapshot.setPendingContainers(getInt(bean, "PendingContainers", 0));
        snapshot.setReservedContainers(getInt(bean, "ReservedContainers", 0));

        LOG.debug("JMX ClusterResources: totalMem={}MB, totalVCores={}, allocatedMem={}MB, pendingContainers={}",
                snapshot.getTotalMemoryMB(), snapshot.getTotalVCores(),
                snapshot.getAllocatedMemoryMB(), snapshot.getPendingContainers());
    }

    /**
     * 解析队列指标。
     *
     * @param isRoot 是否为 root 队列（root 队列包含全局应用累计数据）
     */
    private void parseQueueMetrics(MetricsSnapshot snapshot, JsonNode bean, boolean isRoot) {
        String queueName = isRoot ? "root" : buildQueueName(bean);

        MetricsSnapshot.QueueMetrics qm = new MetricsSnapshot.QueueMetrics();
        qm.setQueueName(queueName);

        if (isRoot) {
            // root 队列的容量指标
            qm.setAbsoluteCapacity(100.0);
            if (snapshot.getTotalMemoryMB() > 0) {
                qm.setUsedCapacity(Math.round(
                        (double) snapshot.getAllocatedMemoryMB() / (double) snapshot.getTotalMemoryMB() * 100.0 * 10.0) / 10.0);
            }
        } else {
            // 子队列从 JMX 获取容量
            qm.setAbsoluteCapacity(getDouble(bean, "AbsoluteCapacity", 0.0) * 100.0);
            qm.setUsedCapacity(getDouble(bean, "UsedCapacity", 0.0) * 100.0);
        }

        qm.setPendingApps(getInt(bean, "AppsPending", 0));
        qm.setActiveApps(getInt(bean, "ActiveApplications",
                getInt(bean, "AppsRunning", 0)));
        qm.setPendingContainers(getInt(bean, "PendingContainers", 0));

        // 对于 root 队列，提取全局应用状态（累计值）
        if (isRoot) {
            snapshot.setActiveApplications(getInt(bean, "ActiveApplications",
                    getInt(bean, "AppsRunning", 0)));
            snapshot.setCompletedApplications(getInt(bean, "AppsCompleted", 0));
            snapshot.setFailedApplications(getInt(bean, "AppsFailed", 0));
            snapshot.setSubmittedApplications(getInt(bean, "AppsSubmitted", 0));
        }

        // 存储队列指标
        Map<String, MetricsSnapshot.QueueMetrics> queueMetrics = snapshot.getQueueMetrics();
        if (queueMetrics == null) {
            queueMetrics = new LinkedHashMap<>();
            snapshot.setQueueMetrics(queueMetrics);
        }
        queueMetrics.put(queueName, qm);
    }

    /**
     * 从 QueueMetrics bean 的 name 字段提取子队列名。
     * name 格式：Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default
     */
    private String buildQueueName(JsonNode bean) {
        String name = bean.has("name") ? bean.get("name").asText("") : "";
        // 提取 q1=xxx 部分
        String[] parts = name.split(",");
        StringBuilder sb = new StringBuilder("root");
        for (String part : parts) {
            part = part.trim();
            if (part.startsWith("q") && part.contains("=") && !part.equals("q0=root")) {
                String val = part.substring(part.indexOf('=') + 1);
                sb.append(".").append(val);
            }
        }
        return sb.toString();
    }

    // ========== JSON 字段安全读取 ==========

    private int getInt(JsonNode node, String field, int defaultValue) {
        JsonNode value = node.get(field);
        if (value == null) return defaultValue;
        try {
            return value.asInt(defaultValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private long getLong(JsonNode node, String field, long defaultValue) {
        JsonNode value = node.get(field);
        if (value == null) return defaultValue;
        try {
            return value.asLong(defaultValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private double getDouble(JsonNode node, String field, double defaultValue) {
        JsonNode value = node.get(field);
        if (value == null) return defaultValue;
        try {
            return value.asDouble(defaultValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
