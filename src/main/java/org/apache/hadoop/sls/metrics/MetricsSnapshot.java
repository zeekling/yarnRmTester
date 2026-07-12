package org.apache.hadoop.sls.metrics;

import java.util.Map;

/**
 * 单次采集的数据快照 POJO。
 * 包含集群资源、容器调度、应用状态、心跳指标、队列详情和节点详情。
 * 所有字段通过构造方法或 setter 赋值，不涉及业务逻辑。
 */
public class MetricsSnapshot {

    // ========== 时间戳 ==========
    private long timestamp;

    // ========== 集群资源 ==========
    private int totalNodes;
    private int lostNodes;
    private int unhealthyNodes;
    private int decommissionedNodes;
    private long totalMemoryMB;
    private int totalVCores;
    private long allocatedMemoryMB;
    private int allocatedVCores;
    private long availableMemoryMB;
    private int availableVCores;
    private double clusterUtilizationPercent;

    // ========== 容器调度 ==========
    private long totalContainersAllocated;
    private long totalContainersReleased;
    private long activeContainers;
    private int pendingContainers;
    private int reservedContainers;

    // ========== 应用状态 ==========
    private int activeApplications;
    private int completedApplications;
    private int failedApplications;
    private int submittedApplications;

    // ========== 心跳指标 ==========
    private long successfulHeartbeats;
    private long failedHeartbeats;
    private double heartbeatSuccessRate;
    private double avgHeartbeatLatencyMs;
    private double maxHeartbeatLatencyMs;
    private double heartbeatThroughput;

    // ========== 队列数据 ==========
    private Map<String, QueueMetrics> queueMetrics;

    // ========== 节点详情 ==========
    private Map<String, NodeMetrics> nodeMetrics;

    /** 默认构造器，timestamp 设为当前系统时间 */
    public MetricsSnapshot() {
        this.timestamp = System.currentTimeMillis();
    }

    // ======================== Getters & Setters ========================

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getTotalNodes() {
        return totalNodes;
    }

    public void setTotalNodes(int totalNodes) {
        this.totalNodes = totalNodes;
    }

    public int getLostNodes() {
        return lostNodes;
    }

    public void setLostNodes(int lostNodes) {
        this.lostNodes = lostNodes;
    }

    public int getUnhealthyNodes() {
        return unhealthyNodes;
    }

    public void setUnhealthyNodes(int unhealthyNodes) {
        this.unhealthyNodes = unhealthyNodes;
    }

    public int getDecommissionedNodes() {
        return decommissionedNodes;
    }

    public void setDecommissionedNodes(int decommissionedNodes) {
        this.decommissionedNodes = decommissionedNodes;
    }

    public long getTotalMemoryMB() {
        return totalMemoryMB;
    }

    public void setTotalMemoryMB(long totalMemoryMB) {
        this.totalMemoryMB = totalMemoryMB;
    }

    public int getTotalVCores() {
        return totalVCores;
    }

    public void setTotalVCores(int totalVCores) {
        this.totalVCores = totalVCores;
    }

    public long getAllocatedMemoryMB() {
        return allocatedMemoryMB;
    }

    public void setAllocatedMemoryMB(long allocatedMemoryMB) {
        this.allocatedMemoryMB = allocatedMemoryMB;
    }

    public int getAllocatedVCores() {
        return allocatedVCores;
    }

    public void setAllocatedVCores(int allocatedVCores) {
        this.allocatedVCores = allocatedVCores;
    }

    public long getAvailableMemoryMB() {
        return availableMemoryMB;
    }

    public void setAvailableMemoryMB(long availableMemoryMB) {
        this.availableMemoryMB = availableMemoryMB;
    }

    public int getAvailableVCores() {
        return availableVCores;
    }

    public void setAvailableVCores(int availableVCores) {
        this.availableVCores = availableVCores;
    }

    public double getClusterUtilizationPercent() {
        return clusterUtilizationPercent;
    }

    public void setClusterUtilizationPercent(double clusterUtilizationPercent) {
        this.clusterUtilizationPercent = clusterUtilizationPercent;
    }

    public long getTotalContainersAllocated() {
        return totalContainersAllocated;
    }

    public void setTotalContainersAllocated(long totalContainersAllocated) {
        this.totalContainersAllocated = totalContainersAllocated;
    }

    public long getTotalContainersReleased() {
        return totalContainersReleased;
    }

    public void setTotalContainersReleased(long totalContainersReleased) {
        this.totalContainersReleased = totalContainersReleased;
    }

    public long getActiveContainers() {
        return activeContainers;
    }

    public void setActiveContainers(long activeContainers) {
        this.activeContainers = activeContainers;
    }

    public int getPendingContainers() {
        return pendingContainers;
    }

    public void setPendingContainers(int pendingContainers) {
        this.pendingContainers = pendingContainers;
    }

    public int getReservedContainers() {
        return reservedContainers;
    }

    public void setReservedContainers(int reservedContainers) {
        this.reservedContainers = reservedContainers;
    }

    public int getActiveApplications() {
        return activeApplications;
    }

    public void setActiveApplications(int activeApplications) {
        this.activeApplications = activeApplications;
    }

    public int getCompletedApplications() {
        return completedApplications;
    }

    public void setCompletedApplications(int completedApplications) {
        this.completedApplications = completedApplications;
    }

    public int getFailedApplications() {
        return failedApplications;
    }

    public void setFailedApplications(int failedApplications) {
        this.failedApplications = failedApplications;
    }

    public int getSubmittedApplications() {
        return submittedApplications;
    }

    public void setSubmittedApplications(int submittedApplications) {
        this.submittedApplications = submittedApplications;
    }

    public long getSuccessfulHeartbeats() {
        return successfulHeartbeats;
    }

    public void setSuccessfulHeartbeats(long successfulHeartbeats) {
        this.successfulHeartbeats = successfulHeartbeats;
    }

    public long getFailedHeartbeats() {
        return failedHeartbeats;
    }

    public void setFailedHeartbeats(long failedHeartbeats) {
        this.failedHeartbeats = failedHeartbeats;
    }

    public double getHeartbeatSuccessRate() {
        return heartbeatSuccessRate;
    }

    public void setHeartbeatSuccessRate(double heartbeatSuccessRate) {
        this.heartbeatSuccessRate = heartbeatSuccessRate;
    }

    public double getAvgHeartbeatLatencyMs() {
        return avgHeartbeatLatencyMs;
    }

    public void setAvgHeartbeatLatencyMs(double avgHeartbeatLatencyMs) {
        this.avgHeartbeatLatencyMs = avgHeartbeatLatencyMs;
    }

    public double getMaxHeartbeatLatencyMs() {
        return maxHeartbeatLatencyMs;
    }

    public void setMaxHeartbeatLatencyMs(double maxHeartbeatLatencyMs) {
        this.maxHeartbeatLatencyMs = maxHeartbeatLatencyMs;
    }

    public double getHeartbeatThroughput() {
        return heartbeatThroughput;
    }

    public void setHeartbeatThroughput(double heartbeatThroughput) {
        this.heartbeatThroughput = heartbeatThroughput;
    }

    public Map<String, QueueMetrics> getQueueMetrics() {
        return queueMetrics;
    }

    public void setQueueMetrics(Map<String, QueueMetrics> queueMetrics) {
        this.queueMetrics = queueMetrics;
    }

    public Map<String, NodeMetrics> getNodeMetrics() {
        return nodeMetrics;
    }

    public void setNodeMetrics(Map<String, NodeMetrics> nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    // ======================== 向后兼容的旧 API 桥接 ========================

    /** @deprecated 使用 getTotalMemoryMB() 替代 */
    @Deprecated
    public long getTotalMemory() {
        return totalMemoryMB;
    }

    /** @deprecated 使用 setTotalMemoryMB(long) 替代 */
    @Deprecated
    public void setTotalMemory(long totalMemory) {
        this.totalMemoryMB = totalMemory;
    }

    /** @deprecated 使用 getAllocatedMemoryMB() 替代 */
    @Deprecated
    public long getAllocatedMemory() {
        return allocatedMemoryMB;
    }

    /** @deprecated 使用 setAllocatedMemoryMB(long) 替代 */
    @Deprecated
    public void setAllocatedMemory(long allocatedMemory) {
        this.allocatedMemoryMB = allocatedMemory;
    }

    /** @deprecated 使用 getAvailableMemoryMB() 替代 */
    @Deprecated
    public long getAvailableMemory() {
        return availableMemoryMB;
    }

    /** @deprecated 使用 setAvailableMemoryMB(long) 替代 */
    @Deprecated
    public void setAvailableMemory(long availableMemory) {
        this.availableMemoryMB = availableMemory;
    }

    /** @deprecated 使用 getClusterUtilizationPercent() 替代 */
    @Deprecated
    public double getClusterMemoryUtilization() {
        return clusterUtilizationPercent;
    }

    /** @deprecated 使用 setClusterUtilizationPercent(double) 替代 */
    @Deprecated
    public void setClusterMemoryUtilization(double utilization) {
        this.clusterUtilizationPercent = utilization;
    }

    /** @deprecated 不再使用 vCore 独立利用率，统一使用 clusterUtilizationPercent */
    @Deprecated
    public double getClusterVCoreUtilization() {
        return clusterUtilizationPercent;
    }

    /** @deprecated 不再使用 vCore 独立利用率 */
    @Deprecated
    public void setClusterVCoreUtilization(double utilization) {
        this.clusterUtilizationPercent = utilization;
    }

    /** @deprecated 使用 getQueueMetrics() 替代，返回第一个队列的队列名 */
    @Deprecated
    public String getQueueName() {
        if (queueMetrics == null || queueMetrics.isEmpty()) return null;
        return queueMetrics.values().iterator().next().getQueueName();
    }

    /** @deprecated 不再支持单个队列名 */
    @Deprecated
    public void setQueueName(String queueName) {
        // no-op, use setQueueMetrics instead
    }

    /** @deprecated 使用 getQueueMetrics() 替代 */
    @Deprecated
    public double getQueueUsedCapacity() {
        if (queueMetrics == null || queueMetrics.isEmpty()) return 0.0;
        return queueMetrics.values().iterator().next().getUsedCapacity();
    }

    /** @deprecated 使用 setQueueMetrics(Map) 替代 */
    @Deprecated
    public void setQueueUsedCapacity(double usedCapacity) {
        // no-op, use setQueueMetrics instead
    }

    /** @deprecated 使用 getQueueMetrics() 替代 */
    @Deprecated
    public double getQueueAbsoluteCapacity() {
        if (queueMetrics == null || queueMetrics.isEmpty()) return 0.0;
        return queueMetrics.values().iterator().next().getAbsoluteCapacity();
    }

    /** @deprecated 使用 setQueueMetrics(Map) 替代 */
    @Deprecated
    public void setQueueAbsoluteCapacity(double absoluteCapacity) {
        // no-op, use setQueueMetrics instead
    }

    /** @deprecated 使用 getQueueMetrics() 替代 */
    @Deprecated
    public int getQueuePendingApps() {
        if (queueMetrics == null || queueMetrics.isEmpty()) return 0;
        return queueMetrics.values().iterator().next().getPendingApps();
    }

    /** @deprecated 使用 setQueueMetrics(Map) 替代 */
    @Deprecated
    public void setQueuePendingApps(int pendingApps) {
        // no-op, use setQueueMetrics instead
    }

    /** @deprecated 使用 getQueueMetrics() 替代 */
    @Deprecated
    public int getQueueActiveApps() {
        if (queueMetrics == null || queueMetrics.isEmpty()) return 0;
        return queueMetrics.values().iterator().next().getActiveApps();
    }

    /** @deprecated 使用 setQueueMetrics(Map) 替代 */
    @Deprecated
    public void setQueueActiveApps(int activeApps) {
        // no-op, use setQueueMetrics instead
    }

    // ======================== 内部类 ========================

    /**
     * 队列指标。
     */
    public static class QueueMetrics {
        private String queueName;
        private double absoluteCapacity;
        private double usedCapacity;
        private int pendingApps;
        private int activeApps;
        private int pendingContainers;

        public QueueMetrics() {
        }

        public String getQueueName() {
            return queueName;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
        }

        public double getAbsoluteCapacity() {
            return absoluteCapacity;
        }

        public void setAbsoluteCapacity(double absoluteCapacity) {
            this.absoluteCapacity = absoluteCapacity;
        }

        public double getUsedCapacity() {
            return usedCapacity;
        }

        public void setUsedCapacity(double usedCapacity) {
            this.usedCapacity = usedCapacity;
        }

        public int getPendingApps() {
            return pendingApps;
        }

        public void setPendingApps(int pendingApps) {
            this.pendingApps = pendingApps;
        }

        public int getActiveApps() {
            return activeApps;
        }

        public void setActiveApps(int activeApps) {
            this.activeApps = activeApps;
        }

        public int getPendingContainers() {
            return pendingContainers;
        }

        public void setPendingContainers(int pendingContainers) {
            this.pendingContainers = pendingContainers;
        }
    }

    /**
     * 节点指标。
     */
    public static class NodeMetrics {
        private String nodeId;
        private long totalHeartbeats;
        private long successfulHeartbeats;
        private long failedHeartbeats;
        private double avgLatencyMs;
        private double maxLatencyMs;

        public NodeMetrics() {
        }

        public String getNodeId() {
            return nodeId;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public long getTotalHeartbeats() {
            return totalHeartbeats;
        }

        public void setTotalHeartbeats(long totalHeartbeats) {
            this.totalHeartbeats = totalHeartbeats;
        }

        public long getSuccessfulHeartbeats() {
            return successfulHeartbeats;
        }

        public void setSuccessfulHeartbeats(long successfulHeartbeats) {
            this.successfulHeartbeats = successfulHeartbeats;
        }

        public long getFailedHeartbeats() {
            return failedHeartbeats;
        }

        public void setFailedHeartbeats(long failedHeartbeats) {
            this.failedHeartbeats = failedHeartbeats;
        }

        public double getAvgLatencyMs() {
            return avgLatencyMs;
        }

        public void setAvgLatencyMs(double avgLatencyMs) {
            this.avgLatencyMs = avgLatencyMs;
        }

        public double getMaxLatencyMs() {
            return maxLatencyMs;
        }

        public void setMaxLatencyMs(double maxLatencyMs) {
            this.maxLatencyMs = maxLatencyMs;
        }
    }
}
