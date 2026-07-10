package org.apache.hadoop.sls.metrics;

/**
 * 单次指标采集快照 POJO。
 * 包含集群资源、容器调度、应用调度、心跳、队列等全部指标字段。
 */
public class MetricsSnapshot {

    // ========== 时间戳 ==========
    private long timestamp;

    // ========== 集群资源 ==========
    private int totalNodes;
    private long totalMemory;
    private int totalVCores;
    private long allocatedMemory;
    private int allocatedVCores;
    private long availableMemory;
    private int availableVCores;
    private double clusterMemoryUtilization;
    private double clusterVCoreUtilization;

    // ========== 容器调度 ==========
    private long totalContainersAllocated;
    private long totalContainersReleased;
    private long activeContainers;
    private int pendingContainers;
    private int reservedContainers;
    private double containerAllocateRate;
    private double containerReleaseRate;

    // ========== 应用调度 ==========
    private int activeApplications;
    private int completedApplications;
    private int failedApplications;
    private int submittedApplications;

    // ========== 心跳 ==========
    private long successfulHeartbeats;
    private long failedHeartbeats;
    private double heartbeatSuccessRate;
    private double avgHeartbeatLatency;
    private long maxHeartbeatLatency;
    private double heartbeatThroughput;

    // ========== 队列 ==========
    private String queueName;
    private double queueUsedCapacity;
    private double queueAbsoluteCapacity;
    private int queuePendingApps;
    private int queueActiveApps;

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

    public long getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public int getTotalVCores() {
        return totalVCores;
    }

    public void setTotalVCores(int totalVCores) {
        this.totalVCores = totalVCores;
    }

    public long getAllocatedMemory() {
        return allocatedMemory;
    }

    public void setAllocatedMemory(long allocatedMemory) {
        this.allocatedMemory = allocatedMemory;
    }

    public int getAllocatedVCores() {
        return allocatedVCores;
    }

    public void setAllocatedVCores(int allocatedVCores) {
        this.allocatedVCores = allocatedVCores;
    }

    public long getAvailableMemory() {
        return availableMemory;
    }

    public void setAvailableMemory(long availableMemory) {
        this.availableMemory = availableMemory;
    }

    public int getAvailableVCores() {
        return availableVCores;
    }

    public void setAvailableVCores(int availableVCores) {
        this.availableVCores = availableVCores;
    }

    public double getClusterMemoryUtilization() {
        return clusterMemoryUtilization;
    }

    public void setClusterMemoryUtilization(double clusterMemoryUtilization) {
        this.clusterMemoryUtilization = clusterMemoryUtilization;
    }

    public double getClusterVCoreUtilization() {
        return clusterVCoreUtilization;
    }

    public void setClusterVCoreUtilization(double clusterVCoreUtilization) {
        this.clusterVCoreUtilization = clusterVCoreUtilization;
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

    public double getContainerAllocateRate() {
        return containerAllocateRate;
    }

    public void setContainerAllocateRate(double containerAllocateRate) {
        this.containerAllocateRate = containerAllocateRate;
    }

    public double getContainerReleaseRate() {
        return containerReleaseRate;
    }

    public void setContainerReleaseRate(double containerReleaseRate) {
        this.containerReleaseRate = containerReleaseRate;
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

    public double getAvgHeartbeatLatency() {
        return avgHeartbeatLatency;
    }

    public void setAvgHeartbeatLatency(double avgHeartbeatLatency) {
        this.avgHeartbeatLatency = avgHeartbeatLatency;
    }

    public long getMaxHeartbeatLatency() {
        return maxHeartbeatLatency;
    }

    public void setMaxHeartbeatLatency(long maxHeartbeatLatency) {
        this.maxHeartbeatLatency = maxHeartbeatLatency;
    }

    public double getHeartbeatThroughput() {
        return heartbeatThroughput;
    }

    public void setHeartbeatThroughput(double heartbeatThroughput) {
        this.heartbeatThroughput = heartbeatThroughput;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public double getQueueUsedCapacity() {
        return queueUsedCapacity;
    }

    public void setQueueUsedCapacity(double queueUsedCapacity) {
        this.queueUsedCapacity = queueUsedCapacity;
    }

    public double getQueueAbsoluteCapacity() {
        return queueAbsoluteCapacity;
    }

    public void setQueueAbsoluteCapacity(double queueAbsoluteCapacity) {
        this.queueAbsoluteCapacity = queueAbsoluteCapacity;
    }

    public int getQueuePendingApps() {
        return queuePendingApps;
    }

    public void setQueuePendingApps(int queuePendingApps) {
        this.queuePendingApps = queuePendingApps;
    }

    public int getQueueActiveApps() {
        return queueActiveApps;
    }

    public void setQueueActiveApps(int queueActiveApps) {
        this.queueActiveApps = queueActiveApps;
    }
}
