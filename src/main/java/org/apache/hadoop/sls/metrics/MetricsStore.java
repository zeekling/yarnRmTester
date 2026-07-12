package org.apache.hadoop.sls.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 线程安全的环形缓冲区，存储近期 MetricsSnapshot 时序数据。
 * 后端使用数组实现循环覆盖。
 * 写操作使用 synchronized，读操作返回快照副本。
 */
public class MetricsStore {

    private final MetricsSnapshot[] buffer;
    private final int capacity;
    private int head;       // 下一个写入位置
    private int count;      // 当前元素数量
    private volatile MetricsSnapshot latest; // 最新快照缓存（volatile 保证可见性）

    /**
     * @param capacity 环形缓冲区最大容量
     */
    public MetricsStore(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new MetricsSnapshot[capacity];
        this.head = 0;
        this.count = 0;
        this.latest = null;
    }

    /**
     * 添加一个快照到缓冲区。当缓冲区满时，覆盖最旧的元素。
     * 同时更新 latest 缓存。
     */
    public synchronized void add(MetricsSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        buffer[head] = snapshot;
        head = (head + 1) % capacity;
        if (count < capacity) {
            count++;
        }
        latest = snapshot;
    }

    /**
     * 获取最近的 N 条记录（从最新到最旧排序）。
     *
     * @param n 要获取的记录数
     * @return 按时间从最新到最旧排序的列表
     */
    public synchronized List<MetricsSnapshot> getRecent(int n) {
        if (count == 0) {
            return Collections.emptyList();
        }
        int actualCount = Math.min(n, count);
        List<MetricsSnapshot> result = new ArrayList<>(actualCount);
        // 从最新的开始遍历
        for (int i = 0; i < actualCount; i++) {
            int idx = (head - 1 - i + capacity) % capacity;
            MetricsSnapshot snap = buffer[idx];
            if (snap != null) {
                result.add(copySnapshot(snap));
            }
        }
        return result;
    }

    /**
     * 根据时间范围查询（包含两端）。
     *
     * @param startTime 起始时间戳（含）
     * @param endTime   结束时间戳（含）
     * @return 按时间升序排列的快照列表
     */
    public synchronized List<MetricsSnapshot> queryByTimeRange(long startTime, long endTime) {
        if (count == 0) {
            return Collections.emptyList();
        }
        List<MetricsSnapshot> result = new ArrayList<>();
        int start;
        int total;
        if (count < capacity) {
            start = 0;
            total = count;
        } else {
            start = head;
            total = capacity;
        }
        for (int i = 0; i < total; i++) {
            int idx = (start + i) % capacity;
            MetricsSnapshot snap = buffer[idx];
            if (snap != null && snap.getTimestamp() >= startTime && snap.getTimestamp() <= endTime) {
                result.add(copySnapshot(snap));
            }
        }
        return result;
    }

    /**
     * 返回所有快照的有序列表（从最旧到最新）。
     */
    public synchronized List<MetricsSnapshot> getAll() {
        if (count == 0) {
            return Collections.emptyList();
        }
        List<MetricsSnapshot> result = new ArrayList<>(count);
        int start;
        int total;
        if (count < capacity) {
            start = 0;
            total = count;
        } else {
            start = head;
            total = capacity;
        }
        for (int i = 0; i < total; i++) {
            int idx = (start + i) % capacity;
            MetricsSnapshot snap = buffer[idx];
            if (snap != null) {
                result.add(copySnapshot(snap));
            }
        }
        return result;
    }

    /**
     * 获取最新快照。
     *
     * @return 最新的 MetricsSnapshot，或 null（缓冲区为空时）
     */
    public MetricsSnapshot getLatest() {
        return latest;
    }

    /**
     * 返回当前存储的快照数量。
     */
    public synchronized int size() {
        return count;
    }

    /**
     * 返回缓冲区最大容量。
     */
    public int getCapacity() {
        return capacity;
    }

    /**
     * 创建 MetricsSnapshot 的深拷贝（字段复制）。
     */
    private MetricsSnapshot copySnapshot(MetricsSnapshot original) {
        MetricsSnapshot copy = new MetricsSnapshot();
        copy.setTimestamp(original.getTimestamp());

        copy.setTotalNodes(original.getTotalNodes());
        copy.setLostNodes(original.getLostNodes());
        copy.setUnhealthyNodes(original.getUnhealthyNodes());
        copy.setDecommissionedNodes(original.getDecommissionedNodes());
        copy.setTotalMemoryMB(original.getTotalMemoryMB());
        copy.setTotalVCores(original.getTotalVCores());
        copy.setAllocatedMemoryMB(original.getAllocatedMemoryMB());
        copy.setAllocatedVCores(original.getAllocatedVCores());
        copy.setAvailableMemoryMB(original.getAvailableMemoryMB());
        copy.setAvailableVCores(original.getAvailableVCores());
        copy.setClusterUtilizationPercent(original.getClusterUtilizationPercent());

        copy.setTotalContainersAllocated(original.getTotalContainersAllocated());
        copy.setTotalContainersReleased(original.getTotalContainersReleased());
        copy.setActiveContainers(original.getActiveContainers());
        copy.setPendingContainers(original.getPendingContainers());
        copy.setReservedContainers(original.getReservedContainers());

        copy.setActiveApplications(original.getActiveApplications());
        copy.setCompletedApplications(original.getCompletedApplications());
        copy.setFailedApplications(original.getFailedApplications());
        copy.setSubmittedApplications(original.getSubmittedApplications());

        copy.setSuccessfulHeartbeats(original.getSuccessfulHeartbeats());
        copy.setFailedHeartbeats(original.getFailedHeartbeats());
        copy.setHeartbeatSuccessRate(original.getHeartbeatSuccessRate());
        copy.setAvgHeartbeatLatencyMs(original.getAvgHeartbeatLatencyMs());
        copy.setMaxHeartbeatLatencyMs(original.getMaxHeartbeatLatencyMs());
        copy.setHeartbeatThroughput(original.getHeartbeatThroughput());

        // 深拷贝 Map 引用，防止调用方篡改 buffer 中的数据
        if (original.getQueueMetrics() != null) {
            copy.setQueueMetrics(new java.util.LinkedHashMap<>(original.getQueueMetrics()));
        }
        if (original.getNodeMetrics() != null) {
            copy.setNodeMetrics(new java.util.LinkedHashMap<>(original.getNodeMetrics()));
        }

        return copy;
    }
}
