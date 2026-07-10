package org.apache.hadoop.sls.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 固定容量环形缓冲区，用于存储 MetricsSnapshot 对象。
 * 线程安全：写操作使用 synchronized，读操作返回快照副本。
 */
public class MetricsStore {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsStore.class);

    private final MetricsSnapshot[] buffer;
    private final int capacity;
    private int head;   // 下一个写入位置
    private int count;  // 当前元素数量
    private boolean wrapped;

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
        this.wrapped = false;
    }

    /**
     * 添加一个快照到缓冲区。当缓冲区满时，覆盖最旧的元素。
     */
    public synchronized void add(MetricsSnapshot snapshot) {
        if (snapshot == null) {
            LOG.warn("Attempted to add null snapshot, ignoring");
            return;
        }
        buffer[head] = snapshot;
        head = (head + 1) % capacity;
        if (count < capacity) {
            count++;
        } else {
            wrapped = true;
        }
    }

    /**
     * 返回最近的一个快照，或 null（缓冲区为空时）。
     */
    public synchronized MetricsSnapshot getLatest() {
        if (count == 0) {
            return null;
        }
        int latestIdx = (head - 1 + capacity) % capacity;
        MetricsSnapshot original = buffer[latestIdx];
        if (original == null) {
            return null;
        }
        return copySnapshot(original);
    }

    /**
     * 返回所有快照的有序列表（从最旧到最新）。
     */
    public synchronized List<MetricsSnapshot> getAll() {
        List<MetricsSnapshot> result = new ArrayList<>(count);
        int start;
        int total;
        if (!wrapped && count < capacity) {
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

    /** 返回当前存储的快照数量 */
    public synchronized int size() {
        return count;
    }

    /** 返回缓冲区最大容量 */
    public int getCapacity() {
        return capacity;
    }

    /** 创建 MetricsSnapshot 的深拷贝（字段复制） */
    private MetricsSnapshot copySnapshot(MetricsSnapshot original) {
        MetricsSnapshot copy = new MetricsSnapshot();
        copy.setTimestamp(original.getTimestamp());

        copy.setTotalNodes(original.getTotalNodes());
        copy.setTotalMemory(original.getTotalMemory());
        copy.setTotalVCores(original.getTotalVCores());
        copy.setAllocatedMemory(original.getAllocatedMemory());
        copy.setAllocatedVCores(original.getAllocatedVCores());
        copy.setAvailableMemory(original.getAvailableMemory());
        copy.setAvailableVCores(original.getAvailableVCores());
        copy.setClusterMemoryUtilization(original.getClusterMemoryUtilization());
        copy.setClusterVCoreUtilization(original.getClusterVCoreUtilization());

        copy.setTotalContainersAllocated(original.getTotalContainersAllocated());
        copy.setTotalContainersReleased(original.getTotalContainersReleased());
        copy.setActiveContainers(original.getActiveContainers());
        copy.setPendingContainers(original.getPendingContainers());
        copy.setReservedContainers(original.getReservedContainers());
        copy.setContainerAllocateRate(original.getContainerAllocateRate());
        copy.setContainerReleaseRate(original.getContainerReleaseRate());

        copy.setActiveApplications(original.getActiveApplications());
        copy.setCompletedApplications(original.getCompletedApplications());
        copy.setFailedApplications(original.getFailedApplications());
        copy.setSubmittedApplications(original.getSubmittedApplications());

        copy.setSuccessfulHeartbeats(original.getSuccessfulHeartbeats());
        copy.setFailedHeartbeats(original.getFailedHeartbeats());
        copy.setHeartbeatSuccessRate(original.getHeartbeatSuccessRate());
        copy.setAvgHeartbeatLatency(original.getAvgHeartbeatLatency());
        copy.setMaxHeartbeatLatency(original.getMaxHeartbeatLatency());
        copy.setHeartbeatThroughput(original.getHeartbeatThroughput());

        copy.setQueueName(original.getQueueName());
        copy.setQueueUsedCapacity(original.getQueueUsedCapacity());
        copy.setQueueAbsoluteCapacity(original.getQueueAbsoluteCapacity());
        copy.setQueuePendingApps(original.getQueuePendingApps());
        copy.setQueueActiveApps(original.getQueueActiveApps());

        return copy;
    }
}
