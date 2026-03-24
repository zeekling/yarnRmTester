package org.apache.hadoop.sls.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class MetricsData {
    private final AtomicLong totalContainersAllocated = new AtomicLong(0);
    private final AtomicLong totalContainersReleased = new AtomicLong(0);
    private final AtomicLong totalHeartbeats = new AtomicLong(0);
    private final AtomicLong successfulHeartbeats = new AtomicLong(0);
    private final AtomicLong failedHeartbeats = new AtomicLong(0);
    private final long lastHeartbeatTime;
    private volatile long lastCollectTime;

    public MetricsData() {
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.lastCollectTime = System.currentTimeMillis();
    }

    public void incrementContainersAllocated() {
        totalContainersAllocated.incrementAndGet();
    }

    public void incrementContainersReleased() {
        totalContainersReleased.incrementAndGet();
    }

    public void incrementHeartbeats() {
        totalHeartbeats.incrementAndGet();
    }

    public void incrementSuccessfulHeartbeats() {
        totalHeartbeats.incrementAndGet();
        successfulHeartbeats.incrementAndGet();
    }

    public void incrementFailedHeartbeats() {
        totalHeartbeats.incrementAndGet();
        failedHeartbeats.incrementAndGet();
    }

    public void updateLastHeartbeatTime() {
    }

    public long getTotalContainersAllocated() {
        return totalContainersAllocated.get();
    }

    public long getTotalContainersReleased() {
        return totalContainersReleased.get();
    }

    public long getTotalHeartbeats() {
        return totalHeartbeats.get();
    }

    public long getSuccessfulHeartbeats() {
        return successfulHeartbeats.get();
    }

    public long getFailedHeartbeats() {
        return failedHeartbeats.get();
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public long getLastCollectTime() {
        return lastCollectTime;
    }

    public void setLastCollectTime(long lastCollectTime) {
        this.lastCollectTime = lastCollectTime;
    }
}