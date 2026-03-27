package org.apache.hadoop.sls.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class NodeHeartbeatStats {
    private final AtomicLong heartbeatCount = new AtomicLong(0);
    private final AtomicLong totalHeartbeatDuration = new AtomicLong(0);
    private final AtomicLong maxHeartbeatDuration = new AtomicLong(0);

    /**
     * 记录心跳持续时间并更新统计信息
     * 
     * @param duration 心跳持续时间（毫秒），必须为非负值
     * @throws IllegalArgumentException 如果 duration 为负数
     */
    public void recordHeartbeat(long duration) {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration cannot be negative: " + duration);
        }
        heartbeatCount.incrementAndGet();
        totalHeartbeatDuration.addAndGet(duration);
        maxHeartbeatDuration.updateAndGet(max -> Math.max(max, duration));
    }

    /**
     * 获取心跳次数
     * 
     * @return 心跳记录的总次数
     */
    public long getHeartbeatCount() {
        return heartbeatCount.get();
    }

    /**
     * 获取心跳总持续时间（毫秒）
     * 
     * @return 所有心跳持续时间的总和
     */
    public long getTotalHeartbeatDuration() {
        return totalHeartbeatDuration.get();
    }

    /**
     * 获取最大心跳持续时间（毫秒）
     * 
     * @return 所有心跳中最大的持续时间
     */
    public long getMaxHeartbeatDuration() {
        return maxHeartbeatDuration.get();
    }

    /**
     * 获取平均心跳持续时间（毫秒）
     * 
     * @return 平均持续时间，如果没有心跳记录则返回 0
     */
    public double getAverageHeartbeatDuration() {
        return heartbeatCount.get() == 0 ? 0 : (double) totalHeartbeatDuration.get() / heartbeatCount.get();
    }
}
