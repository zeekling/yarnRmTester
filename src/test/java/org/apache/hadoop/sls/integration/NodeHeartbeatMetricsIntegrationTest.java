package org.apache.hadoop.sls.integration;

import org.apache.hadoop.sls.metrics.MetricsData;
import org.apache.hadoop.sls.metrics.NodeHeartbeatStats;
import junit.framework.Assert;

public class NodeHeartbeatMetricsIntegrationTest {
    public void testMetricsDataUpdate() {
        MetricsData metricsData = new MetricsData();
        long initialHeartbeatTime = metricsData.getLastHeartbeatTime();

        metricsData.incrementSuccessfulHeartbeats();
        metricsData.updateLastHeartbeatTime();

        Assert.assertEquals(1, metricsData.getSuccessfulHeartbeats());

        long updatedHeartbeatTime = metricsData.getLastHeartbeatTime();
        Assert.assertTrue("Last heartbeat time should be different from initial time",
            updatedHeartbeatTime > initialHeartbeatTime);
    }

    public void testMultipleMetricsDataUpdates() {
        MetricsData metricsData = new MetricsData();
        long initialTime = metricsData.getLastHeartbeatTime();

        // Perform multiple updates
        for (int i = 0; i < 5; i++) {
            long beforeUpdate = metricsData.getLastHeartbeatTime();
            metricsData.incrementSuccessfulHeartbeats();
            metricsData.updateLastHeartbeatTime();
            long afterUpdate = metricsData.getLastHeartbeatTime();

            Assert.assertTrue("Heartbeat time should be non-decreasing",
                afterUpdate >= beforeUpdate);
        }

        // Verify that the final time is different from initial time
        long finalTime = metricsData.getLastHeartbeatTime();

        Assert.assertTrue("Final heartbeat time should be different from initial time",
            finalTime > initialTime);
    }

    public void testNodeHeartbeatStatsBasics() {
        NodeHeartbeatStats stats = new NodeHeartbeatStats();

        Assert.assertEquals(0, stats.getHeartbeatCount());
        Assert.assertEquals(0, stats.getTotalHeartbeatDuration());
        Assert.assertEquals(0L, stats.getMaxHeartbeatDuration());

        stats.recordHeartbeat(100L);
        Assert.assertEquals(1, stats.getHeartbeatCount());
        Assert.assertEquals(100L, stats.getTotalHeartbeatDuration());
        Assert.assertEquals(100L, stats.getMaxHeartbeatDuration());
        Assert.assertTrue(stats.getAverageHeartbeatDuration() > 0);
    }

    public void testConcurrentStatsRecording() throws Exception {
        NodeHeartbeatStats stats = new NodeHeartbeatStats();
        int threadCount = 10;
        int heartbeatsPerThread = 5;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < heartbeatsPerThread; j++) {
                        stats.recordHeartbeat((long) (Math.random() * 100 + 50));
                    }
                } catch (Exception e) {
                    Assert.fail("Unexpected exception in concurrent heartbeat: " + e.getMessage());
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assert.assertEquals(threadCount * heartbeatsPerThread, stats.getHeartbeatCount());
        Assert.assertTrue(stats.getTotalHeartbeatDuration() > 0);
        Assert.assertTrue(stats.getAverageHeartbeatDuration() > 0);
    }
}
