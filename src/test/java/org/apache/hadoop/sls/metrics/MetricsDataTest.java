package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;

public class MetricsDataTest {

    public void testUpdateLastHeartbeatTime() {
        MetricsData metricsData = new MetricsData();
        long initialTime = metricsData.getLastHeartbeatTime();

        // Update the heartbeat time
        metricsData.updateLastHeartbeatTime();

        // Verify that the time has been updated
        long updatedTime = metricsData.getLastHeartbeatTime();
        Assert.assertTrue("Last heartbeat time should be greater than or equal to initial time",
            updatedTime >= initialTime);
    }

    public void testMultipleHeartbeatTimeUpdates() {
        MetricsData metricsData = new MetricsData();

        // Get initial time before updates
        long initialTime = metricsData.getLastHeartbeatTime();

        // Perform multiple updates
        for (int i = 0; i < 5; i++) {
            long beforeUpdate = metricsData.getLastHeartbeatTime();
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
}
