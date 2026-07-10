package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;

public class MetricsSnapshotTest {

    public void testDefaultTimestamp() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        Assert.assertTrue("Timestamp should be set on creation",
                snapshot.getTimestamp() > 0);
    }

    public void testSetAndGetClusterMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setTotalNodes(10);
        snapshot.setTotalMemory(102400L);
        snapshot.setTotalVCores(200);
        snapshot.setAllocatedMemory(51200L);
        snapshot.setAllocatedVCores(100);
        snapshot.setAvailableMemory(51200L);
        snapshot.setAvailableVCores(100);
        snapshot.setClusterMemoryUtilization(0.5);
        snapshot.setClusterVCoreUtilization(0.5);

        Assert.assertEquals(10, snapshot.getTotalNodes());
        Assert.assertEquals(102400L, snapshot.getTotalMemory());
        Assert.assertEquals(200, snapshot.getTotalVCores());
        Assert.assertEquals(51200L, snapshot.getAllocatedMemory());
        Assert.assertEquals(100, snapshot.getAllocatedVCores());
        Assert.assertEquals(51200L, snapshot.getAvailableMemory());
        Assert.assertEquals(100, snapshot.getAvailableVCores());
        Assert.assertEquals(0.5, snapshot.getClusterMemoryUtilization());
        Assert.assertEquals(0.5, snapshot.getClusterVCoreUtilization());
    }

    public void testSetAndGetContainerMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setTotalContainersAllocated(1000L);
        snapshot.setTotalContainersReleased(400L);
        snapshot.setActiveContainers(600L);
        snapshot.setPendingContainers(5);
        snapshot.setReservedContainers(2);
        snapshot.setContainerAllocateRate(10.5);
        snapshot.setContainerReleaseRate(8.3);

        Assert.assertEquals(1000L, snapshot.getTotalContainersAllocated());
        Assert.assertEquals(400L, snapshot.getTotalContainersReleased());
        Assert.assertEquals(600L, snapshot.getActiveContainers());
        Assert.assertEquals(5, snapshot.getPendingContainers());
        Assert.assertEquals(2, snapshot.getReservedContainers());
        Assert.assertEquals(10.5, snapshot.getContainerAllocateRate());
        Assert.assertEquals(8.3, snapshot.getContainerReleaseRate());
    }

    public void testSetAndGetApplicationMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setActiveApplications(5);
        snapshot.setCompletedApplications(20);
        snapshot.setFailedApplications(2);
        snapshot.setSubmittedApplications(27);

        Assert.assertEquals(5, snapshot.getActiveApplications());
        Assert.assertEquals(20, snapshot.getCompletedApplications());
        Assert.assertEquals(2, snapshot.getFailedApplications());
        Assert.assertEquals(27, snapshot.getSubmittedApplications());
    }

    public void testSetAndGetHeartbeatMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setSuccessfulHeartbeats(500L);
        snapshot.setFailedHeartbeats(3L);
        snapshot.setHeartbeatSuccessRate(0.994);
        snapshot.setAvgHeartbeatLatency(12.5);
        snapshot.setMaxHeartbeatLatency(150L);
        snapshot.setHeartbeatThroughput(100.0);

        Assert.assertEquals(500L, snapshot.getSuccessfulHeartbeats());
        Assert.assertEquals(3L, snapshot.getFailedHeartbeats());
        Assert.assertEquals(0.994, snapshot.getHeartbeatSuccessRate());
        Assert.assertEquals(12.5, snapshot.getAvgHeartbeatLatency());
        Assert.assertEquals(150L, snapshot.getMaxHeartbeatLatency());
        Assert.assertEquals(100.0, snapshot.getHeartbeatThroughput());
    }

    public void testSetAndGetQueueMetrics() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setQueueName("default");
        snapshot.setQueueUsedCapacity(0.5);
        snapshot.setQueueAbsoluteCapacity(0.3);
        snapshot.setQueuePendingApps(2);
        snapshot.setQueueActiveApps(3);

        Assert.assertEquals("default", snapshot.getQueueName());
        Assert.assertEquals(0.5, snapshot.getQueueUsedCapacity());
        Assert.assertEquals(0.3, snapshot.getQueueAbsoluteCapacity());
        Assert.assertEquals(2, snapshot.getQueuePendingApps());
        Assert.assertEquals(3, snapshot.getQueueActiveApps());
    }

    public void testTimestampCanBeExplicitlySet() {
        MetricsSnapshot snapshot = new MetricsSnapshot();
        long customTime = 1234567890L;
        snapshot.setTimestamp(customTime);
        Assert.assertEquals(customTime, snapshot.getTimestamp());
    }
}
