package org.apache.hadoop.sls.metrics;

import junit.framework.TestCase;

public class TestMetricsData extends TestCase {

    public void testMetricsDataInitialization() {
        MetricsData data = new MetricsData();
        assertEquals(0, data.getTotalContainersAllocated());
        assertEquals(0, data.getTotalContainersReleased());
        assertEquals(0, data.getTotalHeartbeats());
        assertEquals(0, data.getSuccessfulHeartbeats());
        assertEquals(0, data.getFailedHeartbeats());
        assertTrue(data.getLastHeartbeatTime() > 0);
        assertTrue(data.getLastCollectTime() > 0);
    }

    public void testIncrementContainersAllocated() {
        MetricsData data = new MetricsData();
        data.incrementContainersAllocated();
        assertEquals(1, data.getTotalContainersAllocated());
        data.incrementContainersAllocated();
        assertEquals(2, data.getTotalContainersAllocated());
    }

    public void testIncrementContainersReleased() {
        MetricsData data = new MetricsData();
        data.incrementContainersReleased();
        assertEquals(1, data.getTotalContainersReleased());
        data.incrementContainersReleased();
        assertEquals(2, data.getTotalContainersReleased());
    }

    public void testIncrementHeartbeats() {
        MetricsData data = new MetricsData();
        data.incrementHeartbeats();
        assertEquals(1, data.getTotalHeartbeats());
        data.incrementHeartbeats();
        assertEquals(2, data.getTotalHeartbeats());
    }

    public void testIncrementSuccessfulHeartbeats() {
        MetricsData data = new MetricsData();
        data.incrementSuccessfulHeartbeats();
        assertEquals(1, data.getTotalHeartbeats());
        assertEquals(1, data.getSuccessfulHeartbeats());
        data.incrementSuccessfulHeartbeats();
        assertEquals(2, data.getTotalHeartbeats());
        assertEquals(2, data.getSuccessfulHeartbeats());
    }

    public void testIncrementFailedHeartbeats() {
        MetricsData data = new MetricsData();
        data.incrementFailedHeartbeats();
        assertEquals(1, data.getTotalHeartbeats());
        assertEquals(1, data.getFailedHeartbeats());
        data.incrementFailedHeartbeats();
        assertEquals(2, data.getTotalHeartbeats());
        assertEquals(2, data.getFailedHeartbeats());
    }

    public void testSetLastCollectTime() {
        MetricsData data = new MetricsData();
        long newTime = System.currentTimeMillis() + 1000;
        data.setLastCollectTime(newTime);
        assertEquals(newTime, data.getLastCollectTime());
    }
}