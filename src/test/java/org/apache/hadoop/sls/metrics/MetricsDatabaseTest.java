package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;

/**
 * Test MetricsDatabase with an in-memory SQLite database.
 * Uses JUnit 3 style (method names start with "test").
 */
public class MetricsDatabaseTest {

    public void testDatabaseInitialization() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 10, 7, 3600000);
        // Should not throw - tables created on init
        db.close();
    }

    public void testInsertAndFlushSingleSnapshot() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 10, 7, 3600000);

        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setTotalNodes(10);
        snapshot.setTotalMemory(102400L);
        snapshot.setTotalVCores(200);
        snapshot.setAllocatedMemory(51200L);
        snapshot.setAllocatedVCores(100);
        snapshot.setAvailableMemory(51200L);
        snapshot.setAvailableVCores(100);
        snapshot.setTotalContainersAllocated(1000L);
        snapshot.setTotalContainersReleased(400L);
        snapshot.setActiveContainers(600L);
        snapshot.setActiveApplications(5);
        snapshot.setCompletedApplications(20);
        snapshot.setFailedApplications(2);
        snapshot.setSubmittedApplications(27);
        snapshot.setSuccessfulHeartbeats(500L);
        snapshot.setFailedHeartbeats(3L);

        // batch size is 10, so add won't auto-flush
        db.add(snapshot);
        // Force flush
        db.flush();
        // Should complete without errors
        db.close();
    }

    public void testBatchAutoFlush() {
        // batch size 2, so every 2nd add triggers flush
        MetricsDatabase db = new MetricsDatabase(":memory:", 2, 7, 3600000);

        MetricsSnapshot s1 = new MetricsSnapshot();
        s1.setTimestamp(100L);
        s1.setTotalNodes(5);

        MetricsSnapshot s2 = new MetricsSnapshot();
        s2.setTimestamp(200L);
        s2.setTotalNodes(10);

        MetricsSnapshot s3 = new MetricsSnapshot();
        s3.setTimestamp(300L);
        s3.setTotalNodes(15);

        // s1 and s2 should trigger auto-flush (batch=2)
        db.add(s1);
        db.add(s2);
        // s3 stays in batch until flush or close
        db.add(s3);
        db.close(); // close triggers final flush
    }

    public void testMultipleInsertsNoErrors() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 5, 7, 3600000);

        for (int i = 0; i < 12; i++) {
            MetricsSnapshot s = new MetricsSnapshot();
            s.setTotalNodes(10);
            s.setTotalMemory(102400L);
            s.setTotalVCores(200);
            s.setAllocatedMemory(i * 1000L);
            s.setAllocatedVCores(i * 10);
            s.setAvailableMemory(102400L - i * 1000L);
            s.setAvailableVCores(200 - i * 10);
            s.setTotalContainersAllocated(i * 100L);
            s.setTotalContainersReleased(i * 50L);
            s.setActiveContainers(i * 50L);
            s.setActiveApplications(i);
            s.setCompletedApplications(i * 2);
            s.setFailedApplications(i / 2);
            s.setSubmittedApplications(i * 3);
            s.setSuccessfulHeartbeats(i * 100L);
            s.setFailedHeartbeats((long) i);
            s.setQueueName("default");
            db.add(s);
        }

        db.close();
    }

    public void testCloseWithoutData() {
        // Should handle close gracefully with no data
        MetricsDatabase db = new MetricsDatabase(":memory:", 10, 7, 3600000);
        db.close();
    }
}
