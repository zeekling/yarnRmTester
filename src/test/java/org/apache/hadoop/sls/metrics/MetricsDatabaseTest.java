package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;

/**
 * Test MetricsDatabase with an in-memory SQLite database.
 * Uses JUnit 3 style (method names start with "test").
 */
public class MetricsDatabaseTest {

    public void testDatabaseInitialization() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 10, 7);
        // Should not throw - tables created on init
        try {
            db.init();
        } catch (Exception e) {
            junit.framework.Assert.fail("Database initialization failed: " + e.getMessage());
        }
        db.close();
    }

    public void testInsertAndFlushSingleSnapshot() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 10, 7);
        try {
            db.init();
        } catch (Exception e) {
            junit.framework.Assert.fail("Database initialization failed: " + e.getMessage());
        }

        MetricsSnapshot snapshot = new MetricsSnapshot();
        snapshot.setTimestamp(System.currentTimeMillis());
        snapshot.setTotalNodes(10);
        snapshot.setTotalMemoryMB(102400L);
        snapshot.setTotalVCores(200);
        snapshot.setAllocatedMemoryMB(51200L);
        snapshot.setAllocatedVCores(100);
        snapshot.setAvailableMemoryMB(51200L);
        snapshot.setAvailableVCores(100);
        snapshot.setActiveContainers(600L);
        snapshot.setActiveApplications(5);
        snapshot.setCompletedApplications(20);
        snapshot.setFailedApplications(2);
        snapshot.setSubmittedApplications(27);

        // Use insertBatch
        db.insertBatch(java.util.Collections.singletonList(snapshot));
        // Should complete without errors
        db.close();
    }

    public void testBatchAutoFlush() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 2, 7);
        try {
            db.init();
        } catch (Exception e) {
            junit.framework.Assert.fail("Database initialization failed: " + e.getMessage());
        }

        MetricsSnapshot s1 = new MetricsSnapshot();
        s1.setTimestamp(100L);
        s1.setTotalNodes(5);

        MetricsSnapshot s2 = new MetricsSnapshot();
        s2.setTimestamp(200L);
        s2.setTotalNodes(10);

        MetricsSnapshot s3 = new MetricsSnapshot();
        s3.setTimestamp(300L);
        s3.setTotalNodes(15);

        // Use insertBatch
        db.insertBatch(java.util.Arrays.asList(s1, s2, s3));
        db.close();
    }

    public void testMultipleInsertsNoErrors() {
        MetricsDatabase db = new MetricsDatabase(":memory:", 5, 7);
        try {
            db.init();
        } catch (Exception e) {
            junit.framework.Assert.fail("Database initialization failed: " + e.getMessage());
        }

        java.util.List<MetricsSnapshot> batch = new java.util.ArrayList<>();
        for (int i = 0; i < 12; i++) {
            MetricsSnapshot s = new MetricsSnapshot();
            s.setTimestamp(i * 1000L);
            s.setTotalNodes(10);
            s.setTotalMemoryMB(102400L);
            s.setTotalVCores(200);
            s.setAllocatedMemoryMB(i * 1000L);
            s.setAllocatedVCores(i * 10);
            s.setAvailableMemoryMB(102400L - i * 1000L);
            s.setAvailableVCores(200 - i * 10);
            s.setActiveContainers(i * 50L);
            s.setActiveApplications(i);
            s.setCompletedApplications(i * 2);
            s.setFailedApplications(i / 2);
            s.setSubmittedApplications(i * 3);
            batch.add(s);
        }
        db.insertBatch(batch);
        db.close();
    }

    public void testCloseWithoutData() {
        // Should handle close gracefully with no data
        MetricsDatabase db = new MetricsDatabase(":memory:", 10, 7);
        try {
            db.init();
        } catch (Exception e) {
            junit.framework.Assert.fail("Database initialization failed: " + e.getMessage());
        }
        db.close();
    }
}
