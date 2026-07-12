package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;
import java.util.List;

public class MetricsStoreTest {

    public void testEmptyStore() {
        MetricsStore store = new MetricsStore(10);
        Assert.assertEquals(0, store.size());
        Assert.assertEquals(10, store.getCapacity());
        Assert.assertNull("getLatest() on empty store should return null",
                store.getLatest());
        Assert.assertTrue("getAll() on empty store should be empty",
                store.getAll().isEmpty());
    }

    public void testAddAndRetrieve() {
        MetricsStore store = new MetricsStore(10);
        MetricsSnapshot s1 = new MetricsSnapshot();
        s1.setTotalNodes(5);

        store.add(s1);

        Assert.assertEquals(1, store.size());
        MetricsSnapshot latest = store.getLatest();
        Assert.assertEquals(5, latest.getTotalNodes());
    }

    public void testOrderMaintained() {
        MetricsStore store = new MetricsStore(10);
        MetricsSnapshot s1 = new MetricsSnapshot();
        s1.setTimestamp(100L);
        MetricsSnapshot s2 = new MetricsSnapshot();
        s2.setTimestamp(200L);

        store.add(s1);
        store.add(s2);

        List<MetricsSnapshot> all = store.getAll();
        Assert.assertEquals(2, all.size());
        Assert.assertEquals(100L, all.get(0).getTimestamp());
        Assert.assertEquals(200L, all.get(1).getTimestamp());
    }

    public void testCircularOverwrite() {
        MetricsStore store = new MetricsStore(3);
        for (int i = 1; i <= 5; i++) {
            MetricsSnapshot s = new MetricsSnapshot();
            s.setTimestamp(i * 100L);
            store.add(s);
        }

        // Capacity is 3, so only last 3 items should remain
        Assert.assertEquals(3, store.size());

        List<MetricsSnapshot> all = store.getAll();
        Assert.assertEquals(3, all.size());
        // Items with timestamp 300, 400, 500
        Assert.assertEquals(300L, all.get(0).getTimestamp());
        Assert.assertEquals(400L, all.get(1).getTimestamp());
        Assert.assertEquals(500L, all.get(2).getTimestamp());

        // Latest should be 500
        Assert.assertEquals(500L, store.getLatest().getTimestamp());
    }

    public void testCapacityOfOne() {
        MetricsStore store = new MetricsStore(1);
        MetricsSnapshot s1 = new MetricsSnapshot();
        s1.setTimestamp(100L);
        MetricsSnapshot s2 = new MetricsSnapshot();
        s2.setTimestamp(200L);

        store.add(s1);
        Assert.assertEquals(1, store.size());
        store.add(s2);
        Assert.assertEquals(1, store.size());

        Assert.assertEquals(200L, store.getLatest().getTimestamp());
    }

    public void testConcurrentAccess() throws InterruptedException {
        final MetricsStore store = new MetricsStore(100);
        Thread writer = new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                MetricsSnapshot s = new MetricsSnapshot();
                s.setTimestamp(i * 100L);
                store.add(s);
                try { Thread.sleep(1); } catch (InterruptedException e) { break; }
            }
        });

        writer.start();
        writer.join(5000);

        // Should have some data without corruption
        Assert.assertTrue("Store should have data after concurrent writes",
                store.size() > 0);
        Assert.assertNotNull("getLatest() should not be null", store.getLatest());
    }
}
