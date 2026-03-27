package org.apache.hadoop.sls.nm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.metrics.NodeHeartbeatStats;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;

import junit.framework.Assert;

public class YarnFakeNodeManagerIntegrationTest {
    public void testStatsRecordingOnHeartbeats() throws Exception {
        YarnConfiguration config = new YarnConfiguration();
        Resource capability = Resource.newInstance(1024, 2);
        String configPath = new File("src/test/resources/sls-test.properties").getAbsolutePath();
        SLSConfig slsConfig = new SLSConfig(configPath);

        YarnFakeNodeManager nodeManager = new YarnFakeNodeManager(12345, 8080, "rack1", capability, config, slsConfig);

        NodeHeartbeatStats stats = nodeManager.getHeartbeatStats();
        Assert.assertEquals(0, stats.getHeartbeatCount());

        nodeManager.heartbeat();

        stats = nodeManager.getHeartbeatStats();
        Assert.assertEquals(1, stats.getHeartbeatCount());
        Assert.assertTrue(stats.getTotalHeartbeatDuration() > 0);

        nodeManager.heartbeat();
        nodeManager.heartbeat();

        stats = nodeManager.getHeartbeatStats();
        Assert.assertEquals(3, stats.getHeartbeatCount());
        Assert.assertTrue(stats.getAverageHeartbeatDuration() > 0);
    }

    public void testStatsAccessibility() throws Exception {
        YarnConfiguration config = new YarnConfiguration();
        Resource capability = Resource.newInstance(1024, 2);
        String configPath = new File("src/test/resources/sls-test.properties").getAbsolutePath();
        SLSConfig slsConfig = new SLSConfig(configPath);

        YarnFakeNodeManager nodeManager = new YarnFakeNodeManager(12346, 8081, "rack2", capability, config, slsConfig);

        NodeHeartbeatStats stats = nodeManager.getHeartbeatStats();
        Assert.assertNotNull(stats);
        Assert.assertEquals(0, stats.getHeartbeatCount());

        nodeManager.heartbeat();
        stats = nodeManager.getHeartbeatStats();

        Assert.assertNotNull(stats);
        Assert.assertEquals(1, stats.getHeartbeatCount());
    }

    public void testConcurrentHeartbeatStats() throws Exception {
        YarnConfiguration config = new YarnConfiguration();
        Resource capability = Resource.newInstance(1024, 2);
        String configPath = new File("src/test/resources/sls-test.properties").getAbsolutePath();
        SLSConfig slsConfig = new SLSConfig(configPath);

        YarnFakeNodeManager nodeManager = new YarnFakeNodeManager(12347, 8082, "rack3", capability, config, slsConfig);

        NodeHeartbeatStats stats = nodeManager.getHeartbeatStats();
        Assert.assertEquals(0, stats.getHeartbeatCount());

        int threadCount = 10;
        int heartbeatsPerThread = 5;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < heartbeatsPerThread; j++) {
                        nodeManager.heartbeat();
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

        stats = nodeManager.getHeartbeatStats();
        Assert.assertEquals(threadCount * heartbeatsPerThread, stats.getHeartbeatCount());

        Assert.assertTrue(stats.getTotalHeartbeatDuration() > 0);
        Assert.assertTrue(stats.getAverageHeartbeatDuration() > 0);
    }
}
