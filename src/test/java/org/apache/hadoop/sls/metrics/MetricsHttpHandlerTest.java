package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

public class MetricsHttpHandlerTest {

    public void testGetNodeHeartbeatStatsMap() throws IOException {
        Map<String, HeartbeatResponseCollector> heartbeatCollectors = new HashMap<>();
        ResourceManagerMetricsCollector rmMetricsCollector = new ResourceManagerMetricsCollector(null);
        MetricsServer metricsServer = new MetricsServer(9080, null);

        MetricsHttpHandler handler = new MetricsHttpHandler(heartbeatCollectors, rmMetricsCollector, metricsServer);

        Map<String, NodeHeartbeatStats> statsMap = handler.getNodeHeartbeatStatsMap();
        Assert.assertNotNull("Node heartbeat stats map should not be null", statsMap);
        Assert.assertTrue("Node heartbeat stats map should be empty initially", statsMap.isEmpty());
    }

    public void testGetNodeHeartbeatMetrics() throws IOException {
        Map<String, HeartbeatResponseCollector> heartbeatCollectors = new HashMap<>();
        ResourceManagerMetricsCollector rmMetricsCollector = new ResourceManagerMetricsCollector(null);
        MetricsServer metricsServer = new MetricsServer(9081, null);

        MetricsHttpHandler handler = new MetricsHttpHandler(heartbeatCollectors, rmMetricsCollector, metricsServer);

        Map<String, Map<String, Object>> nodeMetrics = handler.getNodeHeartbeatMetrics();
        Assert.assertNotNull("Node metrics should not be null", nodeMetrics);
        Assert.assertTrue("Node metrics should be empty initially", nodeMetrics.isEmpty());
    }

    public void testGetNodeHeartbeatMetricsWithCollectors() throws Exception {
        Map<String, HeartbeatResponseCollector> heartbeatCollectors = new HashMap<>();
        ResourceManagerMetricsCollector rmMetricsCollector = new ResourceManagerMetricsCollector(null);
        MetricsServer metricsServer = new MetricsServer(9082, null);

        MetricsData metricsData1 = new MetricsData();
        MetricsData metricsData2 = new MetricsData();
        HeartbeatResponseCollector collector1 = new HeartbeatResponseCollector(metricsData1);
        HeartbeatResponseCollector collector2 = new HeartbeatResponseCollector(metricsData2);

        metricsServer.registerHeartbeatCollector("node1", collector1);
        metricsServer.registerHeartbeatCollector("node2", collector2);

        heartbeatCollectors.put("node1", collector1);
        heartbeatCollectors.put("node2", collector2);

        MetricsHttpHandler handler = new MetricsHttpHandler(heartbeatCollectors, rmMetricsCollector, metricsServer);

        Map<String, Map<String, Object>> nodeMetrics = handler.getNodeHeartbeatMetrics();
        Assert.assertNotNull("Node metrics should not be null", nodeMetrics);
        Assert.assertEquals("Should have 2 nodes", 2, nodeMetrics.size());
        Assert.assertTrue("Should contain node1", nodeMetrics.containsKey("node1"));
        Assert.assertTrue("Should contain node2", nodeMetrics.containsKey("node2"));

        Map<String, Object> node1Data = nodeMetrics.get("node1");
        Assert.assertNotNull("Node1 data should not be null", node1Data);
        Assert.assertTrue("Node1 should have totalHeartbeats", node1Data.containsKey("totalHeartbeats"));
        Assert.assertTrue("Node1 should have successfulHeartbeats", node1Data.containsKey("successfulHeartbeats"));
        Assert.assertTrue("Node1 should have failedHeartbeats", node1Data.containsKey("failedHeartbeats"));
    }

    public void testGetNodeHeartbeatMetricsWithStats() throws Exception {
        Map<String, HeartbeatResponseCollector> heartbeatCollectors = new HashMap<>();
        ResourceManagerMetricsCollector rmMetricsCollector = new ResourceManagerMetricsCollector(null);
        MetricsServer metricsServer = new MetricsServer(9083, null);

        MetricsData metricsData = new MetricsData();
        HeartbeatResponseCollector collector = new HeartbeatResponseCollector(metricsData);

        metricsServer.registerHeartbeatCollector("testNode", collector);

        metricsData.incrementSuccessfulHeartbeats();
        metricsData.incrementFailedHeartbeats();

        heartbeatCollectors.put("testNode", collector);

        MetricsHttpHandler handler = new MetricsHttpHandler(heartbeatCollectors, rmMetricsCollector, metricsServer);

        Map<String, Map<String, Object>> nodeMetrics = handler.getNodeHeartbeatMetrics();
        Assert.assertNotNull("Node metrics should not be null", nodeMetrics);
        Assert.assertTrue("Should contain testNode", nodeMetrics.containsKey("testNode"));

        Map<String, Object> nodeData = nodeMetrics.get("testNode");
        Assert.assertEquals("Total heartbeats should be 3 (1 + 1 + 1)", 3L, nodeData.get("totalHeartbeats"));
        Assert.assertEquals("Successful heartbeats should be 1", 1L, nodeData.get("successfulHeartbeats"));
        Assert.assertEquals("Failed heartbeats should be 1", 1L, nodeData.get("failedHeartbeats"));
        Assert.assertEquals("Heartbeat count should be 0", 0L, nodeData.get("heartbeatCount"));
        Assert.assertEquals("Total heartbeat duration should be 0", 0L, nodeData.get("totalHeartbeatDuration"));
        Assert.assertEquals("Min heartbeat duration should be Long.MAX_VALUE", Long.MAX_VALUE, nodeData.get("minHeartbeatDuration"));
        Assert.assertEquals("Max heartbeat duration should be 0", 0L, nodeData.get("maxHeartbeatDuration"));
        Assert.assertEquals("Average heartbeat duration should be 0", 0.0, nodeData.get("avgHeartbeatDuration"));
    }
}
