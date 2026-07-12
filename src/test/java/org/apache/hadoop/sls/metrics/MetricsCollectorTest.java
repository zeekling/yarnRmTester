package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.Assert;

import java.util.Map;

/**
 * MetricsCollector 单元测试。
 * 测试 HTTP polling collector 的 JSON 解析逻辑。
 * 不涉及真实的 HTTP 请求（通过 parseMetricsResponse 直接测试）。
 */
public class MetricsCollectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 测试 parseMetricsResponse 能正确解析 /metrics 的全部字段。
     */
    public void testParseMetricsResponseFullData() throws Exception {
        String json = "{"
                + "  \"timestamp\": 1234567890123,"
                + "  \"cluster\": { \"totalNodes\": 15 },"
                + "  \"scheduling\": {"
                + "    \"totalContainersAllocated\": 1000,"
                + "    \"totalContainersReleased\": 400"
                + "  },"
                + "  \"applications\": {"
                + "    \"active\": 10,"
                + "    \"completed\": 50,"
                + "    \"failed\": 2"
                + "  },"
                + "  \"nodeHeartbeatMetrics\": {"
                + "    \"node1:12000\": {"
                + "      \"totalHeartbeats\": 100,"
                + "      \"successfulHeartbeats\": 98,"
                + "      \"failedHeartbeats\": 2,"
                + "      \"avgHeartbeatDuration\": 15.5,"
                + "      \"maxHeartbeatDuration\": 120.0"
                + "    },"
                + "    \"node2:12001\": {"
                + "      \"totalHeartbeats\": 100,"
                + "      \"successfulHeartbeats\": 100,"
                + "      \"failedHeartbeats\": 0,"
                + "      \"avgHeartbeatDuration\": 10.2,"
                + "      \"maxHeartbeatDuration\": 45.0"
                + "    }"
                + "  }"
                + "}";

        // 使用反射调用 parseMetricsResponse，避免真实 HTTP 请求
        MetricsStore store = new MetricsStore(100);
        MetricsDatabase database = null;
        MetricsCollector collector = new MetricsCollector("http://localhost:28080", 5000, store, database);

        JsonNode root = MAPPER.readTree(json);
        java.lang.reflect.Method parseMethod = MetricsCollector.class.getDeclaredMethod(
                "parseMetricsResponse", JsonNode.class);
        parseMethod.setAccessible(true);
        MetricsSnapshot snapshot = (MetricsSnapshot) parseMethod.invoke(collector, root);

        // Verify timestamp
        Assert.assertEquals("timestamp should match", 1234567890123L, snapshot.getTimestamp());

        // Verify cluster
        Assert.assertEquals("totalNodes should be 15", 15, snapshot.getTotalNodes());

        // Verify scheduling
        Assert.assertEquals("totalContainersAllocated should be 1000",
                1000L, snapshot.getTotalContainersAllocated());
        Assert.assertEquals("totalContainersReleased should be 400",
                400L, snapshot.getTotalContainersReleased());
        Assert.assertEquals("activeContainers should be 600 (1000-400)",
                600L, snapshot.getActiveContainers());

        // Verify applications
        Assert.assertEquals("activeApplications should be 10",
                10, snapshot.getActiveApplications());
        Assert.assertEquals("completedApplications should be 50",
                50, snapshot.getCompletedApplications());
        Assert.assertEquals("failedApplications should be 2",
                2, snapshot.getFailedApplications());

        // Verify heartbeat aggregation
        Assert.assertEquals("successfulHeartbeats should be 198 (98+100)",
                198L, snapshot.getSuccessfulHeartbeats());
        Assert.assertEquals("failedHeartbeats should be 2",
                2L, snapshot.getFailedHeartbeats());
        Assert.assertEquals("heartbeatSuccessRate should be ~99.0",
                99.0, snapshot.getHeartbeatSuccessRate(), 0.1);
        Assert.assertEquals("avgHeartbeatLatencyMs should be ~12.85 ((15.5+10.2)/2)",
                12.85, snapshot.getAvgHeartbeatLatencyMs(), 0.01);
        Assert.assertEquals("maxHeartbeatLatencyMs should be 120.0",
                120.0, snapshot.getMaxHeartbeatLatencyMs(), 0.01);

        // Verify node metrics
        Map<String, MetricsSnapshot.NodeMetrics> nodeMetrics = snapshot.getNodeMetrics();
        Assert.assertNotNull("nodeMetrics should not be null", nodeMetrics);
        Assert.assertEquals("should have 2 nodes", 2, nodeMetrics.size());

        MetricsSnapshot.NodeMetrics node1 = nodeMetrics.get("node1:12000");
        Assert.assertNotNull("node1 should exist", node1);
        Assert.assertEquals("node1 totalHeartbeats", 100L, node1.getTotalHeartbeats());
        Assert.assertEquals("node1 successfulHeartbeats", 98L, node1.getSuccessfulHeartbeats());
        Assert.assertEquals("node1 failedHeartbeats", 2L, node1.getFailedHeartbeats());
        Assert.assertEquals("node1 avgLatencyMs", 15.5, node1.getAvgLatencyMs(), 0.01);
        Assert.assertEquals("node1 maxLatencyMs", 120.0, node1.getMaxLatencyMs(), 0.01);
    }

    /**
     * 测试解析空数据时的降级行为。
     */
    public void testParseMetricsResponseEmptyData() throws Exception {
        String json = "{"
                + "  \"timestamp\": 1234567890123,"
                + "  \"cluster\": { \"totalNodes\": 0 },"
                + "  \"scheduling\": {},"
                + "  \"applications\": {}"
                + "}";

        MetricsStore store = new MetricsStore(100);
        MetricsDatabase database = null;
        MetricsCollector collector = new MetricsCollector("http://localhost:28080", 5000, store, database);

        JsonNode root = MAPPER.readTree(json);
        java.lang.reflect.Method parseMethod = MetricsCollector.class.getDeclaredMethod(
                "parseMetricsResponse", JsonNode.class);
        parseMethod.setAccessible(true);
        MetricsSnapshot snapshot = (MetricsSnapshot) parseMethod.invoke(collector, root);

        Assert.assertNotNull("snapshot should not be null", snapshot);
        Assert.assertEquals("timestamp should match", 1234567890123L, snapshot.getTimestamp());
        Assert.assertEquals("totalNodes should be 0", 0, snapshot.getTotalNodes());
        Assert.assertEquals("activeApplications should be 0", 0, snapshot.getActiveApplications());

        // Node metrics should be null when no nodeHeartbeatMetrics in JSON
        Assert.assertNull("nodeMetrics should be null", snapshot.getNodeMetrics());
    }

    /**
     * 测试 start/stop 生命周期。
     */
    public void testLifecycle() {
        MetricsStore store = new MetricsStore(100);
        MetricsCollector collector = new MetricsCollector("http://localhost:28080", 5000, store, null);

        Assert.assertFalse("should not be running initially", collector.isRunning());

        collector.start();
        Assert.assertTrue("should be running after start", collector.isRunning());

        collector.stop();
        Assert.assertFalse("should not be running after stop", collector.isRunning());
    }
}
