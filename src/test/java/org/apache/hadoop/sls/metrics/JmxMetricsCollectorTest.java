package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import junit.framework.Assert;

import java.util.Map;

/**
 * JmxMetricsCollector 单元测试。
 * 测试 JMX 响应 JSON 的解析逻辑（parseJmxResponse），不涉及真实的 HTTP 请求。
 */
public class JmxMetricsCollectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 测试 parseJmxResponse 能正确解析完整的 JMX 响应。
     * 包含 ClusterMetrics + root QueueMetrics + 子队列 QueueMetrics。
     */
    public void testParseJmxResponseFullData() throws Exception {
        // 构建完整的 JMX 响应 JSON
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode beans = MAPPER.createArrayNode();

        // ---- ClusterMetrics bean ----
        ObjectNode clusterBean = MAPPER.createObjectNode();
        clusterBean.put("name", "Hadoop:service=ResourceManager,name=ClusterMetrics");
        clusterBean.put("NumActiveNMs", 10);
        clusterBean.put("NumLostNMs", 2);
        clusterBean.put("NumUnhealthyNMs", 1);
        clusterBean.put("NumDecommissionedNMs", 1);
        clusterBean.put("NumRebootedNMs", 0);
        beans.add(clusterBean);

        // ---- root QueueMetrics bean ----
        ObjectNode rootQueueBean = MAPPER.createObjectNode();
        rootQueueBean.put("name", "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root");
        rootQueueBean.put("AllocatedMB", 51200L);
        rootQueueBean.put("AvailableMB", 102400L);
        rootQueueBean.put("AllocatedVCores", 100);
        rootQueueBean.put("AvailableVCores", 200);
        rootQueueBean.put("AllocatedContainers", 500L);
        rootQueueBean.put("PendingContainers", 10);
        rootQueueBean.put("ReservedContainers", 2);
        rootQueueBean.put("AppsPending", 3);
        rootQueueBean.put("ActiveApplications", 15);
        rootQueueBean.put("AppsRunning", 15);
        rootQueueBean.put("AppsCompleted", 200);
        rootQueueBean.put("AppsFailed", 5);
        rootQueueBean.put("AppsSubmitted", 300);
        beans.add(rootQueueBean);

        // ---- 子队列 QueueMetrics bean（root.default） ----
        ObjectNode subQueueBean = MAPPER.createObjectNode();
        subQueueBean.put("name", "Hadoop:service=ResourceManager,name=QueueMetrics,q0=root,q1=default");
        subQueueBean.put("AbsoluteCapacity", 0.5);
        subQueueBean.put("UsedCapacity", 0.3);
        subQueueBean.put("AppsPending", 2);
        subQueueBean.put("ActiveApplications", 10);
        subQueueBean.put("AppsRunning", 10);
        beans.add(subQueueBean);

        root.set("beans", beans);

        // 执行解析
        JmxMetricsCollector collector = new JmxMetricsCollector("http://localhost:8088/jmx");
        java.lang.reflect.Method parseMethod = JmxMetricsCollector.class.getDeclaredMethod(
                "parseJmxResponse", JsonNode.class);
        parseMethod.setAccessible(true);
        MetricsSnapshot snapshot = (MetricsSnapshot) parseMethod.invoke(collector, root);

        // ---- 验证 ClusterMetrics ----
        Assert.assertEquals("totalNodes should be 14 (10+2+1+1, without rebootedNMs)",
                14, snapshot.getTotalNodes());
        Assert.assertEquals("lostNodes should be 2", 2, snapshot.getLostNodes());
        Assert.assertEquals("unhealthyNodes should be 1", 1, snapshot.getUnhealthyNodes());
        Assert.assertEquals("decommissionedNodes should be 1", 1, snapshot.getDecommissionedNodes());

        // ---- 验证集群资源 ----
        Assert.assertEquals("totalMemoryMB should be 153600 (51200+102400)",
                153600L, snapshot.getTotalMemoryMB());
        Assert.assertEquals("totalVCores should be 300 (100+200)",
                300, snapshot.getTotalVCores());
        Assert.assertEquals("allocatedMemoryMB should be 51200",
                51200L, snapshot.getAllocatedMemoryMB());
        Assert.assertEquals("allocatedVCores should be 100",
                100, snapshot.getAllocatedVCores());
        Assert.assertEquals("availableMemoryMB should be 102400",
                102400L, snapshot.getAvailableMemoryMB());
        Assert.assertEquals("availableVCores should be 200",
                200, snapshot.getAvailableVCores());

        // ---- 验证集群利用率 ----
        Assert.assertEquals("clusterUtilizationPercent should be 33.3 (51200/153600*100)",
                33.3, snapshot.getClusterUtilizationPercent(), 0.1);

        // ---- 验证容器调度（JMX authoritative 当前值） ----
        Assert.assertEquals("activeContainers should be 500", 500L, snapshot.getActiveContainers());
        Assert.assertEquals("pendingContainers should be 10", 10, snapshot.getPendingContainers());
        Assert.assertEquals("reservedContainers should be 2", 2, snapshot.getReservedContainers());

        // ---- 验证应用状态 ----
        Assert.assertEquals("activeApplications should be 15", 15, snapshot.getActiveApplications());
        Assert.assertEquals("completedApplications should be 200", 200, snapshot.getCompletedApplications());
        Assert.assertEquals("failedApplications should be 5", 5, snapshot.getFailedApplications());
        Assert.assertEquals("submittedApplications should be 300", 300, snapshot.getSubmittedApplications());

        // ---- 验证队列指标 ----
        Map<String, MetricsSnapshot.QueueMetrics> queueMetrics = snapshot.getQueueMetrics();
        Assert.assertNotNull("queueMetrics should not be null", queueMetrics);
        Assert.assertEquals("should have 2 queues (root + default)", 2, queueMetrics.size());

        // root 队列
        MetricsSnapshot.QueueMetrics rootQM = queueMetrics.get("root");
        Assert.assertNotNull("root queue should exist", rootQM);
        Assert.assertEquals("root absoluteCapacity should be 100.0", 100.0, rootQM.getAbsoluteCapacity(), 0.01);
        Assert.assertEquals("root usedCapacity should be 33.3", 33.3, rootQM.getUsedCapacity(), 0.1);
        Assert.assertEquals("root pendingApps should be 3", 3, rootQM.getPendingApps());
        Assert.assertEquals("root activeApps should be 15", 15, rootQM.getActiveApps());

        // 子队列 default
        MetricsSnapshot.QueueMetrics defaultQM = queueMetrics.get("root.default");
        Assert.assertNotNull("root.default queue should exist", defaultQM);
        Assert.assertEquals("default absoluteCapacity should be 50.0", 50.0, defaultQM.getAbsoluteCapacity(), 0.01);
        Assert.assertEquals("default usedCapacity should be 30.0", 30.0, defaultQM.getUsedCapacity(), 0.1);
        Assert.assertEquals("default pendingApps should be 2", 2, defaultQM.getPendingApps());
        Assert.assertEquals("default activeApps should be 10", 10, defaultQM.getActiveApps());
    }

    /**
     * 测试 parseJmxResponse 在空 beans 数组时的降级行为。
     */
    public void testParseJmxResponseNoBeans() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.set("beans", MAPPER.createArrayNode());

        JmxMetricsCollector collector = new JmxMetricsCollector("http://localhost:8088/jmx");
        java.lang.reflect.Method parseMethod = JmxMetricsCollector.class.getDeclaredMethod(
                "parseJmxResponse", JsonNode.class);
        parseMethod.setAccessible(true);
        MetricsSnapshot snapshot = (MetricsSnapshot) parseMethod.invoke(collector, root);

        Assert.assertNotNull("snapshot should not be null", snapshot);
        Assert.assertEquals("totalNodes should be 0", 0, snapshot.getTotalNodes());
        Assert.assertEquals("lostNodes should be 0", 0, snapshot.getLostNodes());
        Assert.assertEquals("unhealthyNodes should be 0", 0, snapshot.getUnhealthyNodes());
        Assert.assertEquals("decommissionedNodes should be 0", 0, snapshot.getDecommissionedNodes());
        Assert.assertEquals("totalMemoryMB should be 0", 0L, snapshot.getTotalMemoryMB());
        Assert.assertEquals("activeContainers should be 0", 0L, snapshot.getActiveContainers());
        Assert.assertEquals("activeApplications should be 0", 0, snapshot.getActiveApplications());
        Assert.assertNull("queueMetrics should be null", snapshot.getQueueMetrics());
    }

    /**
     * 测试 parseJmxResponse 在 beans 为 null 时的降级行为。
     */
    public void testParseJmxResponseNullBeans() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        // 不设置 beans 字段

        JmxMetricsCollector collector = new JmxMetricsCollector("http://localhost:8088/jmx");
        java.lang.reflect.Method parseMethod = JmxMetricsCollector.class.getDeclaredMethod(
                "parseJmxResponse", JsonNode.class);
        parseMethod.setAccessible(true);
        MetricsSnapshot snapshot = (MetricsSnapshot) parseMethod.invoke(collector, root);

        Assert.assertNotNull("snapshot should not be null", snapshot);
        Assert.assertEquals("totalNodes should be default 0", 0, snapshot.getTotalNodes());
    }

    /**
     * 测试只有 ClusterMetrics 没有 QueueMetrics 时的行为。
     */
    public void testParseJmxResponseClusterOnly() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode beans = MAPPER.createArrayNode();

        ObjectNode clusterBean = MAPPER.createObjectNode();
        clusterBean.put("name", "Hadoop:service=ResourceManager,name=ClusterMetrics");
        clusterBean.put("NumActiveNMs", 5);
        clusterBean.put("NumLostNMs", 0);
        clusterBean.put("NumUnhealthyNMs", 0);
        clusterBean.put("NumDecommissionedNMs", 0);
        beans.add(clusterBean);

        root.set("beans", beans);

        JmxMetricsCollector collector = new JmxMetricsCollector("http://localhost:8088/jmx");
        java.lang.reflect.Method parseMethod = JmxMetricsCollector.class.getDeclaredMethod(
                "parseJmxResponse", JsonNode.class);
        parseMethod.setAccessible(true);
        MetricsSnapshot snapshot = (MetricsSnapshot) parseMethod.invoke(collector, root);

        Assert.assertEquals("totalNodes should be 5", 5, snapshot.getTotalNodes());
        // 没有 QueueMetrics 时，资源字段应为 0
        Assert.assertEquals("totalMemoryMB should be 0 without QueueMetrics",
                0L, snapshot.getTotalMemoryMB());
        Assert.assertEquals("clusterUtilizationPercent should be 0 without resources",
                0.0, snapshot.getClusterUtilizationPercent(), 0.01);
        Assert.assertNull("queueMetrics should be null", snapshot.getQueueMetrics());
    }
}
