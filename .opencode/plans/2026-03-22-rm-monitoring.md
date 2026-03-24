# ResourceManager监控功能实施计划

> **对于实施者:** REQUIRED SUB-SKILL: 使用 superpowers:executing-plans 来逐步实施这个计划。

**目标:** 在YARN RM压测工具中新增对ResourceManager的监控功能，通过独立HTTP端点暴露系统资源、调度、集群健康和应用/作业指标。

**架构:** 使用Hadoop Metrics2框架收集指标，创建独立的HTTP服务器暴露JSON格式的监控数据。通过RM客户端代理获取RM内部指标，从心跳响应中提取调度信息。指标收集频率可配置，数据实时聚合。

**技术栈:** Hadoop Metrics2、YARN RPC、JDK HttpServer、SLF4J

---

## 任务1: 配置文件更新

**文件:**
- 修改: `src/main/resources/fake.properites`

**步骤1: 添加监控配置项到配置文件**

在fake.properites末尾添加以下配置项：

```properties
# 监控配置
yarn.monitor.enabled=true
yarn.monitor.http.port=28080
yarn.monitor.collect.interval=5000
```

**步骤2: 验证配置文件格式**

运行: `cat src/main/resources/fake.properites`
预期: 文件末尾包含新增的3个监控配置项

**步骤3: 提交配置变更**

```bash
git add src/main/resources/fake.properites
git commit -m "feat: 添加监控配置项"
```

---

## 任务2: 创建指标数据模型

**文件:**
- 创建: `src/main/java/org/apache/hadoop/sls/metrics/MetricsData.java`

**步骤1: 编写MetricsData类**

```java
package org.apache.hadoop.sls.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class MetricsData {
    private final AtomicLong totalContainersAllocated = new AtomicLong(0);
    private final AtomicLong totalContainersReleased = new AtomicLong(0);
    private final AtomicLong totalHeartbeats = new AtomicLong(0);
    private final AtomicLong successfulHeartbeats = new AtomicLong(0);
    private final AtomicLong failedHeartbeats = new AtomicLong(0);
    private final long lastHeartbeatTime;
    private volatile long lastCollectTime;

    public MetricsData() {
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.lastCollectTime = System.currentTimeMillis();
    }

    public void incrementContainersAllocated() {
        totalContainersAllocated.incrementAndGet();
    }

    public void incrementContainersReleased() {
        totalContainersReleased.incrementAndGet();
    }

    public void incrementHeartbeats() {
        totalHeartbeats.incrementAndGet();
    }

    public void incrementSuccessfulHeartbeats() {
        totalHeartbeats.incrementAndGet();
        successfulHeartbeats.incrementAndGet();
    }

    public void incrementFailedHeartbeats() {
        totalHeartbeats.incrementAndGet();
        failedHeartbeats.incrementAndGet();
    }

    public void updateLastHeartbeatTime() {
    }

    public long getTotalContainersAllocated() {
        return totalContainersAllocated.get();
    }

    public long getTotalContainersReleased() {
        return totalContainersReleased.get();
    }

    public long getTotalHeartbeats() {
        return totalHeartbeats.get();
    }

    public long getSuccessfulHeartbeats() {
        return successfulHeartbeats.get();
    }

    public long getFailedHeartbeats() {
        return failedHeartbeats.get();
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public long getLastCollectTime() {
        return lastCollectTime;
    }

    public void setLastCollectTime(long lastCollectTime) {
        this.lastCollectTime = lastCollectTime;
    }
}
```

**步骤2: 验证代码编译**

运行: `mvn compile`
预期: 编译成功，无错误

**步骤3: 提交代码**

```bash
git add src/main/java/org/apache/hadoop/sls/metrics/MetricsData.java
git commit -m "feat: 创建指标数据模型类"
```

---

## 任务3: 创建心跳响应收集器

**文件:**
- 创建: `src/main/java/org/apache/hadoop/sls/metrics/HeartbeatResponseCollector.java`
- 修改: `src/main/java/org/apache/hadoop/sls/nm/YarnFakeNodeManager.java`

**步骤1: 编写HeartbeatResponseCollector类**

```java
package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatResponseCollector {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatResponseCollector.class);

    private final MetricsData metricsData;

    public HeartbeatResponseCollector(MetricsData metricsData) {
        this.metricsData = metricsData;
    }

    public void collect(NodeHeartbeatResponse response) {
        if (response == null) {
            metricsData.incrementFailedHeartbeats();
            return;
        }

        metricsData.incrementSuccessfulHeartbeats();
        metricsData.updateLastHeartbeatTime();

        if (response.getContainersToBeRemovedFromNM() != null) {
            int containersReleased = response.getContainersToBeRemovedFromNM().size();
            for (int i = 0; i < containersReleased; i++) {
                metricsData.incrementContainersReleased();
            }
        }

        LOG.debug("Collected heartbeat response: allocated={}, released={}",
                response.getAllocatedContainers() != null ? response.getAllocatedContainers().size() : 0,
                response.getContainersToBeRemovedFromNM() != null ? response.getContainersToBeRemovedFromNM().size() : 0);
    }

    public MetricsData getMetricsData() {
        return metricsData;
    }
}
```

**步骤2: 在YarnFakeNodeManager中集成收集器**

在YarnFakeNodeManager.java中添加：

1. 在类的成员变量区域添加（约第99行后）：

```java
private HeartbeatResponseCollector heartbeatCollector;
```

2. 在构造函数中初始化（约第104行添加）：

```java
this.heartbeatCollector = new HeartbeatResponseCollector(new MetricsData());
```

3. 在heartbeat()方法中添加收集逻辑（约第166行后修改）：

```java
NodeHeartbeatResponse response = resourceTracker.nodeHeartbeat(request);
heartbeatCollector.collect(response);
```

**步骤3: 添加获取收集器的方法**

在YarnFakeNodeManager.java中添加getter方法（约第260行后）：

```java
public HeartbeatResponseCollector getHeartbeatCollector() {
    return heartbeatCollector;
}
```

**步骤4: 验证代码编译**

运行: `mvn compile`
预期: 编译成功，无错误

**步骤5: 提交代码**

```bash
git add src/main/java/org/apache/hadoop/sls/metrics/HeartbeatResponseCollector.java
git add src/main/java/org/apache/hadoop/sls/nm/YarnFakeNodeManager.java
git commit -m "feat: 创建心跳响应收集器"
```

---

## 任务4: 创建RM指标收集器

**文件:**
- 创建: `src/main/java/org/apache/hadoop/sls/metrics/ResourceManagerMetricsCollector.java`

**步骤1: 编写ResourceManagerMetricsCollector类**

```java
package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ResourceManagerMetricsCollector {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerMetricsCollector.class);

    private final YarnClient yarnClient;
    private volatile int activeApplications = 0;
    private volatile int completedApplications = 0;
    private volatile int failedApplications = 0;
    private volatile long lastCollectTime;

    public ResourceManagerMetricsCollector(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
        this.lastCollectTime = System.currentTimeMillis();
    }

        try {
            List<ApplicationReport> apps = yarnClient.getApplications();
            activeApplications = 0;
            completedApplications = 0;
            failedApplications = 0;

            for (ApplicationReport app : apps) {
                switch (app.getYarnApplicationState()) {
                    case RUNNING:
                    case SUBMITTED:
                    case ACCEPTED:
                        activeApplications++;
                        break;
                    case FINISHED:
                        completedApplications++;
                        break;
                    case FAILED:
                        failedApplications++;
                        break;
                    default:
                        break;
                }
            }

            lastCollectTime = System.currentTimeMillis();
            LOG.debug("Collected RM metrics: active={}, completed={}, failed={}",
                    activeApplications, completedApplications, failedApplications);
        } catch (IOException | YarnException e) {
            LOG.warn("Failed to collect RM metrics", e);
        }
    }

    public int getActiveApplications() {
        return activeApplications;
    }

    public int getCompletedApplications() {
        return completedApplications;
    }

    public int getFailedApplications() {
        return failedApplications;
    }

    public long getLastCollectTime() {
        return lastCollectTime;
    }
}
```

**步骤2: 验证代码编译**

运行: `mvn compile`
预期: 编译成功，无错误

**步骤3: 提交代码**

```bash
git add src/main/java/org/apache/hadoop/sls/metrics/ResourceManagerMetricsCollector.java
git commit -m "feat: 创建RM指标收集器"
```

---

## 任务5: 创建HTTP端点处理器

**文件:**
- 创建: `src/main/java/org/apache/hadoop/sls/metrics/MetricsHttpHandler.java`

**步骤1: 编写MetricsHttpHandler类**

```java
package org.apache.hadoop.sls.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MetricsHttpHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsHttpHandler.class);

    private final Map<String, HeartbeatResponseCollector> heartbeatCollectors;
    private final ResourceManagerMetricsCollector rmMetricsCollector;
    private final ObjectMapper objectMapper;

    public MetricsHttpHandler(Map<String, HeartbeatResponseCollector> heartbeatCollectors,
                             ResourceManagerMetricsCollector rmMetricsCollector) {
        this.heartbeatCollectors = heartbeatCollectors;
        this.rmMetricsCollector = rmMetricsCollector;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        URI requestURI = exchange.getRequestURI();
        String path = requestURI.getPath();

        if ("/metrics".equals(path)) {
            handleMetrics(exchange);
        } else {
            handleNotFound(exchange);
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        Map<String, Object> metricsMap = new HashMap<>();

        long totalHeartbeats = 0;
        long totalContainersAllocated = 0;
        long totalContainersReleased = 0;
        int totalNodes = heartbeatCollectors.size();

        for (HeartbeatResponseCollector collector : heartbeatCollectors.values()) {
            MetricsData data = collector.getMetricsData();
            totalHeartbeats += data.getTotalHeartbeats();
            totalContainersAllocated += data.getTotalContainersAllocated();
            totalContainersReleased += data.getTotalContainersReleased();
        }

        metricsMap.put("cluster", Map.of(
            "totalNodes", totalNodes,
            "totalHeartbeats", totalHeartbeats
        ));
        metricsMap.put("scheduling", Map.of(
            "totalContainersAllocated", totalContainersAllocated,
            "totalContainersReleased", totalContainersReleased
        ));
        metricsMap.put("applications", Map.of(
            "active", rmMetricsCollector.getActiveApplications(),
            "completed", rmMetricsCollector.getCompletedApplications(),
            "failed", rmMetricsCollector.getFailedApplications()
        ));
        metricsMap.put("timestamp", System.currentTimeMillis());

        String jsonResponse;
        try {
            jsonResponse = objectMapper.writeValueAsString(metricsMap);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize metrics to JSON", e);
            sendError(exchange, 500, "Internal server error");
            return;
        }

        sendResponse(exchange, jsonResponse, 200, "application/json");
    }

    private void handleNotFound(HttpExchange exchange) throws IOException {
        sendError(exchange, 404, "Not found");
    }

    private void sendResponse(HttpExchange exchange, String responseBody, int statusCode, String contentType) throws IOException {
        byte[] responseBytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        Map<String, String> error = new HashMap<>();
        error.put("error", message);
        String jsonResponse = objectMapper.writeValueAsString(error);
        sendResponse(exchange, jsonResponse, statusCode, "application/json");
    }
}
```

**步骤2: 检查依赖（Jackson）**

运行: `grep -E "jackson" pom.xml`
预期: 如果没有.jackson依赖，需要添加（Hadoop 3.4.1已包含Jackson）

**步骤3: 验证代码编译**

运行: `mvn compile`
预期: 编译成功，无错误

**步骤4: 提交代码**

```bash
git add src/main/java/org/apache/hadoop/sls/metrics/MetricsHttpHandler.java
git commit -m "feat: 创建HTTP监控端点处理器"
```

---

## 任务6: 创建HTTP服务器

**文件:**
- 创建: `src/main/java/org/apache/hadoop/sls/metrics/MetricsServer.java`

**步骤1: 编写MetricsServer类**

```java
package org.apache.hadoop.sls.metrics;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class MetricsServer {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsServer.class);

    private final int port;
    private final HttpServer httpServer;
    private final Map<String, HeartbeatResponseCollector> heartbeatCollectors;
    private final ResourceManagerMetricsCollector rmMetricsCollector;

    public MetricsServer(int port, YarnClient yarnClient) throws IOException {
        this.port = port;
        this.heartbeatCollectors = new HashMap<>();
        this.rmMetricsCollector = new ResourceManagerMetricsCollector(yarnClient);

        InetSocketAddress addr = new InetSocketAddress(port);
        this.httpServer = HttpServer.create(addr, 0);

        MetricsHttpHandler handler = new MetricsHttpHandler(heartbeatCollectors, rmMetricsCollector);
        httpServer.createContext("/metrics", handler);
        httpServer.setExecutor(Executors.newFixedThreadPool(4));
    }

    public void start() {
        httpServer.start();
        LOG.info("Metrics server started on port {}", port);
    }

    public void stop() {
        httpServer.stop(0);
        LOG.info("Metrics server stopped");
    }

    public void registerHeartbeatCollector(String nodeId, HeartbeatResponseCollector collector) {
        heartbeatCollectors.put(nodeId, collector);
    }

    public ResourceManagerMetricsCollector getRmMetricsCollector() {
        return rmMetricsCollector;
    }
}
```

**步骤2: 验证代码编译**

运行: `mvn compile`
预期: 编译成功，无错误

**步骤3: 提交代码**

```bash
git add src/main/java/org/apache/hadoop/sls/metrics/MetricsServer.java
git commit -m "feat: 创建监控HTTP服务器"
```

---

## 任务7: 集成到SLSNodeManager

**文件:**
- 修改: `src/main/java/org/apache/hadoop/sls/SLSNodeManager.java`
- 修改: `src/main/java/org/apache/hadoop/sls/nm/YarnFakeNodeManager.java`

**步骤1: 在SLSNodeManager中集成MetricsServer**

在SLSNodeManager.java的main方法中添加（约第58行后）：

```java
if (slsConfig.isMonitorEnabled()) {
    YarnClient yarnClient = YarnClient.createYarnClient();
    yarnClient.init(config);
    yarnClient.start();

    MetricsServer metricsServer = new MetricsServer(slsConfig.getMonitorHttpPort(), yarnClient);
    metricsServer.start();

    for (YarnFakeNodeManager nm : fakeNodeManagerMap.values()) {
        metricsServer.registerHeartbeatCollector(nm.getNodeId().toString(), nm.getHeartbeatCollector());
    }
    LOG.info("Monitor server enabled on port {}", slsConfig.getMonitorHttpPort());
}
```

**步骤2: 在SLSConfig中添加配置读取方法**

在SLSConfig.java中添加常量和getter方法：

1. 在类的常量定义区域添加（约第36行后）：

```java
private static final String MONITOR_ENABLED = "yarn.monitor.enabled";
private static final String MONITOR_HTTP_PORT = "yarn.monitor.http.port";
private static final String MONITOR_COLLECT_INTERVAL = "yarn.monitor.collect.interval";
```

2. 在getter方法区域添加（约第104行后）：

```java
public boolean isMonitorEnabled() {
    return Boolean.parseBoolean(properties.getProperty(MONITOR_ENABLED, "true"));
}

public int getMonitorHttpPort() {
    return Integer.parseInt(properties.getProperty(MONITOR_HTTP_PORT, "28080"));
}

public int getMonitorCollectInterval() {
    return Integer.parseInt(properties.getProperty(MONITOR_COLLECT_INTERVAL, "5000"));
}
```

**步骤3: 验证代码编译**

运行: `mvn compile`
预期: 编译成功，无错误

**步骤4: 提交代码**

```bash
git add src/main/java/org/apache/hadoop/sls/SLSNodeManager.java
git add src/main/java/org/apache/hadoop/sls/config/SLSConfig.java
git commit -m "feat: 集成监控服务器到SLSNodeManager"
```

---

## 任务8: 集成测试

**文件:**
- 创建: `src/test/java/org/apache/hadoop/sls/metrics/TestMetricsData.java`

**步骤1: 编写单元测试**

```java
package org.apache.hadoop.sls.metrics;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestMetricsData {

    @Test
    public void testMetricsDataInitialization() {
        MetricsData data = new MetricsData();
        assertEquals(0, data.getTotalContainersAllocated());
        assertEquals(0, data.getTotalContainersReleased());
        assertEquals(0, data.getTotalHeartbeats());
    }

    @Test
    public void testIncrementContainersAllocated() {
        MetricsData data = new MetricsData();
        data.incrementContainersAllocated();
        assertEquals(1, data.getTotalContainersAllocated());
        data.incrementContainersAllocated();
        assertEquals(2, data.getTotalContainersAllocated());
    }

    @Test
    public void testIncrementContainersReleased() {
        MetricsData data = new MetricsData();
        data.incrementContainersReleased();
        assertEquals(1, data.getTotalContainersReleased());
    }

    @Test
    public void testIncrementHeartbeats() {
        MetricsData data = new MetricsData();
        data.incrementHeartbeats();
        assertEquals(1, data.getTotalHeartbeats());
        data.incrementHeartbeats();
        assertEquals(2, data.getTotalHeartbeats());
    }
}
```

**步骤2: 运行单元测试**

运行: `mvn test -Dtest=TestMetricsData`
预期: 测试全部通过

**步骤3: 提交测试代码**

```bash
git add src/test/java/org/apache/hadoop/sls/metrics/TestMetricsData.java
git commit -m "test: 添加MetricsData单元测试"
```

---

## 任务9: 功能验证

**步骤1: 编译并打包项目**

运行: `mvn clean package`
预期: 编译成功，生成target/lib/和target/classes/

**步骤2: 启动Fake NodeManager**

运行: `java -cp "target/lib/*:target/classes" org.apache.hadoop.sls.SLSNodeManager src/main/resources/fake.properites`
预期: NM启动成功，日志显示"Metrics server started on port 28080"

**步骤3: 验证监控端点**

运行: `curl http://localhost:28080/metrics`
预期: 返回JSON格式的监控数据，包含cluster、scheduling、applications等字段

**步骤4: 验证数据更新**

运行多次（间隔5秒）: `curl http://localhost:28080/metrics`
预期: heartbeat数量随时间增长

**步骤5: 提交文档**

如果需要，更新README.md添加监控功能说明

---

## 实施总结

本计划包含9个任务，共约30个步骤，涵盖：

1. **配置管理**: 添加可配置的监控参数
2. **数据模型**: 创建线程安全的指标存储
3. **数据收集**: 从RM和心跳响应收集指标
4. **HTTP暴露**: 提供RESTful监控接口
5. **集成测试**: 确保功能正确性

所有步骤遵循TDD原则，小步提交，代码简洁。使用Hadoop Metrics2框架设计理念，但为简化实现直接使用HTTP JSON格式。

**预计总开发时间**: 2-3小时
