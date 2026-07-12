package org.apache.hadoop.sls.metrics;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.sls.config.SLSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * SLSMetrics 独立监控服务主入口。
 *
 * 双数据源架构：
 * 1. JmxMetricsCollector — 从 RM JMX 端点采集集群资源、队列调度、应用状态
 * 2. MetricsCollector（可选）— 从本地 MetricsServer 采集 Fake NM 心跳数据
 *
 * SLSMetrics 拥有一个共享调度器，协调两个采集器并合并结果。
 *
 * 启动流程：
 * 1. 读取配置
 * 2. 初始化 MetricsStore + MetricsDatabase
 * 3. 创建 JmxMetricsCollector 和（可选的）MetricsCollector
 * 4. 启动共享调度器定时采集
 * 5. 启动 HTTP Server（静态文件 + REST API）
 *
 * 启动方式：
 * java -cp "target/lib/*;target/classes" org.apache.hadoop.sls.metrics.SLSMetrics [config_dir]
 */
public class SLSMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(SLSMetrics.class);

    private final HttpServer httpServer;
    private final JmxMetricsCollector jmxCollector;
    private final MetricsCollector nmCollector;
    private final boolean nmEnabled;
    private final MetricsStore store;
    private MetricsDatabase database;
    private final ScheduledExecutorService scheduler;
    private final long collectIntervalMs;
    private volatile boolean running = false;

    public SLSMetrics(SLSConfig config) throws IOException {
        // ========== 读取配置 ==========
        String jmxUrl = config.getProperty("yarn.metrics.server.url",
                "http://localhost:8088/jmx?qry=Hadoop:*");
        String nmUrl = config.getProperty("yarn.metrics.nm.url",
                "http://localhost:28080");
        this.nmEnabled = Boolean.parseBoolean(
                config.getProperty("yarn.metrics.nm.collect.enabled", "false"));
        int webPort = Integer.parseInt(
                config.getProperty("yarn.metrics.web.port", "28081"));
        this.collectIntervalMs = Long.parseLong(
                config.getProperty("yarn.metrics.collect.interval", "5000"));
        int storeSize = Integer.parseInt(
                config.getProperty("yarn.metrics.store.size", "3600"));
        String dbPath = config.getProperty("yarn.metrics.db.path",
                "target/metrics/metrics.db");
        int batchSize = Integer.parseInt(
                config.getProperty("yarn.metrics.db.batch.size", "10"));
        int retentionDays = Integer.parseInt(
                config.getProperty("yarn.metrics.db.retention.days", "7"));
        long cleanupInterval = Long.parseLong(
                config.getProperty("yarn.metrics.db.cleanup.interval", "3600000"));

        // ========== 初始化 MetricsStore ==========
        this.store = new MetricsStore(storeSize);

        // ========== 初始化 MetricsDatabase ==========
        this.database = new MetricsDatabase(dbPath, batchSize, retentionDays);
        try {
            database.init();
        } catch (Exception e) {
            LOG.warn("Failed to initialize MetricsDatabase (SQLite), " +
                    "metrics will not be persisted: {}", e.getMessage());
            this.database = null;
        }

        // ========== 启动定时清理（独立线程） ==========
        if (cleanupInterval > 0) {
            Thread cleanupThread = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(cleanupInterval);
                        if (database != null) {
                            database.cleanup();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        LOG.error("Unexpected error in metrics DB cleanup thread, continuing", e);
                    }
                }
            }, "metrics-db-cleanup");
            cleanupThread.setDaemon(true);
            cleanupThread.start();
        }

        // ========== 创建采集器 ==========
        // JMX 采集器：从 RM 获取集群/队列/应用指标
        this.jmxCollector = new JmxMetricsCollector(jmxUrl);

        // NM 采集器（可选）：从本地 MetricsServer 获取 Fake NM 心跳数据
        // 注意：NM 采集器的内部调度器不会被使用，SLSMetrics 拥有共享调度器
        if (nmEnabled) {
            this.nmCollector = new MetricsCollector(nmUrl, collectIntervalMs,
                    store, database, true);
        } else {
            this.nmCollector = null;
        }

        // ========== 创建共享调度器（单线程守护线程） ==========
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sls-metrics-collector");
            t.setDaemon(true);
            return t;
        });

        // ========== 初始化 HTTP Server ==========
        this.httpServer = HttpServer.create(new InetSocketAddress(webPort), 0);
        MetricsApiHandler handler = new MetricsApiHandler(store, database);
        httpServer.createContext("/", handler);
        httpServer.setExecutor(Executors.newFixedThreadPool(4));

        LOG.info("SLSMetrics configured: jmxUrl={}, nmUrl={}, nmEnabled={}, " +
                        "webPort={}, collectInterval={}ms, storeSize={}, dbPath={}",
                jmxUrl, nmUrl, nmEnabled, webPort, collectIntervalMs, storeSize, dbPath);
    }

    public void start() {
        if (running) {
            LOG.warn("SLSMetrics is already running");
            return;
        }
        running = true;

        // 启动共享调度器：定时执行双源采集 + 合并 + 存储
        scheduler.scheduleAtFixedRate(() -> {
            try {
                collectAndMerge();
            } catch (Exception e) {
                LOG.warn("Error during scheduled collection", e);
            }
        }, 0, collectIntervalMs, TimeUnit.MILLISECONDS);

        httpServer.start();
        LOG.info("SLSMetrics started on port {}", httpServer.getAddress());
    }

    /**
     * 双源采集并合并：JMX → NM → 合并 → 存储。
     */
    private void collectAndMerge() {
        // 1. JMX 采集（集群资源、队列调度、应用状态）
        MetricsSnapshot jmxSnapshot = jmxCollector.collectOnce();

        // 2. NM 采集（Fake NM 心跳数据），仅在开启时执行
        MetricsSnapshot nmSnapshot = null;
        if (nmEnabled && nmCollector != null) {
            nmSnapshot = nmCollector.collectOnce();
        }

        // 3. 确定基准快照并合并
        MetricsSnapshot result;
        if (jmxSnapshot != null) {
            result = jmxSnapshot;
            if (nmSnapshot != null) {
                mergeHeartbeatFields(result, nmSnapshot);
            }
        } else if (nmSnapshot != null) {
            // JMX 采集失败但 NM 有数据，使用 NM 快照
            result = nmSnapshot;
        } else {
            LOG.warn("Both JMX and NM collectors returned null, " +
                    "skipping collection cycle");
            return;
        }

        // 4. 存储 + 持久化
        store.add(result);
        if (database != null) {
            database.insertBatch(Collections.singletonList(result));
        }
    }

    /**
     * 将 NM 心跳快照中的心跳相关字段合并到目标快照中。
     * JMX 源不提供心跳数据，NM MetricsServer 是唯一心跳数据源。
     *
     * <p>语义说明：
     * <ul>
     *   <li><b>JMX 权威字段</b>：activeContainers、pendingContainers、reservedContainers、
     *       集群资源（totalMemoryMB 等）、队列调度数据——这些字段来自 RM 的 JMX，
     *       是权威的当前值，不应被 NM 数据覆盖。</li>
     *   <li><b>NM 提供字段</b>：心跳统计数据（成功/失败次数、延迟、吞吐量）、
     *       累计容器分配/释放计数、节点级指标——这些仅由 NM MetricsServer 提供。</li>
     * </ul>
     */
    private void mergeHeartbeatFields(MetricsSnapshot target, MetricsSnapshot source) {
        // 心跳统计：NM 是唯一数据源
        target.setSuccessfulHeartbeats(source.getSuccessfulHeartbeats());
        target.setFailedHeartbeats(source.getFailedHeartbeats());
        target.setHeartbeatSuccessRate(source.getHeartbeatSuccessRate());
        target.setAvgHeartbeatLatencyMs(source.getAvgHeartbeatLatencyMs());
        target.setMaxHeartbeatLatencyMs(source.getMaxHeartbeatLatencyMs());
        target.setHeartbeatThroughput(source.getHeartbeatThroughput());

        // 累计容器分配/释放计数（仅用于趋势分析，不是权威当前值）
        target.setTotalContainersAllocated(source.getTotalContainersAllocated());
        target.setTotalContainersReleased(source.getTotalContainersReleased());

        // ⚠️ 不覆盖 activeContainers：JMX AllocatedContainers 是权威当前值，
        //    NM 计算方式（totalAllocated - totalReleased）基于累计值，可能因漂移而不准确

        // 节点级指标
        if (source.getNodeMetrics() != null) {
            target.setNodeMetrics(new java.util.LinkedHashMap<>(source.getNodeMetrics()));
        }
    }

    public void stop() {
        LOG.info("Stopping SLSMetrics...");
        running = false;

        // 关闭共享调度器
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 关闭 NM 采集器内部调度器（即使未启动过也安全关闭）
        if (nmCollector != null) {
            nmCollector.stop();
        }

        httpServer.stop(0);
        if (database != null) {
            database.close();
        }
        LOG.info("SLSMetrics stopped");
    }

    public static void main(String[] args) throws Exception {
        String configPath = "src/main/resources";
        if (args.length > 0) {
            configPath = args[0];
        }

        SLSConfig config = new SLSConfig(configPath + File.separator + "fake.properites");
        SLSMetrics metricsApp = new SLSMetrics(config);
        metricsApp.start();

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered, stopping SLSMetrics...");
            metricsApp.stop();
        }));

        LOG.info("SLSMetrics started successfully. Waiting for shutdown...");
    }
}
