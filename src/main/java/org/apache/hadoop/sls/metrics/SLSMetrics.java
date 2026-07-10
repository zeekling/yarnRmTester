package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;

/**
 * SLSMetrics 监控服务主入口。
 * <p>
 * 启动流程：
 * <ol>
 *   <li>加载 fake.properites 配置</li>
 *   <li>初始化 YarnClient 并连接 RM</li>
 *   <li>初始化内存存储和 SQLite 持久化</li>
 *   <li>启动定期采集和图表生成</li>
 *   <li>注册关闭钩子，等待退出信号</li>
 * </ol>
 * </p>
 *
 * 用法：java org.apache.hadoop.sls.metrics.SLSMetrics [config_dir]
 */
public class SLSMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(SLSMetrics.class);

    private SLSMetrics() {
        // 工具类，禁止实例化
    }

    public static void main(String[] args) throws Exception {
        String configPath = "src/main/resources";
        if (args.length > 0) {
            configPath = args[0];
        }
        LOG.info("SLSMetrics starting with config path: {}", configPath);

        // 加载 fake.properites 配置
        SLSConfig slsConfig = new SLSConfig(configPath + File.separator + "fake.properites");

        // 读取所有配置项
        long collectInterval = Long.parseLong(
                slsConfig.getProperty("yarn.metrics.collect.interval", "5000"));
        long chartInterval = Long.parseLong(
                slsConfig.getProperty("yarn.metrics.chart.interval", "30000"));
        int storeSize = Integer.parseInt(
                slsConfig.getProperty("yarn.metrics.store.size", "3600"));
        String outputDir = slsConfig.getProperty(
                "yarn.metrics.output.dir", "target/metrics");
        String serverUrl = slsConfig.getProperty(
                "yarn.metrics.server.url", "http://localhost:28080/metrics");
        String dbPath = slsConfig.getProperty(
                "yarn.metrics.db.path", "target/metrics/metrics.db");
        int batchSize = Integer.parseInt(
                slsConfig.getProperty("yarn.metrics.db.batch.size", "10"));
        int retentionDays = Integer.parseInt(
                slsConfig.getProperty("yarn.metrics.db.retention.days", "7"));
        long cleanupInterval = Long.parseLong(
                slsConfig.getProperty("yarn.metrics.db.cleanup.interval", "3600000"));

        // 初始化 YarnClient，加载 site 配置
        YarnConfiguration yarnConfig = new YarnConfiguration();
        yarnConfig.addResource(new Path(configPath + File.separator + "core-site.xml"));
        yarnConfig.addResource(new Path(configPath + File.separator + "hdfs-site.xml"));
        yarnConfig.addResource(new Path(configPath + File.separator + "yarn-site.xml"));

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfig);
        yarnClient.start();
        LOG.info("YarnClient initialized and started");

        // 初始化存储层
        MetricsStore store = new MetricsStore(storeSize);
        MetricsDatabase database = null;
        MetricsCollector collector = null;
        ChartGenerator chartGenerator = null;

        try {
            database = new MetricsDatabase(
                    dbPath, batchSize, retentionDays, cleanupInterval);

            // 启动采集器和图表生成器
            collector = new MetricsCollector(
                    serverUrl, yarnClient, store, database, collectInterval);
            chartGenerator = new ChartGenerator(
                    outputDir, store, chartInterval);
        } catch (RuntimeException e) {
            // 数据库初始化失败时清理已启动的 YARN Client
            yarnClient.stop();
            throw e;
        }

        // JVM 关闭钩子
        final MetricsDatabase db = database;
        final MetricsCollector col = collector;
        final ChartGenerator cg = chartGenerator;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered, releasing resources...");
            try {
                if (col != null) col.close();
            } catch (Exception e) {
                LOG.warn("Error closing MetricsCollector", e);
            }
            try {
                if (cg != null) cg.close();
            } catch (Exception e) {
                LOG.warn("Error closing ChartGenerator", e);
            }
            try {
                if (db != null) db.close();
            } catch (Exception e) {
                LOG.warn("Error closing MetricsDatabase", e);
            }
            yarnClient.stop();
            LOG.info("SLSMetrics shutdown complete");
        }));

        LOG.info("SLSMetrics started successfully. " +
                        "collectInterval={}ms, chartInterval={}ms, storeSize={}, outputDir={}",
                collectInterval, chartInterval, storeSize, outputDir);

        // 保持主线程存活（带超时防止永远卡住）
        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
}
