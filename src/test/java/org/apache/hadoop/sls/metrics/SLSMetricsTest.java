package org.apache.hadoop.sls.metrics;

import junit.framework.Assert;
import org.apache.hadoop.sls.config.SLSConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

/**
 * SLSMetrics 单元测试。
 * 测试构造器初始化和生命周期管理。
 * 不涉及真实的 RM JMX 连接（NM 采集已禁用）。
 */
public class SLSMetricsTest {

    /**
     * 测试 SLSMetrics 构造器能正确初始化并正常关闭。
     * 使用临时配置文件和临时目录。
     */
    public void testConstructorAndLifecycle() throws Exception {
        // 创建临时目录用于数据库
        File tempDir = new File("target/test-output/slsmetrics-test");
        tempDir.mkdirs();
        File dbFile = new File(tempDir, "test-metrics.db");
        if (dbFile.exists()) {
            dbFile.delete();
        }

        // 创建临时配置文件
        File configFile = File.createTempFile("slsmetrics-test-", ".properties");
        configFile.deleteOnExit();

        Properties props = new Properties();
        // 使用端口 0（操作系统分配临时端口）
        props.setProperty("yarn.metrics.web.port", "0");
        // 数据库路径
        props.setProperty("yarn.metrics.db.path", dbFile.getAbsolutePath());
        // 禁用 NM 采集
        props.setProperty("yarn.metrics.nm.collect.enabled", "false");
        // 禁用清理线程
        props.setProperty("yarn.metrics.db.cleanup.interval", "0");
        // 其他必要配置
        props.setProperty("yarn.metrics.collect.interval", "5000");
        props.setProperty("yarn.metrics.store.size", "100");
        props.setProperty("yarn.metrics.db.batch.size", "10");
        props.setProperty("yarn.metrics.db.retention.days", "1");

        try (OutputStream os = new FileOutputStream(configFile)) {
            props.store(os, "SLSMetricsTest config");
        }

        // 构造 SLSMetrics
        SLSConfig config = new SLSConfig(configFile.getAbsolutePath());
        SLSMetrics metrics = null;
        try {
            metrics = new SLSMetrics(config);
            Assert.assertNotNull("SLSMetrics should be created successfully", metrics);
        } finally {
            // 清理
            if (metrics != null) {
                metrics.stop();
            }
            // 清理临时数据库文件
            if (dbFile.exists()) {
                dbFile.delete();
            }
            tempDir.delete();
            configFile.delete();
        }
    }

    /**
     * 测试 database.init() 失败时 SLSMetrics 能正常创建和关闭。
     * 使用非法路径模拟 database.init 失败。
     */
    public void testConstructorWithDbInitFailure() throws Exception {
        // 创建临时配置文件
        File configFile = File.createTempFile("slsmetrics-dbfail-", ".properties");
        configFile.deleteOnExit();

        Properties props = new Properties();
        props.setProperty("yarn.metrics.web.port", "0");
        // 使用非法路径（不存在的目录）触发 init 失败
        props.setProperty("yarn.metrics.db.path",
                "target/test-output/nonexistent-dir/subdir/metrics.db");
        props.setProperty("yarn.metrics.nm.collect.enabled", "false");
        props.setProperty("yarn.metrics.db.cleanup.interval", "0");
        props.setProperty("yarn.metrics.collect.interval", "5000");
        props.setProperty("yarn.metrics.store.size", "100");
        props.setProperty("yarn.metrics.db.batch.size", "10");
        props.setProperty("yarn.metrics.db.retention.days", "1");

        try (OutputStream os = new FileOutputStream(configFile)) {
            props.store(os, "SLSMetricsTest DB fail config");
        }

        SLSConfig config = new SLSConfig(configFile.getAbsolutePath());
        SLSMetrics metrics = null;
        try {
            metrics = new SLSMetrics(config);
            Assert.assertNotNull("SLSMetrics should be created even with DB init failure", metrics);
            // stop() 应能安全处理 database==null 的情况
        } finally {
            if (metrics != null) {
                metrics.stop();
            }
            configFile.delete();
        }
    }

    /**
     * 测试 NM 采集启用时 SLSMetrics 能正常创建。
     * 注意：不会真正连接 NM，仅仅是验证构造流程。
     */
    public void testConstructorWithNmEnabled() throws Exception {
        File tempDir = new File("target/test-output/slsmetrics-nm-test");
        tempDir.mkdirs();
        File dbFile = new File(tempDir, "test-nm-metrics.db");
        if (dbFile.exists()) {
            dbFile.delete();
        }

        File configFile = File.createTempFile("slsmetrics-nm-", ".properties");
        configFile.deleteOnExit();

        Properties props = new Properties();
        props.setProperty("yarn.metrics.web.port", "0");
        props.setProperty("yarn.metrics.db.path", dbFile.getAbsolutePath());
        // 启用 NM 采集（虽然不会有真正的 NM 运行，但构造器应能正常创建实例）
        props.setProperty("yarn.metrics.nm.collect.enabled", "true");
        props.setProperty("yarn.metrics.nm.url", "http://localhost:0");
        props.setProperty("yarn.metrics.db.cleanup.interval", "0");
        props.setProperty("yarn.metrics.collect.interval", "5000");
        props.setProperty("yarn.metrics.store.size", "100");
        props.setProperty("yarn.metrics.db.batch.size", "10");
        props.setProperty("yarn.metrics.db.retention.days", "1");

        try (OutputStream os = new FileOutputStream(configFile)) {
            props.store(os, "SLSMetricsTest NM config");
        }

        SLSConfig config = new SLSConfig(configFile.getAbsolutePath());
        SLSMetrics metrics = null;
        try {
            metrics = new SLSMetrics(config);
            Assert.assertNotNull("SLSMetrics should be created with NM enabled", metrics);
        } finally {
            if (metrics != null) {
                metrics.stop();
            }
            if (dbFile.exists()) {
                dbFile.delete();
            }
            tempDir.delete();
            configFile.delete();
        }
    }
}
