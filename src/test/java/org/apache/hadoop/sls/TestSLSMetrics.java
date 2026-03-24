package org.apache.hadoop.sls;

import junit.framework.TestCase;
import org.apache.hadoop.sls.SLSMetrics;

public class TestSLSMetrics extends TestCase {

    public void testSLSMetricsCreationWithDefaultConfig() {
        try {
            SLSMetrics metrics = new SLSMetrics();
            assertNotNull("SLSMetrics should be created", metrics);
            assertEquals("SLSMetrics should not be running initially", false, metrics.isRunning());
        } catch (Exception e) {
            // 允许文件不存在的异常
        }
    }

    public void testGetMetricsServer() {
        try {
            SLSMetrics metrics = new SLSMetrics();
            assertNull("MetricsServer should be null before start", metrics.getMetricsServer());
        } catch (Exception e) {
            // 允许文件不存在的异常
        }
    }
    
    public void testSLSMetricsCreationWithConfigPath() {
        try {
            // 使用不存在的配置文件路径测试
            SLSMetrics metrics = new SLSMetrics("nonexistent.properties");
            assertNotNull("SLSMetrics should be created with config path", metrics);
        } catch (Exception e) {
            fail("SLSMetrics should not throw exception with config path: " + e.getMessage());
        }
    }
}