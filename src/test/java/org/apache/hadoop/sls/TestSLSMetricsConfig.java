package org.apache.hadoop.sls;

import junit.framework.TestCase;
import org.apache.hadoop.sls.SLSMetrics;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestSLSMetricsConfig extends TestCase {

    private static final String TEST_CONFIG_DIR = "test-config-dir";
    private static final String TEST_CONFIG_FILE = TEST_CONFIG_DIR + "/fake.properites";
    
    public void testSLSMetricsCreationWithConfigDir() throws IOException {
        // 创建测试配置目录和文件
        createTestConfigDirAndFile();
        
        try {
            // 使用配置目录创建SLSMetrics实例
            SLSMetrics metrics = new SLSMetrics(TEST_CONFIG_DIR);
            assertNotNull("SLSMetrics should be created with config dir", metrics);
        } catch (Exception e) {
            fail("SLSMetrics should not throw exception with config dir: " + e.getMessage());
        } finally {
            // 清理测试配置目录和文件
            File configFile = new File(TEST_CONFIG_FILE);
            if (configFile.exists()) {
                configFile.delete();
            }
            File configDir = new File(TEST_CONFIG_DIR);
            if (configDir.exists()) {
                configDir.delete();
            }
        }
    }
    
    public void testSLSMetricsCreationWithNonExistentConfigDir() {
        try {
            // 使用不存在的配置目录创建SLSMetrics实例
            SLSMetrics metrics = new SLSMetrics("nonexistent-dir");
            assertNotNull("SLSMetrics should be created with non-existent config dir", metrics);
        } catch (Exception e) {
            fail("SLSMetrics should not throw exception with non-existent config dir: " + e.getMessage());
        }
    }
    
    private void createTestConfigDirAndFile() throws IOException {
        // 创建测试目录
        File configDir = new File(TEST_CONFIG_DIR);
        if (!configDir.exists()) {
            configDir.mkdirs();
        }
        
        // 创建测试配置文件
        File configFile = new File(TEST_CONFIG_FILE);
        FileWriter writer = new FileWriter(configFile);
        writer.write("# Test configuration\n");
        writer.write("yarn.monitor.http.port=38080\n");
        writer.write("yarn.monitor.enabled=true\n");
        writer.close();
    }
}