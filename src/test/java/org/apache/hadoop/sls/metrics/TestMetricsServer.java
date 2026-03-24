package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class TestMetricsServer {
    public static void main(String[] args) {
        try {
            // 创建一个简单的测试来验证监控服务器是否能正常启动
            System.out.println("Testing MetricsServer startup...");
            
            // 创建YarnConfiguration
            YarnConfiguration conf = new YarnConfiguration();
            conf.set("yarn.resourcemanager.hostname", "localhost");
            conf.set("yarn.resourcemanager.address", "localhost:8032");
            
            // 创建YarnClient
            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            
            // 创建MetricsServer
            MetricsServer metricsServer = new MetricsServer(28080, yarnClient);
            
            System.out.println("MetricsServer created successfully on port 28080");
            System.out.println("Test completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Error during test: " + e.getMessage());
            e.printStackTrace();
        }
    }
}