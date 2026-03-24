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