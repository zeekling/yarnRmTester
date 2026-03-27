package org.apache.hadoop.sls;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.metrics.MetricsServer;
import org.apache.hadoop.sls.metrics.HeartbeatResponseCollector;
import org.apache.hadoop.sls.nm.JobStatUpdater;
import org.apache.hadoop.sls.nm.YarnFakeNodeManager;
import org.apache.hadoop.sls.util.CommonUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.hadoop.sls.nm.NodeManagerCommon.FAKE_NODE_MANAGER_MAP;

/**
 * SLSNodeManager manages fake NodeManagers and metrics collection for YARN simulation.
 * This class initializes multiple fake NodeManagers and starts a metrics server to 
 * collect and expose metrics about the YARN cluster simulation.
 */
public class SLSNodeManager {

    private static final Logger LOG = LoggerFactory.getLogger(SLSNodeManager.class);

    private static ExecutorService executor = null;
    private static MetricsServer metricsServer;
    private static int metricsPort = 28080; // Default port

    public static void main(String[] args) throws IOException, YarnException {
        String configPath = "D:\\project\\gitea\\yarnRmTester\\src\\main\\resources";
        if (args.length != 0) {
            configPath = args[0];
        }
        SLSConfig slsConfig = new SLSConfig(configPath + File.separator + "fake.properites");
        YarnConfiguration config = new YarnConfiguration();
        config.addResource(new Path(configPath + File.separator + "core-site.xml"));
        config.addResource(new Path(configPath + File.separator + "hdfs-site.xml"));
        config.addResource(new Path(configPath + File.separator + "yarn-site.xml"));

        long memory = Long.parseLong(config.get(YarnConfiguration.NM_PMEM_MB));
        int vcore = Integer.parseInt(config.get(YarnConfiguration.NM_VCORES));
        Resource capacity = Resource.newInstance(memory, vcore);
        executor = Executors.newFixedThreadPool(slsConfig.getThreadPoolSize());
        Map<NodeId, YarnFakeNodeManager> fakeNodeManagerMap = FAKE_NODE_MANAGER_MAP;
        LOG.info("Fake container capacity: {}", slsConfig.getJobContainerResource());
        initFakeNM(slsConfig, capacity, config, fakeNodeManagerMap);
        LOG.info("==== Init Fake NM success, Fake NM count={} ======", fakeNodeManagerMap.size());

        // Initialize metrics server
        initMetricsServer(config, slsConfig);
        
        JobStatUpdater updater = new JobStatUpdater(slsConfig, fakeNodeManagerMap, config);
        updater.updateAsync();
        
        // Add shutdown hook for metrics server
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down metrics server...");
            if (metricsServer != null) {
                try {
                    metricsServer.stop();
                } catch (Exception e) {
                    LOG.error("Error stopping metrics server", e);
                }
            }
        }));
        
        beginHeartBeat(fakeNodeManagerMap, executor);
    }


    private static void initFakeNM(SLSConfig slsConfig, Resource capacity, YarnConfiguration config, Map<NodeId, YarnFakeNodeManager> fakeNodeManagerMap) throws IOException, YarnException {
        ResourceManagerAdministrationProtocol rmAdmin = ClientRMProxy.createRMProxy(config, ResourceManagerAdministrationProtocol.class);
        List<Future<?>> futures = new ArrayList<>(slsConfig.getFakeNMCount());
        for (int i = 0; i < slsConfig.getFakeNMCount(); i++) {
            int finalI = i;
            Runnable runnable = () -> {
                YarnFakeNodeManager fakeNodeManager = null;
                try {
                    fakeNodeManager = new YarnFakeNodeManager(
                            slsConfig.getRpcBeginPort() + finalI, slsConfig.getHttpBeginPort() + finalI,
                            slsConfig.getSlsNmRack(), capacity, config, slsConfig);
                    fakeNodeManagerMap.put(fakeNodeManager.getNodeId(), fakeNodeManager);
                } catch (IOException | YarnException e) {
                    LOG.warn("failed to init NodeManager", e);
                }
            };
            Future<?> future = executor.submit(runnable);
            futures.add(future);
        }
        CommonUtils.waitFutures(futures);
    }

    private static void initMetricsServer(YarnConfiguration config, SLSConfig slsConfig) {
        try {
            String portStr = slsConfig.getProperty("yarn.monitor.http.port", "28080");
            metricsPort = Integer.parseInt(portStr);
            
            if (!slsConfig.isMonitorEnabled()) {
                LOG.info("Metrics server is disabled");
                return;
            }
            
            YarnClient yarnClient = YarnClient.createYarnClient();
            yarnClient.init(config);
            yarnClient.start();
            
            metricsServer = new MetricsServer(metricsPort, yarnClient);
            
            // Register heartbeat collectors for each fake NM
            for (Map.Entry<NodeId, YarnFakeNodeManager> entry : FAKE_NODE_MANAGER_MAP.entrySet()) {
                YarnFakeNodeManager nm = entry.getValue();
                metricsServer.registerHeartbeatCollector(nm.getNodeId().toString(), nm.getHeartbeatCollector());
            }
            
            metricsServer.start();
            LOG.info("Metrics server started on port {}", metricsPort);
        } catch (Exception e) {
            LOG.error("Failed to initialize metrics server", e);
        }
    }

    private static void beginHeartBeat(Map<NodeId, YarnFakeNodeManager> fakeNodeManagerMap, ExecutorService executor) {
        Map<NodeId, Future<?>> futureMap = new HashMap<>(fakeNodeManagerMap.size());
        while (true) {
            for (Map.Entry<NodeId, YarnFakeNodeManager> entry : FAKE_NODE_MANAGER_MAP.entrySet()) {
                YarnFakeNodeManager fakeNodeManager = entry.getValue();
                Future<?> future = futureMap.get(fakeNodeManager.getNodeId());
                boolean needHeartBeat = true;
                if (future != null) {
                    try {
                        future.get(20, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException e) {
                        needHeartBeat = false;
                    } catch (Exception e) {
                        LOG.warn("updateContainerStatus exception", e);
                        future.cancel(true);
                        futureMap.remove(fakeNodeManager.getNodeId());
                    }
                }
                if (!needHeartBeat) {
                    continue;
                }
                Runnable runnable = () -> {
                    try {
                        LOG.debug("begin heartbeat for {}", fakeNodeManager.getNodeId());
                        fakeNodeManager.heartbeat();
                        LOG.debug("heartbeat for {} success", fakeNodeManager.getNodeId());
                    } catch (IOException | YarnException e) {
                        LOG.warn("heart beat failed");
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                };
                future = executor.submit(runnable);
                futureMap.put(fakeNodeManager.getNodeId(), future);
            }
        }
    }

}
