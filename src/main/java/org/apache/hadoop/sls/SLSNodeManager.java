package org.apache.hadoop.sls;

import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.util.CommonUtils;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SLSNodeManager {

    private static final Logger LOG = LoggerFactory.getLogger(SLSNodeManager.class);

    private static ExecutorService executor = null;

    public static void main(String[] args) throws IOException {
        String configPath = "/home/zeekling/project/gitea/yarnRmTester/src/main/resources";
        SLSConfig slsConfig = new SLSConfig(configPath + File.separator + "fake.properites");
        YarnConfiguration config = new YarnConfiguration();
        config.addResource(configPath + File.separator + "core-site.xml");
        config.addResource(configPath + File.separator + "hdfs-site.xml");
        config.addResource(configPath + File.separator + "yarn-site.xml");
        long memory = Long.parseLong(config.get(YarnConfiguration.NM_PMEM_MB));
        int vcore = Integer.parseInt(config.get(YarnConfiguration.NM_VCORES));
        Resource capacity = Resource.newInstance(memory, vcore);
        executor = Executors.newFixedThreadPool(slsConfig.getThreadPoolSize());
        List<YarnFakeNodeManager> fakeNodeManagers = new ArrayList<>();
        initFakeNM(slsConfig, capacity, config, fakeNodeManagers);
        System.out.println("NM nodes count=" + fakeNodeManagers.size());
        beginHeartBeat(fakeNodeManagers, executor);
    }

    private static void initFakeNM(SLSConfig slsConfig, Resource capacity, YarnConfiguration config, List<YarnFakeNodeManager> fakeNodeManagers) {
        List<Future<?>> futures = new ArrayList<>(slsConfig.getFakeNMCount());
        for (int i = 0; i < slsConfig.getFakeNMCount(); i++) {
            int finalI = i;
            Runnable runnable = () -> {
                YarnFakeNodeManager fakeNodeManager = null;
                try {
                    fakeNodeManager = new YarnFakeNodeManager(slsConfig.getHostName(),
                            slsConfig.getRpcBeginPort() + finalI, slsConfig.getHttpBeginPort() + finalI,
                            slsConfig.getSlsNmRack(), capacity, config);
                    fakeNodeManagers.add(fakeNodeManager);
                } catch (IOException | YarnException e) {
                    LOG.warn("failed to init NodeManager", e);
                }
            };
            Future<?> future = executor.submit(runnable);
            futures.add(future);
        }
        CommonUtils.waitFutures(futures);
    }

    private static void beginHeartBeat(List<YarnFakeNodeManager> fakeNodeManagers, ExecutorService executor) {
        Map<NodeId, Future<?>> futureMap = new HashMap<>(fakeNodeManagers.size());
        while (true) {
            for (YarnFakeNodeManager fakeNodeManager: fakeNodeManagers) {
                Future<?> future = futureMap.get(fakeNodeManager.getNodeId());
                boolean needHeartBeat = true;
                if (future != null) {
                    try {
                        future.get(20, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        needHeartBeat = false;
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
