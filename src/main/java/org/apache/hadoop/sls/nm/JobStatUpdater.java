package org.apache.hadoop.sls.nm;

import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

import static org.apache.hadoop.sls.nm.NodeManagerCommon.FAKE_NODE_MANAGER_MAP;

public class JobStatUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatUpdater.class);

    private final Map<NodeId, YarnFakeNodeManager> fakeNodeManagerMap;

    private Thread updateThread = null;

    private ExecutorService jobUpdatePool = null;

    private boolean isStoped = false;

    private final ApplicationMasterProtocol appMasterClient;

    public JobStatUpdater(SLSConfig slsConfig, Map<NodeId, YarnFakeNodeManager> fakeNodeManagerMap, YarnConfiguration config) throws IOException {
        this.fakeNodeManagerMap = fakeNodeManagerMap;
        this.jobUpdatePool = Executors.newFixedThreadPool(slsConfig.getJobUpdateThreadPoolSize());
        this.appMasterClient = ClientRMProxy.createRMProxy(config, ApplicationMasterProtocol.class);
        initUpdateThread();
    }

    private void initUpdateThread() {
        Runnable runnable = () -> {
            Map<NodeId, Future<?>> futureMap = new ConcurrentHashMap<>();
            while (!isStoped) {
                for (Map.Entry<NodeId, YarnFakeNodeManager> entry : FAKE_NODE_MANAGER_MAP.entrySet()) {
                    YarnFakeNodeManager fakeNodeManager = entry.getValue();
                    Future<?> future = futureMap.get(fakeNodeManager.getNodeId());
                    boolean needUpdate = true;
                    if (future != null) {
                        try {
                            future.get(20, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            needUpdate = false;
                        } catch (Exception e) {
                            LOG.warn("updateContainerStatus exception", e);
                            future.cancel(true);
                            futureMap.remove(fakeNodeManager.getNodeId());
                        }
                    }
                    if (!needUpdate) {
                        continue;
                    }
                    Runnable update = ()-> {
                        try {
                            fakeNodeManager.updateContainerStatus();
                        } catch (IOException | YarnException e) {
                            LOG.warn("update Container failed");
                        }
                    };
                    future = jobUpdatePool.submit(update);
                    futureMap.put(fakeNodeManager.getNodeId(), future);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        updateThread = new Thread(runnable);
    }


    public void updateAsync() {
        updateThread.start();
    }

    public void close() {
        isStoped = true;
        if (jobUpdatePool != null) {
            jobUpdatePool.shutdown();
        }
    }

}
