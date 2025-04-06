package org.apache.hadoop.sls.nm;

import org.apache.hadoop.sls.SLSNodeManager;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class JobStatUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(JobStatUpdater.class);

    private final List<YarnFakeNodeManager> fakeNodeManagers;

    private Thread updateThread = null;

    private ExecutorService jobUpdatePool = null;

    private boolean isStoped = false;

    private final ApplicationMasterProtocol appMasterClient;

    public JobStatUpdater(SLSConfig slsConfig, List<YarnFakeNodeManager> fakeNodeManagers, YarnConfiguration config) throws IOException {
        this.fakeNodeManagers = fakeNodeManagers;
        this.jobUpdatePool = Executors.newFixedThreadPool(slsConfig.getJobUpdateThreadPoolSize());
        this.appMasterClient = ClientRMProxy.createRMProxy(config, ApplicationMasterProtocol.class);
        initUpdateThread();
    }

    private void initUpdateThread() {
        Runnable runnable = () -> {
            Map<NodeId, Future<?>> futureMap = new HashMap<>();
            while (!isStoped) {
                for (YarnFakeNodeManager fakeNodeManager : fakeNodeManagers) {
                    Future<?> future = futureMap.get(fakeNodeManager.getNodeId());
                    boolean needUpdate = true;
                    if (future != null) {
                        try {
                            future.get(20, TimeUnit.MILLISECONDS);
                        } catch (Exception e) {
                            needUpdate = false;
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
