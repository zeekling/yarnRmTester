package org.apache.hadoop.sls;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.sls.job.FakeJob;
import org.apache.hadoop.sls.util.CommonUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class SLSRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SLSRunner.class);

    private static ExecutorService jobSubmitPool = null;

    public static void main(String[] args) throws IOException, InterruptedException {
        String configPath = "/home/zeekling/project/gitea/yarnRmTester/src/main/resources";
        SLSConfig slsConfig = new SLSConfig(configPath + File.separator + "fake.properites");
        YarnConfiguration config = new YarnConfiguration();
        config.addResource(configPath + File.separator + "core-site.xml");
        config.addResource(configPath + File.separator + "hdfs-site.xml");
        config.addResource(configPath + File.separator + "yarn-site.xml");
        jobSubmitPool = Executors.newFixedThreadPool(slsConfig.getJobParallel());
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        LOG.info("currentUser={} {}", currentUser, currentUser.getTokens());
        int count = 0;
        List<Future<?>> futures = new ArrayList<>();
        while (count <= slsConfig.getJobCycleTimes()) {
            int finalCount = count;
            for (int i=0; i < 500; i++) {
                int finalI = i;
                Runnable runnable = () -> {
                    try {
                        FakeJob job = new FakeJob(config, slsConfig, "test_" + finalCount + "_" + finalI);
                        job.submit();
                        Thread.sleep(200);
                    } catch (Exception e) {
                        LOG.warn("submit job failed");
                    }
                };
                Future<?> future = jobSubmitPool.submit(runnable);
                futures.add(future);
            }
            CommonUtils.waitFutures(futures);
            Thread.sleep(1000);
            count++;
        }

        jobSubmitPool.shutdown();
    }


}
