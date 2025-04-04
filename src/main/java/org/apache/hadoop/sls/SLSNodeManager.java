package org.apache.hadoop.sls;

import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SLSNodeManager {

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        String configPath = "/home/zeekling/project/gitea/yarnRmTester/src/main/resources";
        SLSConfig slsConfig = new SLSConfig(configPath + File.separator + "fake.properites");
        YarnConfiguration config = new YarnConfiguration();
        config.addResource(configPath + File.separator + "core-site.xml");
        config.addResource(configPath + File.separator + "hdfs-site.xml");
        config.addResource(configPath + File.separator + "yarn-site.xml");
        long memory = Long.parseLong(config.get(YarnConfiguration.NM_PMEM_MB));
        int vcore = Integer.parseInt(config.get(YarnConfiguration.NM_VCORES));
        Resource capacity = Resource.newInstance(memory, vcore);
        List<YarnFakeNodeManager> fakeNodeManagers = new ArrayList<>();
        for (int i = 0; i < slsConfig.getFakeNMCount(); i++) {
            YarnFakeNodeManager fakeNodeManager = new YarnFakeNodeManager(slsConfig.getHostName(),
                    slsConfig.getRpcBeginPort() + i, slsConfig.getHttpBeginPort() + i,
                    slsConfig.getSlsNmRack(), capacity, config);
            fakeNodeManagers.add(fakeNodeManager);
        }
        System.out.println("NM nodes count=" + fakeNodeManagers.size());
        Thread.sleep(10000);
    }

}
