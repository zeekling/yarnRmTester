package org.apache.hadoop.sls.config;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class SLSConfig {

    private static final String SLS_NM_HOSTNAME = "yarn.fake.nodemanager.hostname";

    private static final String SLS_NM_RACK = "yarn.fake.nodemanager.rack";

    private static final String SLS_NM_COUNT = "yarn.fake.nodemanger.count";

    private static final String SLS_NM_RPC_PORT_BEGIN = "yarn.fake.nodemanger.rpc.port.begin";

    private static final String SLS_NM_HTTP_PORT_BEGIN = "yarn.fake.nodemanger.http.port.begin";

    private static final String SLS_THREAD_POOL_SIZE = "yarn.fake.threadpool.size";

    private static final String SLS_JOB_TOKEN_SERVERS = "yarn.fake.job.token-servers";

    private static final String SLS_JOB_PARALLEL = "yarn.fake.job.parallel";

    private static final String SLS_JOB_COUNTS = "yarn.fake.job.cycle.times";

    private static final String SLS_JOB_QUEUE = "yarn.fake.job.queue";

    public static final String SLS_NODE_LABEL = "fake_test";

    private static final String SLS_JOB_UPDATE_THREAD_POOL_SIZE = "yarn.fake.job.update.threadpool.size";

    private static final String SLS_JOB_DURATION = "yarn.fake.job.duration";

    private static final String SLS_JOB_CONTAINER_NUM = "yarn.fake.job.container.nums";

    public static final Set<NodeLabel> NODE_LABEL_SET = new HashSet<NodeLabel>(1) {{
        add(NodeLabel.newInstance(SLSConfig.SLS_NODE_LABEL));
    }};

    private final Properties properties = new Properties();

    public SLSConfig(String path) throws IOException {
        properties.load(FileUtils.openInputStream(FileUtils.getFile(path)));
    }

    public String getHostName() {
        return properties.getProperty(SLS_NM_HOSTNAME);
    }

    public String getSlsNmRack() {
        return properties.getProperty(SLS_NM_RACK);
    }

    public int getFakeNMCount() {
        return Integer.parseInt(properties.getProperty(SLS_NM_COUNT, "1"));
    }

    public int getRpcBeginPort() {
        return Integer.parseInt(properties.getProperty(SLS_NM_RPC_PORT_BEGIN, "1200"));
    }

    public int getHttpBeginPort() {
        return Integer.parseInt(properties.getProperty(SLS_NM_HTTP_PORT_BEGIN, "2200"));
    }

    public int getThreadPoolSize() {
        return Integer.parseInt(properties.getProperty(SLS_THREAD_POOL_SIZE, "10"));
    }

    public Path[] getJobTokenServers() {
        String property = properties.getProperty(SLS_JOB_TOKEN_SERVERS);
        String[] split = property.split(",");
        Path[] paths = new Path[split.length];
        for (int i=0; i< split.length ; i++) {
            String str = split[i];
            Path path = new Path(str);
            paths[i] = path;
        }
        return paths;
    }

    public int getJobParallel() {
        return Integer.parseInt(properties.getProperty(SLS_JOB_PARALLEL, "1"));
    }

    public double getJobCycleTimes() {
        return Double.parseDouble(properties.getProperty(SLS_JOB_COUNTS, "1000"));
    }

    public String getJobQueue() {
        return properties.getProperty(SLS_JOB_QUEUE, "default");
    }

    public int getJobUpdateThreadPoolSize() {
        return Integer.parseInt(properties.getProperty(SLS_JOB_UPDATE_THREAD_POOL_SIZE, "10"));
    }

    public int getJobDuration() {
        return Integer.parseInt(properties.getProperty(SLS_JOB_DURATION, "20000"));
    }

    public int getJobContainerNums() {
        return Integer.parseInt(properties.getProperty(SLS_JOB_CONTAINER_NUM, "20"));
    }

    public Resource getJobContainerResource() {
        int vcore = Integer.parseInt(properties.getProperty("yarn.fake.job.container.vcore", "1"));
        long memory = Long.parseLong(properties.getProperty("yarn.fake.job.container.memory-mb", "4096"));
        return Resource.newInstance(memory, vcore);
    }
}
