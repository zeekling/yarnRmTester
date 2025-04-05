package org.apache.hadoop.sls.config;

import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.util.Properties;

public class SLSConfig {

    public static final String SLS_NM_HOSTNAME = "yarn.fake.nodemanager.hostname";

    public static final String SLS_NM_RACK = "yarn.fake.nodemanager.rack";

    public static final String SLS_NM_COUNT = "yarn.fake.nodemanger.count";

    public static final String SLS_NM_RPC_PORT_BEGIN = "yarn.fake.nodemanger.rpc.port.begin";

    public static final String SLS_NM_HTTP_PORT_BEGIN = "yarn.fake.nodemanger.http.port.begin";

    public static final String SLS_THREAD_POOL_SIZE = "yarn.fake.threadpool.size";

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
}
