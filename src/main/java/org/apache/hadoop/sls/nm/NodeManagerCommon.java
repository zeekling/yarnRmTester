package org.apache.hadoop.sls.nm;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.sls.config.SLSConfig;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeManagerCommon {

    public static final Map<NodeId, YarnFakeNodeManager> FAKE_NODE_MANAGER_MAP = new ConcurrentHashMap<>();

    public static final AtomicInteger count = new AtomicInteger();

    public static NodeId getTaragetNode(SLSConfig slsConfig) {
        if (count.get() > FAKE_NODE_MANAGER_MAP.size()) {
            count.set(0);
        }
        return NodeId.newInstance(slsConfig.getHostName(), slsConfig.getRpcBeginPort() + count.getAndIncrement() % FAKE_NODE_MANAGER_MAP.size());
    }

}
