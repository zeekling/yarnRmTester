package org.apache.hadoop.sls.nm;

import org.apache.hadoop.sls.job.FakeApplication;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManagerCommon {

    public static final Map<NodeId, YarnFakeNodeManager> FAKE_NODE_MANAGER_MAP = new ConcurrentHashMap<>();


}
