package org.apache.hadoop.sls.nm;

import org.apache.hadoop.sls.job.FakeApplication;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NodeManagerCommon {

    public static final Map<NodeId, YarnFakeNodeManager> FAKE_NODE_MANAGER_MAP = new ConcurrentHashMap<>();

    public static FakeApplication getContainer(ContainerId containerId) {
        for (Map.Entry<NodeId, YarnFakeNodeManager> entry: FAKE_NODE_MANAGER_MAP.entrySet()) {
            YarnFakeNodeManager fakeNodeManager = entry.getValue();
            ApplicationId applicationId = containerId.getApplicationAttemptId().getApplicationId();
            FakeApplication fakeApplication = fakeNodeManager.getApplicationMap().get(applicationId);
            if (fakeApplication == null || fakeApplication.getAppMaster() == null) {
                continue;
            }
            return fakeApplication;
        }
        return null;
    }

}
