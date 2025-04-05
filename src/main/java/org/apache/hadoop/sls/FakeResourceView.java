package org.apache.hadoop.sls;

import org.apache.hadoop.yarn.server.nodemanager.ResourceView;

public class FakeResourceView implements ResourceView {

    @Override
    public long getVmemAllocatedForContainers() {
        return 0;
    }

    @Override
    public long getPmemAllocatedForContainers() {
        return 0;
    }

    @Override
    public long getVCoresAllocatedForContainers() {
        return 0;
    }

    @Override
    public boolean isVmemCheckEnabled() {
        return true;
    }

    @Override
    public boolean isPmemCheckEnabled() {
        return true;
    }
}
