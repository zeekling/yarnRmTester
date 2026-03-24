package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatResponseCollector {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatResponseCollector.class);

    private final MetricsData metricsData;

    public HeartbeatResponseCollector(MetricsData metricsData) {
        this.metricsData = metricsData;
    }

    public void collect(NodeHeartbeatResponse response) {
        if (response == null) {
            metricsData.incrementFailedHeartbeats();
            return;
        }

        metricsData.incrementSuccessfulHeartbeats();
        metricsData.updateLastHeartbeatTime();

        if (response.getContainersToBeRemovedFromNM() != null) {
            int containersReleased = response.getContainersToBeRemovedFromNM().size();
            for (int i = 0; i < containersReleased; i++) {
                metricsData.incrementContainersReleased();
            }
        }

        LOG.debug("Collected heartbeat response: containers released={}",
                response.getContainersToBeRemovedFromNM() != null ? response.getContainersToBeRemovedFromNM().size() : 0);
    }

    public MetricsData getMetricsData() {
        return metricsData;
    }
}