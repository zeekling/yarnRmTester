package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ResourceManagerMetricsCollector {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerMetricsCollector.class);

    private final YarnClient yarnClient;
    private volatile int activeApplications = 0;
    private volatile int completedApplications = 0;
    private volatile int failedApplications = 0;
    private volatile long lastCollectTime;

    public ResourceManagerMetricsCollector(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
        this.lastCollectTime = System.currentTimeMillis();
    }

    public void collectMetrics() {
        try {
            List<ApplicationReport> apps = yarnClient.getApplications();
            activeApplications = 0;
            completedApplications = 0;
            failedApplications = 0;

            for (ApplicationReport app : apps) {
                switch (app.getYarnApplicationState()) {
                    case RUNNING:
                    case SUBMITTED:
                    case ACCEPTED:
                        activeApplications++;
                        break;
                    case FINISHED:
                        completedApplications++;
                        break;
                    case FAILED:
                        failedApplications++;
                        break;
                    default:
                        break;
                }
            }

            lastCollectTime = System.currentTimeMillis();
            LOG.debug("Collected RM metrics: active={}, completed={}, failed={}",
                    activeApplications, completedApplications, failedApplications);
        } catch (IOException | YarnException e) {
            LOG.warn("Failed to collect RM metrics", e);
        }
    }

    public int getActiveApplications() {
        return activeApplications;
    }

    public int getCompletedApplications() {
        return completedApplications;
    }

    public int getFailedApplications() {
        return failedApplications;
    }

    public long getLastCollectTime() {
        return lastCollectTime;
    }
}