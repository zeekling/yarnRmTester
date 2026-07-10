package org.apache.hadoop.sls.metrics;

import org.apache.hadoop.sls.metrics.charts.ApplicationStatusChart;
import org.apache.hadoop.sls.metrics.charts.ContainerTrendChart;
import org.apache.hadoop.sls.metrics.charts.HeartbeatLatencyChart;
import org.apache.hadoop.sls.metrics.charts.ResourceUtilizationChart;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 定期图表生成器：从 MetricsStore 读取快照，生成 PNG 图表文件保存到输出目录。
 */
public class ChartGenerator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ChartGenerator.class);

    private final String outputDirPath;
    private final MetricsStore store;
    private final ScheduledExecutorService scheduler;

    /**
     * @param outputDirPath   图表输出目录
     * @param store           指标内存存储
     * @param chartIntervalMs 图表生成间隔（毫秒）
     */
    public ChartGenerator(String outputDirPath, MetricsStore store, long chartIntervalMs) {
        this.outputDirPath = outputDirPath;
        this.store = store;

        // 确保输出目录存在
        File outputDir = new File(outputDirPath);
        if (!outputDir.exists()) {
            boolean created = outputDir.mkdirs();
            if (created) {
                LOG.info("Created chart output directory: {}", outputDirPath);
            } else {
                LOG.warn("Failed to create chart output directory: {}", outputDirPath);
            }
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chart-generator");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(this::generateAllCharts,
                0, chartIntervalMs, TimeUnit.MILLISECONDS);

        LOG.info("ChartGenerator started: outputDir={}, interval={}ms", outputDirPath, chartIntervalMs);
    }

    /**
     * 生成所有类型的图表并保存为 PNG 文件。
     */
    void generateAllCharts() {
        List<MetricsSnapshot> snapshots = store.getAll();
        if (snapshots == null || snapshots.isEmpty()) {
            LOG.debug("No snapshots available for chart generation, skipping");
            return;
        }

        generateChart("container-trend.png",
                ContainerTrendChart.createChart(snapshots));
        generateChart("resource-util.png",
                ResourceUtilizationChart.createChart(snapshots));
        generateChart("app-status.png",
                ApplicationStatusChart.createChart(snapshots));
        generateChart("heartbeat-latency.png",
                HeartbeatLatencyChart.createChart(snapshots));

        LOG.debug("All charts generated successfully ({} snapshots)", snapshots.size());
    }

    /**
     * 保存单个图表为 PNG 文件。
     */
    private void generateChart(String filename, JFreeChart chart) {
        File outputFile = new File(outputDirPath, filename);
        try {
            ChartUtils.saveChartAsPNG(outputFile, chart, 960, 540);
            LOG.debug("Chart saved: {}", outputFile.getAbsolutePath());
        } catch (IOException e) {
            LOG.error("Failed to save chart: {}", filename, e);
        }
    }

    @Override
    public void close() {
        LOG.info("Stopping ChartGenerator");
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
