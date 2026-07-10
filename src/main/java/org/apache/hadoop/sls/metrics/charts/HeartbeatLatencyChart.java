package org.apache.hadoop.sls.metrics.charts;

import org.apache.hadoop.sls.metrics.MetricsSnapshot;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 心跳延迟折线图：显示平均延迟、最大延迟和吞吐量随时间变化。
 */
public class HeartbeatLatencyChart {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatLatencyChart.class);

    private HeartbeatLatencyChart() {
        // 工具类，禁止实例化
    }

    /**
     * 根据快照列表创建心跳延迟折线图。
     *
     * @param snapshots 快照列表（按时间顺序）
     * @return JFreeChart 对象
     */
    public static JFreeChart createChart(List<MetricsSnapshot> snapshots) {
        TimeSeries avgLatencySeries = new TimeSeries("Avg Latency (ms)");
        TimeSeries maxLatencySeries = new TimeSeries("Max Latency (ms)");
        TimeSeries throughputSeries = new TimeSeries("Throughput (ops/s)");

        for (MetricsSnapshot snap : snapshots) {
            Millisecond period = new Millisecond(new Date(snap.getTimestamp()));
            avgLatencySeries.addOrUpdate(period, snap.getAvgHeartbeatLatency());
            maxLatencySeries.addOrUpdate(period, (double) snap.getMaxHeartbeatLatency());
            throughputSeries.addOrUpdate(period, snap.getHeartbeatThroughput());
        }

        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(avgLatencySeries);
        dataset.addSeries(maxLatencySeries);
        dataset.addSeries(throughputSeries);

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Heartbeat Latency",
                "Time",
                "Latency (ms) / Throughput (ops/s)",
                dataset,
                true,
                true,
                false
        );

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        renderer.setDefaultShapesVisible(false);
        renderer.setSeriesPaint(0, Color.decode("#1f77b4")); // 蓝色 - Avg Latency
        renderer.setSeriesPaint(1, Color.decode("#d62728")); // 红色 - Max Latency
        renderer.setSeriesPaint(2, Color.decode("#2ca02c")); // 绿色 - Throughput

        DateAxis dateAxis = (DateAxis) plot.getDomainAxis();
        dateAxis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));

        return chart;
    }
}
