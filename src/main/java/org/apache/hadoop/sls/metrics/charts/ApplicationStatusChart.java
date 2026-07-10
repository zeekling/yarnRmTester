package org.apache.hadoop.sls.metrics.charts;

import org.apache.hadoop.sls.metrics.MetricsSnapshot;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 应用状态堆叠柱状图：显示最近 20 个快照中 Active、Completed、Failed、Submitted 的应用数量。
 */
public class ApplicationStatusChart {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationStatusChart.class);

    private ApplicationStatusChart() {
        // 工具类，禁止实例化
    }

    /**
     * 根据快照列表创建应用状态堆叠柱状图（取最后 20 个快照）。
     *
     * @param snapshots 快照列表（按时间顺序）
     * @return JFreeChart 对象
     */
    public static JFreeChart createChart(List<MetricsSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            // 返回空图表
            return ChartFactory.createStackedBarChart(
                    "Application Status", "Time", "Count",
                    new DefaultCategoryDataset(),
                    PlotOrientation.VERTICAL, true, true, false);
        }

        // 取最后 20 个快照
        int startIdx = Math.max(0, snapshots.size() - 20);
        List<MetricsSnapshot> subList = snapshots.subList(startIdx, snapshots.size());

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");

        for (MetricsSnapshot snap : subList) {
            String timeLabel = sdf.format(new Date(snap.getTimestamp()));
            dataset.addValue(snap.getActiveApplications(), "Active", timeLabel);
            dataset.addValue(snap.getCompletedApplications(), "Completed", timeLabel);
            dataset.addValue(snap.getFailedApplications(), "Failed", timeLabel);
            dataset.addValue(snap.getSubmittedApplications(), "Submitted", timeLabel);
        }

        JFreeChart chart = ChartFactory.createStackedBarChart(
                "Application Status",
                "Time",
                "Count",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );

        // 设置颜色
        chart.getPlot().setBackgroundPaint(Color.WHITE);
        chart.getPlot().setOutlinePaint(Color.LIGHT_GRAY);

        return chart;
    }
}
