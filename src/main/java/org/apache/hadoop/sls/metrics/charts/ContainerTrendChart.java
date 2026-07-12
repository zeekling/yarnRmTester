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
 * 容器趋势折线图：显示活跃(Active) 容器数量随时间变化。
 * 数据来源：RM RPC NodeReport.getNumContainers()。
 */
public class ContainerTrendChart {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerTrendChart.class);

    private ContainerTrendChart() {
        // 工具类，禁止实例化
    }

    /**
     * 根据快照列表创建容器趋势图。
     *
     * @param snapshots 快照列表（按时间顺序）
     * @return JFreeChart 对象
     */
    public static JFreeChart createChart(List<MetricsSnapshot> snapshots) {
        TimeSeries activeSeries = new TimeSeries("Active Containers");

        for (MetricsSnapshot snap : snapshots) {
            Millisecond period = new Millisecond(new Date(snap.getTimestamp()));
            activeSeries.addOrUpdate(period, (double) snap.getActiveContainers());
        }

        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(activeSeries);

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Active Containers",
                "Time",
                "Count",
                dataset,
                true,
                true,
                false
        );

        // 自定义样式
        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        // 隐藏形状，设置颜色
        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        renderer.setDefaultShapesVisible(false);
        renderer.setSeriesPaint(0, Color.decode("#2ca02c")); // 绿色 - Active Containers

        // X 轴日期格式
        DateAxis dateAxis = (DateAxis) plot.getDomainAxis();
        dateAxis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));

        return chart;
    }
}
