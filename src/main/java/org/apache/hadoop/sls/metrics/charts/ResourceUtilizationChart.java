package org.apache.hadoop.sls.metrics.charts;

import org.apache.hadoop.sls.metrics.MetricsSnapshot;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYAreaRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.BasicStroke;
import java.awt.Color;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 资源利用率面积图：显示内存和虚拟核利用率百分比随时间变化。
 */
public class ResourceUtilizationChart {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceUtilizationChart.class);

    private ResourceUtilizationChart() {
        // 工具类，禁止实例化
    }

    /**
     * 根据快照列表创建资源利用率面积图。
     *
     * @param snapshots 快照列表（按时间顺序）
     * @return JFreeChart 对象
     */
    public static JFreeChart createChart(List<MetricsSnapshot> snapshots) {
        TimeSeries memSeries = new TimeSeries("Memory Utilization");
        TimeSeries vcoreSeries = new TimeSeries("VCore Utilization");

        for (MetricsSnapshot snap : snapshots) {
            Millisecond period = new Millisecond(new Date(snap.getTimestamp()));
            // 转换为百分比（* 100）
            memSeries.addOrUpdate(period, snap.getClusterMemoryUtilization() * 100);
            vcoreSeries.addOrUpdate(period, snap.getClusterVCoreUtilization() * 100);
        }

        TimeSeriesCollection dataset = new TimeSeriesCollection();
        dataset.addSeries(memSeries);
        dataset.addSeries(vcoreSeries);

        JFreeChart chart = ChartFactory.createTimeSeriesChart(
                "Resource Utilization",
                "Time",
                "Utilization (%)",
                dataset,
                true,
                true,
                false
        );

        XYPlot plot = (XYPlot) chart.getPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        // 使用面积渲染器（仅面积，不显示形状和线条），80% 透明度
        XYAreaRenderer renderer = new XYAreaRenderer(XYAreaRenderer.AREA);
        renderer.setSeriesPaint(0, new Color(0x1f, 0x77, 0xb4, 128)); // 蓝色半透明
        renderer.setSeriesPaint(1, new Color(0xff, 0x7f, 0x0e, 128)); // 橙色半透明
        renderer.setSeriesStroke(0, new BasicStroke(1.5f));
        renderer.setSeriesStroke(1, new BasicStroke(1.5f));
        plot.setRenderer(renderer);

        // 范围轴 0-100
        NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setRange(0, 100);
        rangeAxis.setAutoRange(false);

        // X 轴日期格式
        DateAxis dateAxis = (DateAxis) plot.getDomainAxis();
        dateAxis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));

        return chart;
    }
}
