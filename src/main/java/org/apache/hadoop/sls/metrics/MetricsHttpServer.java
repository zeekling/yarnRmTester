package org.apache.hadoop.sls.metrics;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * SLSMetrics 内嵌 HTTP 服务器。
 * <p>
 * 统一管理 REST API 和前端静态资源的 HTTP 路由：
 * <ul>
 *   <li>{@code /api/*} — REST API（由 MetricsApiHandler 处理）</li>
 *   <li>{@code /} — 前端静态资源（由 StaticResourceHandler 处理）</li>
 * </ul>
 * </p>
 */
public class MetricsHttpServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsHttpServer.class);

    private final HttpServer server;
    private final int port;

    /**
     * @param port      监听端口
     * @param store     指标内存存储（用于最新快照）
     * @param database  SQLite 持久化层（用于历史查询）
     */
    public MetricsHttpServer(int port, MetricsStore store, MetricsDatabase database) {
        this.port = port;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.setExecutor(Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "metrics-http-worker");
                t.setDaemon(true);
                return t;
            }));

            // REST API 路由
            server.createContext("/api", new MetricsApiHandler(store, database));

            // 前端静态资源路由（处理 / 以及所有非 /api 的请求）
            server.createContext("/", new StaticResourceHandler());

            server.start();
            LOG.info("MetricsHttpServer started on port {}", port);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start MetricsHttpServer on port " + port, e);
        }
    }

    public int getPort() {
        return port;
    }

    @Override
    public void close() {
        LOG.info("Stopping MetricsHttpServer on port {}", port);
        if (server != null) {
            server.stop(2); // 等待最多 2 秒完成已有请求
        }
    }
}
