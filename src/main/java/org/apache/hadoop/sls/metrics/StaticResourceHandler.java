package org.apache.hadoop.sls.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * 前端静态资源 HTTP 处理器。
 * <p>
 * 从 classpath 的 {@code /frontend/} 目录加载 HTML/JS/CSS 等静态文件。
 * 路由规则：
 * <ul>
 *   <li>{@code /} → {@code /frontend/index.html}</li>
 *   <li>{@code /js/*} → {@code /frontend/js/*}</li>
 *   <li>{@code /css/*} → {@code /frontend/css/*}</li>
 *   <li>{@code /favicon.ico} → 返回 204（无图标）</li>
 * </ul>
 * </p>
 */
public class StaticResourceHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(StaticResourceHandler.class);

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestPath = exchange.getRequestURI().getPath();

        // 如果路径以 /api 开头，跳过（由 MetricsApiHandler 处理）
        if (requestPath.startsWith("/api")) {
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        // 将请求路径映射到 classpath 资源路径
        String resourcePath = mapToResourcePath(requestPath);
        if (resourcePath == null) {
            sendNotFound(exchange, requestPath);
            return;
        }

        InputStream in = getClass().getResourceAsStream(resourcePath);
        if (in == null) {
            LOG.debug("Static resource not found: {} (mapped from {})", resourcePath, requestPath);
            sendNotFound(exchange, requestPath);
            return;
        }

        String contentType = getContentType(requestPath);
        exchange.getResponseHeaders().set("Content-Type", contentType + "; charset=utf-8");
        exchange.sendResponseHeaders(200, 0);

        try (OutputStream os = exchange.getResponseBody()) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) != -1) {
                os.write(buf, 0, len);
            }
        } catch (IOException e) {
            LOG.warn("Error sending static resource: {}", requestPath, e);
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * 将 HTTP 请求路径映射到 classpath 资源路径。
     */
    private String mapToResourcePath(String requestPath) {
        if (requestPath == null || requestPath.isEmpty() || "/".equals(requestPath)) {
            return "/frontend/index.html";
        }
        // 移除开头的 /
        String cleanPath = requestPath.startsWith("/") ? requestPath.substring(1) : requestPath;
        return "/frontend/" + cleanPath;
    }

    /**
     * 根据文件扩展名返回 Content-Type。
     */
    private String getContentType(String path) {
        if (path.endsWith(".html") || path.endsWith(".htm")) {
            return "text/html";
        } else if (path.endsWith(".js")) {
            return "application/javascript";
        } else if (path.endsWith(".css")) {
            return "text/css";
        } else if (path.endsWith(".png")) {
            return "image/png";
        } else if (path.endsWith(".svg")) {
            return "image/svg+xml";
        } else if (path.endsWith(".ico")) {
            return "image/x-icon";
        } else if (path.endsWith(".json")) {
            return "application/json";
        }
        return "application/octet-stream";
    }

    private void sendNotFound(HttpExchange exchange, String path) throws IOException {
        String msg = "404 Not Found: " + path;
        byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(404, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
