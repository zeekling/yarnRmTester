package org.apache.hadoop.sls.nm;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.hadoop.sls.job.FakeApplication;
import org.apache.hadoop.sls.util.CommonUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class NMHttpHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NMHttpHandler.class);

    private final YarnFakeNodeManager nodeManager;

    public NMHttpHandler(YarnFakeNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        URI requestURI = exchange.getRequestURI();
        String path = requestURI.getPath();
        if (path.contains("container_")) {
            appMasterInfo(exchange);
        } else {
            nodeManagerInfo(exchange);
        }
    }

    private void appMasterInfo(HttpExchange exchange) throws IOException {
        URI requestURI = exchange.getRequestURI();
        String path = requestURI.getPath();
        String containerIdStr = path.replace("/", "");
        try {
            ContainerId containerId = ContainerId.fromString(containerIdStr);
            FakeApplication application = NodeManagerCommon.getContainer(containerId);
            if (application == null) {
                sendMessage(exchange, containerIdStr + " Not Found!");
                return;
            }
            StringBuilder sb = new StringBuilder("Fake appMaster\n");
            sb.append("started Time: ").append(new Date(application.getAppStartTime())).append("\n");
            sb.append("applicationId:").append(application.getAppMaster().getId().getApplicationAttemptId().getApplicationId()).append("\n");
            sb.append("allocated containers:").append(application.getAllocatedCount()).append("\n");
            sb.append("all Containers:").append(nodeManager.getSlsConfig().getJobContainerNums()).append("\n");
            sendMessage(exchange, sb.toString());
        } catch (Exception e) {
            sendMessage(exchange, e.getMessage());
        }

    }

    private void nodeManagerInfo(HttpExchange exchange) throws IOException {
        StringBuilder sb = new StringBuilder("<p><strong> Fake NodeManager ").append(nodeManager.getNodeId()).append(" </strong></p>");
        sb.append("Rack: ").append(nodeManager.getRackName()).append("<br>");
        sb.append("Capacity: ").append(CommonUtils.getResourceStr(nodeManager.getCapability())).append("<br>");
        sb.append("Avail: ").append(CommonUtils.getResourceStr(nodeManager.getAvailable())).append("<br>");
        sb.append("Used: ").append(CommonUtils.getResourceStr(nodeManager.getUsed())).append("<br>");
        sb.append("Version: 1.0<br>");
        handleResponse(exchange, sb.toString());
    }

    private void handleResponse(HttpExchange httpExchange, String responsetext) throws IOException {
        String responseContentStr = "<html><head> <title>NodeManager information </title> </head><body>" +
                responsetext +
                "</body></html>";
        sendMessage(httpExchange, responseContentStr);
    }

    private static void sendMessage(HttpExchange httpExchange, String responseContentStr) throws IOException {
        byte[] responseContentByte = responseContentStr.getBytes(StandardCharsets.UTF_8);

        httpExchange.getResponseHeaders().add("Content-Type:", "text/html;charset=utf-8");

        httpExchange.sendResponseHeaders(200, responseContentByte.length);

        OutputStream out = httpExchange.getResponseBody();
        out.write(responseContentByte);
        out.flush();
        out.close();
    }
}
