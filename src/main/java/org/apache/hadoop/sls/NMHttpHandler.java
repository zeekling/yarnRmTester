package org.apache.hadoop.sls;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.hadoop.sls.util.CommonUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class NMHttpHandler implements HttpHandler {


    private final YarnFakeNodeManager nodeManager;

    public NMHttpHandler(YarnFakeNodeManager nodeManager) {
        this.nodeManager = nodeManager;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
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
        byte[] responseContentByte = responseContentStr.getBytes(StandardCharsets.UTF_8);

        httpExchange.getResponseHeaders().add("Content-Type:", "text/html;charset=utf-8");

        httpExchange.sendResponseHeaders(200, responseContentByte.length);

        OutputStream out = httpExchange.getResponseBody();
        out.write(responseContentByte);
        out.flush();
        out.close();
    }
}
