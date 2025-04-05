package org.apache.hadoop.sls.util;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CommonUtils {


    public static void waitFutures(List<Future<?>> futures) {
        while (true) {
            Iterator<Future<?>> it = futures.iterator();
            while (it.hasNext()) {
                Future<?> future = it.next();
                try {
                    future.get(100, TimeUnit.MILLISECONDS);
                    it.remove();
                } catch (Exception e) {
                  // skip
                }
            }
            if (futures.isEmpty()) {
                return;
            }
        }
    }

    public static String getResourceStr(Resource resource) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"memory:").append(resource.getMemorySize()).append(", vCores:").append(resource.getVirtualCores());
        for(int i = 2; i < resource.getResources().length; ++i) {
            ResourceInformation ri = resource.getResources()[i];
            if (ri.getValue() != 0L) {
                sb.append(", ").append(ri.getName()).append(": ").append(ri.getValue()).append(ri.getUnits());
            }
        }
        sb.append("\" ");
        return sb.toString();
    }

}
