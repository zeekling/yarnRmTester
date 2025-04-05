package org.apache.hadoop.sls.util;

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

}
