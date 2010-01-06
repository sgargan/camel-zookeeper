package org.apache.camel.component.zookeeper.monitors;

import java.util.ArrayList;
import java.util.List;

public class CompositeMonitor {

    private List<WatchFuture> operations = new ArrayList<WatchFuture>();

    public void addOperation(WatchFuture monitor){
        operations.add(monitor);
    }
}
