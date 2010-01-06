package org.apache.camel.component.zookeeper.monitors;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class ExistenceChangedMonitor extends WatchFuture<String> {

    public ExistenceChangedMonitor(ZooKeeper connection, String znode) {
        super(connection, znode, EventType.NodeCreated, EventType.NodeDeleted);
    }

    @Override
    protected WatchFutureResult<String> getResult(WatchedEvent event) {
        try {
            String path = getNode();
            Stat statistics = connection.exists(path, true);
            return new WatchFutureResult<String>(path, statistics);
        } catch (Exception e) {
            return new WatchFutureResult<String>(e);
        }
    }
}
