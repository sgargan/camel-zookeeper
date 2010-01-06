package org.apache.camel.component.zookeeper.monitors;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class ChildrenChangedMonitor extends WatchFuture<List<String>> {

    public ChildrenChangedMonitor(ZooKeeper connection, String znode) {
        super(connection, znode, EventType.NodeChildrenChanged);
    }

    @Override
    protected void beginNodeWatch() throws InterruptedException{
        connection.getChildren(getNode(), this);
    }

    @Override
    protected WatchFutureResult<List<String>> getResult(WatchedEvent event) {
        Stat statistics = new Stat();
        try {
            List<String> children = connection.getChildren(getNode(), true, statistics);
            return new WatchFutureResult<List<String>>(children, statistics);
        } catch (Exception e) {
            return new WatchFutureResult<List<String>>(e);
        }
    }

}
