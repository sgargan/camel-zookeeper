package org.apache.camel.component.zookeeper.monitors;

import static java.lang.String.format;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class DataChangedMonitor extends WatchFuture<byte[]> {

    private final Logger log = Logger.getLogger(getClass());

    public DataChangedMonitor(ZooKeeper connection, String znode) {
        super(connection, znode, EventType.NodeDataChanged, EventType.NodeDeleted);
    }

    @Override
    protected WatchFutureResult<byte[]> getResult(WatchedEvent event) {
        try {
            Stat statistics = new Stat();

            if (log.isDebugEnabled()) {
                if (log.isTraceEnabled()) {
                    log.trace(format("Received data from '%s' path with statistics '%s'", getNode(), statistics));
                } else {
                    log.debug(format("Received data from '%s' path ", getNode()));
                }
            }
            return new WatchFutureResult<byte[]>(connection.getData(getNode(), true, statistics), statistics);
        } catch (Exception e) {
            return new WatchFutureResult<byte[]>(e);
        }
    }

}
