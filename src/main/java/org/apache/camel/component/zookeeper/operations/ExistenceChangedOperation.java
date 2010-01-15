package org.apache.camel.component.zookeeper.operations;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

public class ExistenceChangedOperation extends FutureEventDrivenOperation<String> {

    public ExistenceChangedOperation(ZooKeeper connection, String znode) {
        super(connection, znode, EventType.NodeCreated, EventType.NodeDeleted);
    }

    @Override
    protected void installWatch(){
        connection.exists(getNode(), this, new StatCallback() {
            public void processResult(int rc, String path, Object ctx, Stat stat) {
            }
        }, null);
        if (log.isDebugEnabled()) {
            log.debug("Installed exists watch");
        }
    }

    @Override
    public OperationResult<String> getResult() {
        try {
            String path = getNode();
            Stat statistics = connection.exists(path, true);
            return new OperationResult<String>(path, statistics);
        } catch (Exception e) {
            return new OperationResult<String>(e);
        }
    }
}
