package org.apache.camel.component.zookeeper.operations;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ChildrenChangedOperation</code> is an event driven operation.
 * It will wait for an event indicating that the children associated with a
 * given node have been modified before retrieving the changed list.
 *
 */
public class ChildrenChangedOperation extends FutureEventDrivenOperation<List<String>> {

    private boolean getChangedListing;

    public ChildrenChangedOperation(ZooKeeper connection, String znode) {
        this(connection, znode, true);
    }

    public ChildrenChangedOperation(ZooKeeper connection, String znode, boolean getChangedListing) {
        super(connection, znode, EventType.NodeChildrenChanged);
        this.getChangedListing = getChangedListing;
    }

    @Override
    protected void installWatch() {
        connection.getChildren(getNode(), this, new Children2Callback() {
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            }
        }, null);
    }

    @Override
    public OperationResult<List<String>> getResult() {
        return getChangedListing ? new GetChildrenOperation(connection, node).getResult() : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ZooKeeperOperation createCopy() throws Exception {
        ChildrenChangedOperation copy = (ChildrenChangedOperation) super.createCopy();
        copy.getChangedListing = getChangedListing;
        return copy;
    }
}
