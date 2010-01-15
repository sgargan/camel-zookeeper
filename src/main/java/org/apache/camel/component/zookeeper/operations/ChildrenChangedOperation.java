package org.apache.camel.component.zookeeper.operations;

import static java.lang.String.format;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ChildrenChangedOperation</code> is a zookeeper event driven operation.
 * It will wait for an event indicating that the children associated with a
 * given node have been modified before retrieving the changed list.
 *
 * @author sgargan
 */
public class ChildrenChangedOperation extends FutureEventDrivenOperation<List<String>> {

    public ChildrenChangedOperation(ZooKeeper connection, String znode) {
        super(connection, znode, EventType.NodeChildrenChanged);
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
        Stat statistics = new Stat();
        try {
            List<String> children = connection.getChildren(getNode(), true, statistics);
            if (log.isDebugEnabled()) {
                if (log.isTraceEnabled()) {
                    log.trace(format("Child listing of path '%s' with statistics '%s'\nChildren are: %s ", node, statistics, children));
                } else {
                    log.debug(format("Child listing of path '%s'", node));
                }
            }
            return new OperationResult<List<String>>(children, statistics);
        } catch (Exception e) {
            return new OperationResult<List<String>>(e);
        }
    }

}
