package org.apache.camel.component.zookeeper.operations;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

/**
 * <code>DataChangedOperation</code> is an watch driven operation. It will wait
 * for an watched event indicating that the data contained in a given
 * node has changed before optionally retrieving the changed data.
 */
@SuppressWarnings("unchecked")
public class DataChangedOperation extends FutureEventDrivenOperation<byte[]> {

    private boolean getChangedData;

    public DataChangedOperation(ZooKeeper connection, String znode, boolean getChangedData) {
        super(connection, znode, EventType.NodeDataChanged, EventType.NodeDeleted);
        this.getChangedData = getChangedData;
    }

    @Override
    protected void installWatch() {
        connection.getData(getNode(), this, new DataCallback() {
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            }
        }, null);
    }

    public OperationResult<byte[]> getResult() {
        return getChangedData ? new GetDataOperation(connection, getNode()).getResult() : null;
    }

    protected final static Class[] constructorArgs = {ZooKeeper.class, String.class, boolean.class};

    @Override
    public ZooKeeperOperation createCopy() throws Exception {
        return getClass().getConstructor(constructorArgs).newInstance(new Object[] {connection, node, getChangedData});
    }
}
