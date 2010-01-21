package org.apache.camel.component.zookeeper.operations;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * <code>SetDataOperation</code> sets the content of a given ZooKeeper node.
 *
 * @author sgargan
 */
public class SetDataOperation extends ZooKeeperOperation<byte[]> {

    private byte[] data;

    private int version = -1;

    public SetDataOperation(ZooKeeper connection, String node, byte[] data) {
        super(connection, node);
        this.data = data;
    }

    public OperationResult<byte[]> getResult() {
        try {
            Stat statistics = connection.setData(node, data, version);
            return new OperationResult<byte[]>(data, statistics);
        } catch (Exception e) {
            return new OperationResult<byte[]>(e);
        }
    }

    public void setVersion(int version) {
        this.version = version;
    }

}
