package org.apache.camel.component.zookeeper.operations;

import static java.lang.String.format;

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
            if (log.isDebugEnabled()) {
                if (log.isTraceEnabled()) {
                    log.trace(format("Set data of node '%s'  with '%d' bytes of data, retrieved statistics '%s' ",
                                     node, data != null ? data.length : 0, statistics));
                } else {
                    log.debug(format("Set data of node '%s' with '%d' bytes of data", node, data != null ? data.length : 0));
                }
            }
            return new OperationResult<byte[]>(data, statistics);
        } catch (Exception e) {
            return new OperationResult<byte[]>(e);
        }
    }

    public void setVersion(int version) {
        this.version = version;
    }

}
