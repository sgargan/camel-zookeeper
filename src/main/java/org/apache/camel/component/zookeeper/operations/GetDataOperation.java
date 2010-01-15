package org.apache.camel.component.zookeeper.operations;

import static java.lang.String.format;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * <code>GetDataOperation</code> defines an operation to immediately retrieve
 * the data associated with a given ZooKeeper node.
 *
 * @author sgargan
 */
public class GetDataOperation extends ZooKeeperOperation<byte[]> {

    public GetDataOperation(ZooKeeper connection, String node) {
       super(connection, node);
    }

    public OperationResult<byte[]> getResult() {
        try {
            Stat statistics = new Stat();

            if (log.isDebugEnabled()) {
                if (log.isTraceEnabled()) {
                    log.trace(format("Received data from '%s' path with statistics '%s'", node, statistics));
                } else {
                    log.debug(format("Received data from '%s' path ", node));
                }
            }
            return new OperationResult<byte[]>(connection.getData(node, true, statistics), statistics);
        } catch (Exception e) {
            return new OperationResult<byte[]>(e);
        }
    }

}
