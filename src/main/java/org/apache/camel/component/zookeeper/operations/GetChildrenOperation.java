package org.apache.camel.component.zookeeper.operations;

import static java.lang.String.format;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * <code>GetChildrenOperation</code> is a basic ZooKeeper operation used to retrieve the
 * list of children belonging to a given ZooKeeper node.
 */
public class GetChildrenOperation extends ZooKeeperOperation<List<String>>{

    public GetChildrenOperation(ZooKeeper connection, String node) {
        super(connection, node);
     }

     public OperationResult<List<String>> getResult() {
         try {
             Stat statistics = new Stat();

             List<String> children = connection.getChildren(node, true, statistics);
             if (log.isDebugEnabled()) {
                 if (log.isTraceEnabled()) {
                     log.trace(format("Received children from '%s' path with statistics '%s'", node, statistics));
                 } else {
                     log.debug(format("Received children from '%s' path ", node));
                 }
             }
             return new OperationResult<List<String>>(children, statistics);
         } catch (Exception e) {
             return new OperationResult<List<String>>(e);
         }
     }
}
