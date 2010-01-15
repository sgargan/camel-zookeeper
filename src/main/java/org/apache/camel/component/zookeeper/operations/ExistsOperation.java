package org.apache.camel.component.zookeeper.operations;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ExistsOperation extends ZooKeeperOperation<String> {

    private boolean mustExist;

    public ExistsOperation(ZooKeeper connection, String node) {
        this(connection, node, true);
    }

    public ExistsOperation(ZooKeeper connection, String node, boolean mustExist) {
        super(connection, node);
        this.mustExist = mustExist;
    }

    @Override
    public OperationResult<String> getResult() {
        try {
            Stat statistics = connection.exists(node, true);
            return new OperationResult<String>(node, statistics, isOk(statistics));
        } catch (Exception e) {
            return new OperationResult<String>(e);
        }
    }

    private boolean isOk(Stat statistics) {
        boolean ok = false;
        if (mustExist) {
            ok = statistics != null;
        } else {
            ok = statistics == null;
        }
        return ok;
    }

}
