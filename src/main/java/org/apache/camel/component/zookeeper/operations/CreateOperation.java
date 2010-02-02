package org.apache.camel.component.zookeeper.operations;

import static java.lang.String.format;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public class CreateOperation extends ZooKeeperOperation<String> {

    private static final List<ACL> DefaultPermissions = Ids.OPEN_ACL_UNSAFE;

    private static final CreateMode DefaultMode = CreateMode.EPHEMERAL;

    private byte[] data;

    private List<ACL> permissions = DefaultPermissions;

    private CreateMode createMode = DefaultMode;

    public CreateOperation(ZooKeeper connection, String node) {
        super(connection, node);
    }

    @Override
    public OperationResult<String> getResult() {
        try {
            String created = connection.create(node, data, permissions, createMode);
            if (log.isDebugEnabled()) {
                log.debug(format("Created node '%s' using mode '%s'", created, createMode));
            }
            return new OperationResult<String>(created, new Stat()); // for completeness return empty stats.
        } catch (Exception e) {
            return new OperationResult<String>(e);
        }
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setPermissions(List<ACL> permissions) {
        this.permissions = permissions;
    }

    public void setCreateMode(CreateMode createMode) {
        this.createMode = createMode;
    }


}
