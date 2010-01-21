package org.apache.camel.component.zookeeper.operations;

import java.util.Collections;
import java.util.List;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;

public class CreateOperationTest extends ZooKeeperTestSupport{

    private ZooKeeper connection;

    @Before
    public void setupConnection()
    {
        connection = getConnection();
    }

    @Test
    public void createBasic() throws Exception {

        CreateOperation create = new CreateOperation(connection, "/one");

        OperationResult<String> result = create.get();
        assertEquals("/one", result.getResult());

        verifyNodeContainsData("/one", null);
    }

    @Test
    public void createBasicWithData() throws Exception {
        CreateOperation create = new CreateOperation(connection, "/two");
        create.setData(TestPayload.getBytes());

        OperationResult<String> result = create.get();

        assertEquals("/two", result.getResult());
        verifyNodeContainsData("/two", TestPayloadBytes);
    }

    @Test
    public void createSequencedNodeToTestCreateMode() throws Exception {
        CreateOperation create = new CreateOperation(connection, "/three");
        create.setData(TestPayload.getBytes());
        create.setCreateMode(CreateMode.EPHEMERAL_SEQUENTIAL);

        OperationResult<String> result = create.get();
        assertEquals("/three0000000002", result.getResult());

        verifyNodeContainsData("/three0000000002", TestPayloadBytes);
    }

    @Test
    public void createNodeWithSpecificAccess() throws Exception {
        CreateOperation create = new CreateOperation(connection, "/four");
        create.setData(TestPayload.getBytes());
        List<ACL> perms = Collections.singletonList(new ACL(Perms.CREATE, Ids.ANYONE_ID_UNSAFE));
        create.setPermissions(perms);

        OperationResult<String> result = create.get();
        assertEquals("/four", result.getResult());

        verifyAccessControlList("/four", perms);
    }
}
