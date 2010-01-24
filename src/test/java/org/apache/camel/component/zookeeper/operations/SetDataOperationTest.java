package org.apache.camel.component.zookeeper.operations;

import java.util.concurrent.ExecutionException;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class SetDataOperationTest extends ZooKeeperTestSupport {

    private ZooKeeper connection;

    @Before
    public void setupConnection() {
        connection = getConnection();
    }

    @Test
    public void setData() throws Exception {

        client.create("/one", TestPayload);
        SetDataOperation operation = new SetDataOperation(connection, "/one", "Updated".getBytes());
        OperationResult<byte[]> result = operation.get();
        verifyNodeContainsData("/one", "Updated".getBytes());
        assertEquals(1, result.getStatistics().getVersion());
    }

    @Test
    public void setSpecificVersionOfData() throws Exception {

        client.create("/two", TestPayload);
        for (int x = 0; x < 10; x++) {
            byte[] payload = ("Updated_" + x).getBytes();
            updateDataOnNode("/two", payload, x, x+1);
            verifyNodeContainsData("/two", payload);
        }
    }

    @Test
    public void setWithNull() throws Exception {

        client.create("/three", TestPayload);
        updateDataOnNode("/three", null, -1, 1);

    }

    private void updateDataOnNode(String node, byte[] payload, int version, int expectedVersion) throws InterruptedException, ExecutionException, Exception {
        SetDataOperation operation = new SetDataOperation(connection, node, payload);
        operation.setVersion(version);
        OperationResult<byte[]> result = operation.get();
        assertEquals(null, result.getException());
        verifyNodeContainsData(node, payload);
        assertEquals(expectedVersion, result.getStatistics().getVersion());
    }
}
