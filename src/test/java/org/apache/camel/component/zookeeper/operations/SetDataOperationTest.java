package org.apache.camel.component.zookeeper.operations;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class SetDataOperationTest extends ZooKeeperTestSupport{

    private ZooKeeper connection;

    @Before
    public void setupConnection()
    {
        connection = getConnection();
    }

    @Test
    public void setData() throws Exception {

        client.create("/one", TestPayload);
        SetDataOperation operation = new SetDataOperation(connection, "/one", "Updated".getBytes());
        verifyNodeContainsData("/one", "Updated".getBytes());
        assertEquals(2, operation.get().getStatistics().getVersion());
    }

    @Test
    public void setSpecificVersionOfData() throws Exception {

        client.create("/two", TestPayload);
        SetDataOperation operation = new SetDataOperation(connection, "/two", "Updated".getBytes());
        operation.setVersion(2);
        verifyNodeContainsData("/one", "Updated".getBytes());
        assertEquals(2, operation.get().getStatistics().getVersion());
    }


}
