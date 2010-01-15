package org.apache.camel.component.zookeeper.operations;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class DataChangedOperationTest extends ZooKeeperTestSupport{

    @Test
    public void getsDataWhenNodeChanges() throws Exception {
        client.create("/datachanged", "this won't hurt a bit");
        ZooKeeper connection = getConnection();

        DataChangedOperation future = new DataChangedOperation(connection, "/datachanged", true);
        connection.getData("/datachanged", future, null);

        client.setData("/datachanged", "Really trust us", -1);
        assertArrayEquals("Really trust us".getBytes(), future.get().getResult());
    }


    @Test
    public void getsNotifiedWhenNodeIsDeleted() throws Exception {

        client.create("/existedButWasDeleted", "this won't hurt a bit");
        ZooKeeper connection = getConnection();

        DataChangedOperation future = new DataChangedOperation(connection, "/existedButWasDeleted", true);
        connection.getData("/existedButWasDeleted", future, null);

        client.delete("/existedButWasDeleted");
        assertEquals(null, future.get().getResult());
    }
}
