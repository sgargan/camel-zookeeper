package org.apache.camel.component.zookeeper.monitors;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.camel.component.zookeeper.monitors.DataChangedMonitor;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class DataChangedMonitorTest extends ZooKeeperTestSupport{

    @Test
    public void getsDataWhenNodeChanges() throws Exception {
        client.create("/datachanged", "this won't hurt a bit");
        ZooKeeper connection = getConnection();

        DataChangedMonitor future = new DataChangedMonitor(connection, "/datachanged");
        connection.getData("/datachanged", future, null);

        client.setData("/datachanged", "Really trust us", -1);
        assertArrayEquals("Really trust us".getBytes(), future.get().getResult());
    }


    @Test
    public void getsNotifiedWhenNodeIsDeleted() throws Exception {

        client.create("/existedButWasDeleted", "this won't hurt a bit");
        ZooKeeper connection = getConnection();
        DataChangedMonitor future = new DataChangedMonitor(connection, "/existedButWasDeleted");
        connection.getData("/existedButWasDeleted", future, null);
        client.delete("/existedButWasDeleted");
        assertEquals(null, future.get().getResult());
    }
}
