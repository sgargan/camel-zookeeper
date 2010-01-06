package org.apache.camel.component.zookeeper.monitors;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.camel.component.zookeeper.monitors.ExistenceChangedMonitor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ExistenceChangedMonitorTest extends ZooKeeperTestSupport {

    @Test
    public void getStatsWhenNodeIsCreated() throws Exception {
        String path = "/doesNotYetExist";
        ExistenceChangedMonitor future = setupMonitor(path);

        client.create(path, "This is a test");
        assertEquals(path, future.get().getResult());
        assertNotNull(future.get().getStatistics());
    }

    @Test
    public void getsNotifiedWhenNodeIsDeleted() throws Exception {
        String path = "/soonToBeDeleted";
        client.create(path, "This is a test");
        ExistenceChangedMonitor future = setupMonitor(path);

        client.delete(path);
        assertEquals(path, future.get().getResult());
        assertNull(future.get().getStatistics());
    }

    private ExistenceChangedMonitor setupMonitor(String path) throws KeeperException, InterruptedException {
        ZooKeeper connection = getConnection();
        ExistenceChangedMonitor future = new ExistenceChangedMonitor(connection, path);
        connection.exists(path, future);
        return future;
    }
}
