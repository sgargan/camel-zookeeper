package org.apache.camel.component.zookeeper.operations;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.camel.component.zookeeper.operations.ExistenceChangedOperation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ExistenceChangedOperationTest extends ZooKeeperTestSupport {

    @Test
    public void getStatsWhenNodeIsCreated() throws Exception {
        String path = "/doesNotYetExist";
        ExistenceChangedOperation future = setupMonitor(path);

        client.create(path, "This is a test");
        assertEquals(path, future.get().getResult());
        assertNotNull(future.get().getStatistics());
    }

    @Test
    public void getsNotifiedWhenNodeIsDeleted() throws Exception {
        String path = "/soonToBeDeleted";
        client.create(path, "This is a test");
        ExistenceChangedOperation future = setupMonitor(path);

        client.delete(path);
        assertEquals(path, future.get().getResult());
        assertNull(future.get().getStatistics());
    }

    private ExistenceChangedOperation setupMonitor(String path) throws KeeperException, InterruptedException {
        ZooKeeper connection = getConnection();
        ExistenceChangedOperation future = new ExistenceChangedOperation(connection, path);
        connection.exists(path, future);
        return future;
    }
}
