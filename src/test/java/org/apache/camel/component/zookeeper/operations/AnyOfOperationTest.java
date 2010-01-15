package org.apache.camel.component.zookeeper.operations;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class AnyOfOperationTest extends ZooKeeperTestSupport {


    @Test
    public void testExistsOrWaitsWhenNodeExists() throws Exception {
        String node = "/cogito";
        client.create(node, "ergo sum");
        AnyOfOperations operation = getExistsOrWaitOperation(node);
        assertEquals(node, operation.get().getResult());
    }


    @Test
    public void testExistsOrWaitsWhenNodeDoesNotExist() throws Exception {
        String node = "/chapter-one";
        AnyOfOperations operation = getExistsOrWaitOperation(node);
        Thread.sleep(1000);
        client.create(node, "I am born");
        assertEquals(node, operation.get().getResult());
    }

    private AnyOfOperations getExistsOrWaitOperation(String node) {
        ZooKeeper connection = getConnection();
        AnyOfOperations operation = new AnyOfOperations(node, new ExistsOperation(connection, node),
                                                        new ExistenceChangedOperation(connection, node));
        return operation;
    }
}
