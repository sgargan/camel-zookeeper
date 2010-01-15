package org.apache.camel.component.zookeeper.operations;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ExistenceOperationTest extends ZooKeeperTestSupport{

    @Test
    public void okWhenNodeMustExistAndDoesExists() throws Exception {
        client.create("/ergosum", "not a figment of your imagination");
        ZooKeeper connection = getConnection();

        ExistsOperation exists = new ExistsOperation(connection, "/ergosum");
        assertTrue(exists.get().isOk());
    }

    @Test
    public void notOkWhenNodeMustExistButDoesNotExist() throws Exception {
        ZooKeeper connection = getConnection();
        ExistsOperation exists = new ExistsOperation(connection, "/figment");
        assertFalse(exists.get().isOk());
    }


    @Test
    public void okWhenNodeMustNotExistAndDoesNotExists() throws Exception {
        ZooKeeper connection = getConnection();
        ExistsOperation exists = new ExistsOperation(connection, "/figment", false);
        assertTrue(exists.get().isOk());
    }

    @Test
    public void notOkWhenNodeMustExistButDoesExist() throws Exception {
        client.create("/i-exist-too", "not a figment of your imagination");
        ZooKeeper connection = getConnection();

        ExistsOperation exists = new ExistsOperation(connection, "/i-exist-too", false);
        assertFalse(exists.get().isOk());
    }
}
