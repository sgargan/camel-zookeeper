package org.apache.camel.component.zookeeper.operations;


import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ChildrenChangedOperationTest extends ZooKeeperTestSupport {

    @Test
    public void getsListingWhenNodeIsCreated() throws Exception {
        String path = "/parent";
        client.createPersistent(path, null);

        ZooKeeper connection = getConnection();
        ChildrenChangedOperation future = new ChildrenChangedOperation(connection, path);
        connection.getChildren(path, future, null);

        client.createPersistent(path + "/child1", null);
        assertEquals(createChildListing("child1"), future.get().getResult());
    }




    @Test
    public void getsNotifiedWhenNodeIsDeleted() throws Exception {

        String path = "/parent2";
        client.createPersistent(path, null);
        client.createPersistent(path + "/child1", null);

        ZooKeeper connection = getConnection();
        ChildrenChangedOperation future = new ChildrenChangedOperation(connection, path);
        connection.getChildren(path, future, null);

        client.delete(path + "/child1");
        assertEquals(createChildListing(), future.get().getResult());
    }

    @Test
    public void getsNoListingWhenOnlyChangeIsRequired() throws Exception {
        String path = "/parent3";
        client.createPersistent(path, null);

        ZooKeeper connection = getConnection();
        ChildrenChangedOperation future = new ChildrenChangedOperation(connection, path, false);
        connection.getChildren(path, future, null);

        client.createPersistent(path + "/child3", null);
        assertEquals(null, future.get());
    }
}
