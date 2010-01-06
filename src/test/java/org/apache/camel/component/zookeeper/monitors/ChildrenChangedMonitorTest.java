package org.apache.camel.component.zookeeper.monitors;

import java.util.Arrays;
import java.util.List;

import org.apache.camel.component.zookeeper.ZooKeeperTestSupport;
import org.apache.camel.component.zookeeper.monitors.ChildrenChangedMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Test;

public class ChildrenChangedMonitorTest extends ZooKeeperTestSupport {


    @Test
    public void getsListingWhenNodeIsCreated() throws Exception {
        String path = "/parent";
        client.create(path , null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZooKeeper connection = getConnection();
        ChildrenChangedMonitor future = new ChildrenChangedMonitor(connection, path);
        connection.getChildren(path, future, null);

        client.create(path + "/child1", null,  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(createChildListing("child1"), future.get().getResult());
    }

    private List<String> createChildListing(String... children) {
        return Arrays.asList(children);
    }

    @Test
    public void getsNotifiedWhenNodeIsDeleted() throws Exception {

        String path = "/parent2";
        client.create(path , null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        client.create(path + "/child1", null,  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        ZooKeeper connection = getConnection();
        ChildrenChangedMonitor future = new ChildrenChangedMonitor(connection, path);
        connection.getChildren(path, future, null);

        client.delete(path + "/child1");
        assertEquals(createChildListing(), future.get().getResult());
    }
}
