package org.apache.camel.component.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZookeeperConnectionManagerTest extends ZooKeeperTestSupport{

    @Test
    public void shouldWaitForConnection() {
        ZooKeeperConfiguration config = new ZooKeeperConfiguration();
        config.addZookeeperServer("localhost:39913");

        ZooKeeperComponent component = new ZooKeeperComponent(config);
        component.setConfiguration(config);
        component.setCamelContext(context);

        ZooKeeperEndpoint zep = new ZooKeeperEndpoint("zoo:someserver/this/is/a/path", component, config);

        ZooKeeperConnectionManager zkcm = new ZooKeeperConnectionManager(zep);
        ZooKeeper zk = zkcm.getConnection();
        assertEquals(zk.getState().CONNECTED, zk.getState());
    }
}
