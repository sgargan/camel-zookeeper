package org.apache.camel.component.zookeeper;

public class ZookeeperComponentTest extends ZooKeeperTestSupport {

    public static TestZookeeperServer zookeeper;

    public synchronized void estSimpleConnection() throws Exception {
        // startZookeeperServer();
        TestClient cl = new TestClient();
        cl.addData("/camel", "This is a test");

        TestClient reader = new TestClient();
        String data = new String(cl.waitForNodeChange("/camel"));

        wait();
    }

}
