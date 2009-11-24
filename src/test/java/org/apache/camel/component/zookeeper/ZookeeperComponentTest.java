package org.apache.camel.component.zookeeper;

import java.io.File;

import org.apache.camel.test.CamelTestSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZookeeperComponentTest extends CamelTestSupport {

    public static TestZookeeperServer zookeeper;

    private static int clientPort = 39700;

    private static int sessionTimeout = 5000;



    @BeforeClass
    public static void startZookeeperServer() throws Exception {
        zookeeper = new TestZookeeperServer(clientPort);
    }

    @AfterClass
    public static void stopZookeeperServer() throws Exception {
        zookeeper.shutdown();
    }


    public synchronized void testSimpleConnection() throws Exception {
        startZookeeperServer();
        TestClient cl = new TestClient();
        cl.addData("/camel", "This is a test");
        wait();
    }

    public static class TestZookeeperServer {
        private NIOServerCnxn.Factory connectionFactory;
        private ZooKeeperServer zkServer;

        public TestZookeeperServer(int clientPort) throws Exception {
            zkServer = new ZooKeeperServer();

            FileTxnSnapLog ftxn = new FileTxnSnapLog(new File("./target/zookeeper/log"), new File("./target/zookeeper/data"));
            zkServer.setTxnLogFactory(ftxn);
            zkServer.setTickTime(5);
            connectionFactory = new NIOServerCnxn.Factory(clientPort, 5);
            connectionFactory.startup(zkServer);

            while (!zkServer.isRunning()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }

        public void shutdown() throws Exception {
            connectionFactory.shutdown();
            connectionFactory.join();
            if (zkServer.isRunning()) {
                zkServer.shutdown();
            }
        }
    }

    public static class TestClient implements Watcher {

        private ZooKeeper zk;

        public TestClient() throws Exception {
            zk = new ZooKeeper("localhost:" + clientPort, sessionTimeout, this);
        }

        public void stop() throws Exception {
            zk.close();
        }

        public void addData(String znode, String data) throws Exception {
            zk.create(znode, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        public void process(WatchedEvent event) {
            System.err.println(event.toString());
        }
    }
}
