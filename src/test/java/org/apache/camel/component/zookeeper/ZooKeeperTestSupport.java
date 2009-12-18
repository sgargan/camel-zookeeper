package org.apache.camel.component.zookeeper;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import org.apache.camel.test.junit4.CamelTestSupport;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZooKeeperTestSupport extends CamelTestSupport {

    protected static TestZookeeperServer server;

    @BeforeClass
    public static void setupTestServer() throws Exception {
        System.err.println("Starting test server");
        server = new TestZookeeperServer(getServerPort());
    }

    @AfterClass
    public static void shutdownServer() throws Exception {
        server.shutdown();
    }

    protected static int getServerPort() {
        return 39913;
    }

    protected int getTestClientSessionTimeout() {
        return 5000;
    }

    public static class TestZookeeperServer {
        private NIOServerCnxn.Factory connectionFactory;
        private ZooKeeperServer zkServer;

        public TestZookeeperServer(int clientPort) throws Exception {
            zkServer = new ZooKeeperServer();
            File dataDir = new File("./target/zookeeper/log");
            File snapDir = new File("./target/zookeeper/data");

            FileTxnSnapLog ftxn = new FileTxnSnapLog(dataDir, snapDir);
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

    public class TestClient implements Watcher {

        private final Logger log = Logger.getLogger(getClass());

        private ZooKeeper zk;

        private CountDownLatch connected = new CountDownLatch(1);

        public TestClient() throws Exception {
            zk = new ZooKeeper("localhost:" + getServerPort(), getTestClientSessionTimeout(), this);
            connected.await();
        }

        public byte[] waitForNodeChange(String node) throws Exception {
            Stat stat = new Stat();
            byte[] data = zk.getData(node, this, stat);
            return data;
        }

        public void stop() throws Exception {
            zk.close();
        }

        public void addData(String znode, String data) throws Exception {
            String created = zk.create(znode, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if (log.isInfoEnabled()) {
                log.info(String.format("Created znode named '%s'", created));
            }
        }

        public void process(WatchedEvent event) {

            if (event.getState() == KeeperState.SyncConnected) {
                if (log.isInfoEnabled()) {
                    log.info("TestClient connected");
                }
                connected.countDown();
            } else {
                System.err.println(event.toString());
            }
        }
    }

}
