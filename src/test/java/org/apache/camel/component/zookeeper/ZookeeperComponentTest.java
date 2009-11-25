package org.apache.camel.component.zookeeper;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.camel.test.CamelTestSupport;
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

public class ZookeeperComponentTest extends CamelTestSupport {

    public static TestZookeeperServer zookeeper;

    private static int clientPort = 2181;

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
        // startZookeeperServer();
        TestClient cl = new TestClient();
        cl.addData("/camel", "This is a test");

        TestClient reader = new TestClient();
        String data = new String(cl.waitForNodeChange("/camel"));
        System.err.println(data);

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

        private final Logger log = Logger.getLogger(getClass());

        private ZooKeeper zk;

        private Map<String, CountDownLatch> nodeLatches = new HashMap<String, CountDownLatch>();

        private CountDownLatch connected = new CountDownLatch(1);

        public TestClient() throws Exception {
            zk = new ZooKeeper("localhost:" + clientPort, sessionTimeout, this);
            connected.await();
        }

        public byte[] waitForNodeChange(String node) throws Exception{
            Stat stat = new Stat();
            byte[] data = zk.getData(node, this, stat);
            return data;
        }

        public void stop() throws Exception {
            zk.close();
        }

        public void addData(String znode, String data) throws Exception {
            String created = zk.create(znode, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if(log.isInfoEnabled())
            {
                log.info(String.format("Created znode named '%s'", created));
            }
        }

        public void process(WatchedEvent event) {

            if(event.getState() == KeeperState.SyncConnected){
                if(log.isInfoEnabled())
                {
                    log.info("TestClient connected");
                }
                connected.countDown();
            }
            else
            {
                System.err.println(event.toString());
            }

        }
    }
}
