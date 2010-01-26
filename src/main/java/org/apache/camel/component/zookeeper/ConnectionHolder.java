package org.apache.camel.component.zookeeper;

import static java.lang.String.format;

import java.util.concurrent.CountDownLatch;

import org.apache.camel.RuntimeCamelException;
import org.apache.camel.util.ObjectHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * <code>ConnectionHolder</code> watches for Connection based events from
 * {@link ZooKeeper} and can be used to block until a connection has been
 * established.
 */
public class ConnectionHolder implements Watcher {

    private static final transient Log log = LogFactory.getLog(ConnectionHolder.class);

    private CountDownLatch connectionLatch = new CountDownLatch(1);

    private ZooKeeperConfiguration configuration;

    private ZooKeeper zookeeper;

    public ConnectionHolder(ZooKeeperConfiguration configuration) {
        this.configuration = configuration;
    }

    public ZooKeeper getZooKeeper() {
        if (configuration.getConnectString() == null) {
            throw new RuntimeCamelException("Cannot create ZooKeeper connection as connection string is null. Have servers been configured?");
        }
        try {
            if (configuration.getSessionId() > 0 && configuration.getSessionPassword() != null) {
                zookeeper = new ZooKeeper(configuration.getConnectString(), configuration.getTimeout(), this, configuration.getSessionId(), configuration.getSessionPassword());
            } else {
                zookeeper = new ZooKeeper(configuration.getConnectString(), configuration.getTimeout(), this);
            }
        } catch (Exception e) {
            ObjectHelper.wrapRuntimeCamelException(e);
        }
        awaitConnection();
        return zookeeper;
    }

    public boolean isConnected() {
        return connectionLatch.getCount() == 0;
    }

    public void awaitConnection() {
        if (log.isDebugEnabled()) {
            log.debug(format("Awaiting Connection event from Zookeeper cluster %s", configuration.getConnectString()));
        }
        try {
            connectionLatch.await();
        } catch (InterruptedException e) {
            ObjectHelper.wrapRuntimeCamelException(e);
        }
    }

    public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
            connectionLatch.countDown();
        }
        connectionLatch.countDown();
    }

    public void closeConnection() {
        try {
            zookeeper.close();
        } catch (InterruptedException e) {
            log.error("Error closing zookeeper connection.", e);
        }
    }
}
