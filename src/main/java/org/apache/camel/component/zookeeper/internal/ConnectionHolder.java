package org.apache.camel.component.zookeeper.internal;

import static java.lang.String.format;

import java.util.concurrent.CountDownLatch;

import org.apache.camel.component.zookeeper.ZooKeeperConfiguration;
import org.apache.camel.util.ObjectHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * <code>ConnectionHolder</code> watches for Connection based events from
 * {@link ZooKeeper}.
 */
public class ConnectionHolder implements Watcher {

    private static final transient Log log = LogFactory.getLog(ConnectionHolder.class);

    private CountDownLatch connectionLatch = new CountDownLatch(1);

    private ZooKeeperConfiguration configuration;

    private ZooKeeper zookeeper;

    public ConnectionHolder(ZooKeeperConfiguration configuration){
        this.configuration = configuration;
        try {
            zookeeper = new ZooKeeper(configuration.getConnectString(), configuration.getTimeout(), this, configuration.getSessionId(), configuration.getSessionPassword());
        } catch (Exception e) {
            ObjectHelper.wrapRuntimeCamelException(e);
        }
    }

    public ZooKeeper getZooKeeper(){
        awaitConnection();
        return zookeeper;
    }

    public boolean isConnected() {
        return connectionLatch.getCount() == 0;
    }

    public void awaitConnection()  {
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
            if (log.isDebugEnabled()) {
                log.debug(format("Connected to Zookeeper cluster %s", configuration.getConnectString()));
            }
            connectionLatch.countDown();
        }
        connectionLatch.countDown();
    }
}
