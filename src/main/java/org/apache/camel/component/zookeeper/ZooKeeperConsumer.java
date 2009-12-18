package org.apache.camel.component.zookeeper;

import static java.lang.String.format;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;

public class ZooKeeperConsumer extends DefaultConsumer {

    private ZooKeeperConnectionManager connectionManager;

    private ZooKeeper connection;

    private ZooKeeperConfiguration configuration;

    public ZooKeeperConsumer(ZooKeeperEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.connectionManager = endpoint.getConnectionManager();
        this.configuration = endpoint.getConfiguration();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        connection = connectionManager.getConnection();
        if (log.isDebugEnabled()) {
            log.debug(format("Connected to Zookeeper cluster %s", configuration.getConnectString()));
        }

        connection.getData(configuration.getPath(), configuration.shouldWatch(), new DataCallback() {

            public void processResult(int rc, String path, Object ctx, byte[] data, Stat statistics) {
                if (log.isDebugEnabled()) {
                    if (log.isTraceEnabled()) {
                        log.trace(format("Received data from '%s' path with statistics '%s'", path, statistics));
                    } else {
                        log.debug(format("Received data from '%s' path ", path));
                    }
                }
                Exchange e = getEndpoint().createExchange();
                ZooKeeperMessage in = new ZooKeeperMessage(path, statistics);
                in.setBody(data, Byte[].class);
                e.setIn(in);
                try {
                    getProcessor().process(e);
                } catch (Exception ex) {
                    handleException(ex);
                }
            }
        }, this);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }
}
