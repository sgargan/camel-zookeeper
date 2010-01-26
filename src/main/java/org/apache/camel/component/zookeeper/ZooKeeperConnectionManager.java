package org.apache.camel.component.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooKeeper;

/**
 * <code>ZookeeperConnectionManager</code> is a simple class to manage {@link ZooKeeper}
 * connections.
 *
 * @author sgargan
 */
public class ZooKeeperConnectionManager {

    private final Log log = LogFactory.getLog(ZooKeeperConnectionManager.class);

    public ZookeeperConnectionStrategy strategy;

    public ZooKeeperConnectionManager(ZooKeeperEndpoint endpoint){
       // strategy = isJmxEnabled(endpoint) ? new ReconfigureableZookeeperConnectionStrategy(endpoint) : ;
        strategy = new DefaultZookeeperConnectionStrategy(endpoint);
    }

    public ZooKeeper getConnection()
    {
        return strategy.getConnection().getZooKeeper();
    }

//    private boolean isJmxEnabled(ZooKeeperEndpoint endpoint) {
//        CamelContext context = endpoint.getCamelContext();
//        return context.getManagementStrategy() instanceof ManagedManagementStrategy;
//    }

    private interface ZookeeperConnectionStrategy {
        public ConnectionHolder getConnection();

        public void shutdown();
    }

    private class DefaultZookeeperConnectionStrategy implements ZookeeperConnectionStrategy {
        private ConnectionHolder holder;
        private ZooKeeperConfiguration configuration;

        public DefaultZookeeperConnectionStrategy(ZooKeeperEndpoint endpoint){
            this.configuration = endpoint.getConfiguration();
            if (log.isDebugEnabled()) {
                log.debug(String.format("Creating connection with static configuration of %s", configuration));
            }
            holder = new ConnectionHolder(configuration);
        }

        public ConnectionHolder getConnection(){
            return holder;
        }

        public void shutdown() {
            holder.closeConnection();
        }
    }

    public void shutdown() {
        strategy.shutdown();
    }

}
