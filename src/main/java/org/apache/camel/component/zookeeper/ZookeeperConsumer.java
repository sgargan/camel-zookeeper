package org.apache.camel.component.zookeeper;

import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

public class ZookeeperConsumer extends DefaultConsumer {

    public ZookeeperConsumer(ZookeeperEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }



}
