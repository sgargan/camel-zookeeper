/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.zookeeper;

import static java.lang.String.format;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.util.ExchangeHelper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ZookeeperProducer</code> takes the content of an exchange and attempts to
 *
 * @version $
 */
public class ZookeeperProducer extends DefaultProducer {

    private ZooKeeperEndpoint endpoint;

    private ZooKeeperConnectionManager zkm;

    private LoggingCallback loggingStatHandler;

    public ZookeeperProducer(ZooKeeperEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.zkm = endpoint.getConnectionManager();
    }

    public void process(Exchange exchange) throws Exception {

        String node = getZookeeperProperty(exchange, ZooKeeperMessage.ZOOKEEPER_NODE, endpoint.getConfiguration().getPath(), String.class);
        Integer version = getZookeeperProperty(exchange, ZooKeeperMessage.ZOOKEEPER_NODE_VERSION, -1, Integer.class);
        ZooKeeper connection = zkm.getConnection();

        byte[] data = ExchangeHelper.convertToType(exchange, byte[].class, exchange.getIn().getBody());
        if (ExchangePattern.InOnly.equals(exchange.getPattern())) {
            if (log.isDebugEnabled()) {
                log.debug(format("Storing data to node '%s', not waiting for confirmation", node));
            }
            connection.setData(node, data, version, getLoggingCallback(), this);
        }
        else
        {
            if (log.isDebugEnabled()) {
                log.debug(format("Storing data '%s' to znode '%s', waiting for confirmation", node));
            }
            Stat statistics = connection.setData(node, data, version);
            logStoreComplete(node, statistics);
            ZooKeeperMessage out = new ZooKeeperMessage(node, statistics);
            out.setHeaders(exchange.getIn().getHeaders());
            exchange.setOut(out);
        }
    }

    private StatCallback getLoggingCallback() {
        if(loggingStatHandler == null)
        {
            loggingStatHandler = new LoggingCallback();
        }
        return loggingStatHandler;
    }

    private class LoggingCallback implements StatCallback {

        public void processResult(int rc, String path, Object ctx, Stat statistics) {
            System.err.println(org.apache.zookeeper.KeeperException.Code.get(rc));
            logStoreComplete(path, statistics);
        }
    }

    private void logStoreComplete(String path, Stat statistics) {
        if (log.isDebugEnabled()) {
            if (log.isTraceEnabled()) {
                log.trace(format("Received stats from storing data to znode '%s'", path, statistics));
            } else {
                log.debug(format("Received data from '%s' path ", path));
            }
        }
    }

    public <T> T getZookeeperProperty(Exchange e, String propertyName, T defaultValue, Class<? extends T> type) {
        T value = e.getIn().getHeader(propertyName, type);
        if(value == null)
        {
            value = defaultValue;
        }
        return value;
    }
}
