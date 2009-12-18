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

    private LoggingStatHandler loggingStatHandler;

    public ZookeeperProducer(ZooKeeperEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
        this.zkm = endpoint.getConnectionManager();
    }

    public void process(Exchange exchange) throws Exception {

        String znodePath = getZookeeperProperty(exchange, ZooKeeperMessage.ZOOKEEPER_PATH, endpoint.getConfiguration().getPath(), String.class);
        Integer version = getZookeeperProperty(exchange, ZooKeeperMessage.ZOOKEEPER_ZNODE_VERSION, -1, Integer.class);
        ZooKeeper zoo = zkm.getConnection();

        byte[] data = ExchangeHelper.convertToType(exchange, byte[].class, exchange.getIn().getBody());
        if (ExchangePattern.InOnly.equals(exchange.getPattern())) {
            if (log.isDebugEnabled()) {
                log.debug(format("Storing data to znode '%s', not waiting for confirmation", znodePath));
            }
            zoo.setData(znodePath, data, version, getLoggingStatHandler(), this);
        }
        else
        {
            if (log.isDebugEnabled()) {
                log.debug(format("Storing data '%s' to znode '%s', waiting for confirmation", znodePath));
            }
            Stat statistics = zoo.setData(znodePath, data, version);
            logStoreComplete(znodePath, statistics);
            ZooKeeperMessage out = new ZooKeeperMessage(znodePath, statistics);
            out.setHeaders(exchange.getIn().getHeaders());
            exchange.setOut(out);
        }
    }

    private StatCallback getLoggingStatHandler() {
        if(loggingStatHandler == null)
        {
            loggingStatHandler = new LoggingStatHandler();
        }
        return loggingStatHandler;
    }

    private class LoggingStatHandler implements StatCallback {

        public void processResult(int rc, String path, Object ctx, Stat statistics) {
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
        T value = defaultValue;
        value = e.getIn().getHeader(propertyName, type);
        return value;
    }
}
