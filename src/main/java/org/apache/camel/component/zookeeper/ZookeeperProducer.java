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
import static org.apache.camel.component.zookeeper.ZooKeeperUtils.getAclList;
import static org.apache.camel.component.zookeeper.ZooKeeperUtils.getCreateMode;
import static org.apache.camel.component.zookeeper.ZooKeeperUtils.getPayloadFromExchange;
import static org.apache.camel.component.zookeeper.ZooKeeperUtils.getZookeeperProperty;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Message;
import org.apache.camel.component.zookeeper.operations.CreateOperation;
import org.apache.camel.component.zookeeper.operations.OperationResult;
import org.apache.camel.component.zookeeper.operations.SetDataOperation;
import org.apache.camel.impl.DefaultProducer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * <code>ZookeeperProducer</code> takes the content of an exchange and attempts
 * to
 *
 * @version $
 */
@SuppressWarnings("unchecked")
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

        Message m = exchange.getIn();
        String node = getZookeeperProperty(m, ZooKeeperMessage.ZOOKEEPER_NODE, endpoint.getConfiguration().getPath(), String.class);
        Integer version = getZookeeperProperty(m, ZooKeeperMessage.ZOOKEEPER_NODE_VERSION, -1, Integer.class);

        ZooKeeper connection = zkm.getConnection();

        byte[] payloadFromExchange = getPayloadFromExchange(exchange);
        if (ExchangePattern.InOnly.equals(exchange.getPattern())) {
            if (log.isDebugEnabled()) {
                log.debug(format("Storing data to node '%s', not waiting for confirmation", node));
            }
            connection.setData(node, payloadFromExchange, version, getLoggingCallback(), new AsyncContext(connection, exchange));
        } else {
            if (log.isDebugEnabled()) {
                log.debug(format("Storing data '%s' to znode '%s', waiting for confirmation", node));
            }

            OperationResult result = synchronouslySetData(connection, node, exchange);

            ZooKeeperMessage out = new ZooKeeperMessage(node, result.getStatistics());
            if (result.isOk()) {
                out.setBody(result.getResult());
            } else {
                exchange.setException(result.getException());
            }
            out.setHeaders(exchange.getIn().getHeaders());
            exchange.setOut(out);
        }
    }

    private Exchange createExchange(String path, OperationResult result) {
        Exchange e = getEndpoint().createExchange();
        ZooKeeperMessage in = new ZooKeeperMessage(path, result.getStatistics());
        e.setIn(in);

        return e;
    }

    private StatCallback getLoggingCallback() {
        if (loggingStatHandler == null) {
            loggingStatHandler = new LoggingCallback();
        }
        return loggingStatHandler;
    }

    private class AsyncContext {
        ZooKeeper connection;
        Exchange exchange;

        public AsyncContext(ZooKeeper connection, Exchange exchange) {
            this.connection = connection;
            this.exchange = exchange;
        }
    }

    private class LoggingCallback implements StatCallback {

        public void processResult(int rc, String node, Object ctx, Stat statistics) {
            if (Code.NONODE.equals(Code.get(rc))) {
                if (endpoint.getConfiguration().shouldCreate()) {
                    log.warn(format("Node '%s' did not exist, creating it...", node));
                    AsyncContext context = (AsyncContext)ctx;
                    OperationResult<String> result = null;
                    try {
                        result = createNode(context.connection, node, context.exchange);

                    } catch (Exception e) {
                        log.warn(format("Node '%s' did not exist, creating it...", node));
                    }

                    if (result != null && result.isOk()) {
                        try {
                            synchronouslySetData(context.connection, node, context.exchange);
                        } catch (Exception e) {
                            log.error(format("Error setting data of node '%s' in async mode...", node), e);
                        }
                    }
                }
            }
            logStoreComplete(node, statistics);
        }
    }

    private OperationResult<String> createNode(ZooKeeper connection, String node, Exchange e) throws Exception {
        CreateOperation create = new CreateOperation(connection, node);
        create.setPermissions(getAclList(e.getIn()));
        create.setCreateMode(getCreateMode(e.getIn()));
        create.setData(getPayloadFromExchange(e));
        return create.get();
    }


    /**
     * Tries to set the data first and if a nonode error is recieved then an attempt will be made to create and set it again
     */
    private OperationResult synchronouslySetData(ZooKeeper connection, String node, Exchange e) throws Exception {

        SetDataOperation setData = new SetDataOperation(connection, node, getPayloadFromExchange(e));
        setData.setVersion(getVersionFromMessageHeader(e));

        OperationResult result = setData.get();

        if (!result.isOk() && endpoint.getConfiguration().shouldCreate() && result.failedDueTo(Code.NONODE)) {
            log.warn(format("Node '%s' did not exist, creating it...", node));
            result = createNode(connection, node, e);

        }
        return result;
    }

    private Integer getVersionFromMessageHeader(Exchange e) {
        return getZookeeperProperty(e.getIn(), ZooKeeperMessage.ZOOKEEPER_NODE_VERSION, -1, Integer.class);
    }

    private void logStoreComplete(String path, Stat statistics) {
        if (log.isDebugEnabled()) {
            if (log.isTraceEnabled()) {
                log.trace(format("Stored data to node '%s', and receive statistics %s", path, statistics));
            } else {
                log.debug(format("Stored data to node '%s'", path));
            }
        }
    }
}
